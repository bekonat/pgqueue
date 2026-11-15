# pgqueue

A background job queue for Go that uses **PostgreSQL** as its only dependency. Jobs are rows in one table. Workers claim them with `FOR UPDATE SKIP LOCKED`, hold a **lease** that a heartbeat keeps extending, and save the result with a **fenced** `UPDATE`. A worker that crashes loses its lease, and the job goes back to the queue. Retries use exponential backoff with jitter. Jobs that keep failing end up in a dead state, where they can be inspected and retried by hand.

You can use it as a library inside your own service, or run it as a small HTTP service that has an admin API and Prometheus metrics.

```
  your service ──EnqueueTx (same tx as business data)──┐
  curl / other services ── POST /v1/jobs ──▶ ┌────────┐ │
                                             │  api   │ │
                                             └───┬────┘ ▼
                                    ┌───────────────────────────┐
             NOTIFY pgqueue_jobs ◀──│ PostgreSQL: pgqueue_jobs  │◀── maintainer: reap expired
             (wake idle workers)    └───────────────────────────┘    leases, delete old jobs
                      │           ▲ claim: FOR UPDATE SKIP LOCKED
                      ▼           │ heartbeat: extend lease
              ┌──────────┐ ┌──────────┐ ┌──────────┐
              │ worker 1 │ │ worker 2 │ │ worker N │   handlers by job kind,
              └──────────┘ └──────────┘ └──────────┘   N goroutines each
```

## Features

- **Concurrent claiming without blocking.** `SELECT … FOR UPDATE SKIP LOCKED` inside an `UPDATE … RETURNING` claims a batch in one round trip. Workers never wait on each other's row locks. A test runs 4 workers × 8 goroutines over 500 jobs and checks that every job runs exactly once.
- **Leases instead of long transactions.** A claimed job is marked `running` with `locked_until = now() + lease`, and the transaction commits right away. A slow job does not keep a transaction open, so it does not hold back VACUUM. A heartbeat extends the leases of every in-flight job in one `UPDATE … FROM unnest(...)`.
- **Fencing tokens.** `attempts` grows on every claim, and every result is written with `WHERE id = $1 AND attempts = $2 AND state = 'running'`. Suppose a worker is paused (GC, network partition), loses its lease, and wakes up later. It cannot overwrite the result of the worker that took the job over. When the heartbeat finds a lost lease, it cancels that handler's context with the cause `lease lost`.
- **Crash recovery.** The maintainer returns jobs with expired leases to `pending`. A job that had used its last attempt goes to `dead` instead. Several maintainers can run at the same time, because they skip rows locked by the others.
- **Retries.** Exponential backoff with equal jitter (1s → 1h by default) and `max_attempts` per job. Handlers have three ways to control what happens: `pgqueue.Permanent(err)` sends the job straight to `dead`, `pgqueue.Snooze(d)` reschedules it without using up an attempt, and a panic is recovered and counted as a failed attempt with its stack trace stored.
- **Transactional enqueue.** `EnqueueTx` accepts a `pgx.Tx`. The job is committed or rolled back together with your business data, so no outbox table is needed when the queue lives in the same database.
- **Deduplication.** `unique_key` is enforced by a partial unique index over `pending` and `running` jobs. Enqueueing a duplicate returns the existing job's id with `duplicate: true`. Once that job finishes, the key is free again.
- **Low latency without busy polling.** `pg_notify` runs in the enqueue transaction, so the notification is delivered on commit and a rolled-back enqueue wakes nobody. Each worker listens on a dedicated connection outside the pool and falls back to polling while it reconnects. In tests an idle worker picks up a new job in about 4 ms.
- **Batched completions.** Successful results are collected from all goroutines and written with one `UPDATE` per batch. Under load, many jobs share one commit, and an idle worker adds no delay (see the benchmark below).
- **Graceful shutdown.** On SIGTERM the worker stops claiming jobs and waits `ShutdownTimeout` for the in-flight ones. Then it cancels them, and jobs that were interrupted go back to `pending` without using up an attempt. A handler that ignores cancellation is abandoned to lease expiry instead of blocking the shutdown forever.
- **Priorities, delayed jobs, per-job timeouts**, and kind routing: a worker claims only the kinds it has handlers for, so different services can share one queue.
- **Bulk enqueue.** `EnqueueMany` inserts thousands of jobs with one `INSERT … SELECT FROM unnest(...)`.
- **Small indexes.** The fetch, lease, and cleanup indexes are partial, so the hot fetch index covers only `pending` rows even when the table holds millions of finished jobs. Finished jobs are deleted in batches of 5000 after the retention period.
- **Observability.** Prometheus metrics: jobs processed by queue, kind, and outcome, a handler latency histogram, reaped leases, job counts per state, and **queue lag** (the age of the oldest runnable job). JSON logs via `log/slog`.

## Job lifecycle

```
               claim (attempts+1)              handler returns nil
  pending ───────────────────────▶ running ─────────────────────────▶ succeeded
     ▲  ▲                           │  │  │
     │  └── error, attempts left ───┘  │  └── Permanent(err) or no attempts left ──▶ dead
     │      (run_at = now + backoff)   │                                              │
     │                                 └── lease expired: maintainer returns it ──────┤
     │                                     (or kills it if no attempts left)          │
     └──────────────────────── Client.Retry (attempts = 0) ◀─────────────────────────┘
  pending ── Client.Cancel ──▶ cancelled
```

Delivery is **at least once**. If a worker dies after the side effect but before saving the result, the job runs again, so handlers must be idempotent: use the job id as an idempotency key downstream.

## Library usage

```go
pool, _ := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
if err := pgqueue.Migrate(ctx, pool); err != nil { ... }

// Producer: the job is written in the same transaction as the order.
client := pgqueue.NewClient(pool)
err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
    if _, err := tx.Exec(ctx, "INSERT INTO orders ...", ...); err != nil {
        return err
    }
    _, err := client.EnqueueTx(ctx, tx, pgqueue.EnqueueParams{
        Kind:      "email.send",
        Payload:   Email{To: "user@example.com", Template: "order_created"},
        UniqueKey: "order-created-" + orderID,
    })
    return err
})

// Consumer
w := pgqueue.NewWorker(pool, pgqueue.Config{Concurrency: 16, JobTimeout: time.Minute})
w.Handle("email.send", func(ctx context.Context, job *pgqueue.Job) error {
    var e Email
    if err := job.Decode(&e); err != nil {
        return pgqueue.Permanent(err) // retrying will not fix a bad payload
    }
    if err := smtp.Send(ctx, e); errors.Is(err, smtp.ErrRateLimited) {
        return pgqueue.Snooze(30 * time.Second)
    } else if err != nil {
        return err // retried with backoff
    }
    return nil
})
go pgqueue.NewMaintainer(pool, pgqueue.MaintainerConfig{}).Run(ctx)
w.Run(ctx) // blocks until ctx is cancelled, then drains
```

## HTTP API

The `pgqueue` binary serves the admin API, `/metrics`, and `/healthz`, and runs the maintainer.

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/v1/jobs` | Enqueue. Body: `kind` (required), `queue`, `payload`, `priority`, `run_at` or `delay_seconds`, `max_attempts`, `unique_key`. Returns `201`, or `200` with `duplicate: true` |
| `GET` | `/v1/jobs/{id}` | Full job row including `attempts`, `last_error`, `locked_by` |
| `GET` | `/v1/jobs?queue=&kind=&state=&after_id=&limit=` | Keyset pagination; the response contains `next_after_id` |
| `POST` | `/v1/jobs/{id}/retry` | `dead` or `cancelled` → `pending` with a fresh attempt counter (`409` if not allowed) |
| `POST` | `/v1/jobs/{id}/cancel` | `pending` → `cancelled` (`409` for running jobs) |
| `GET` | `/v1/stats` | Counts per queue and state, plus queue lag |

```bash
$ curl -s -XPOST localhost:8080/v1/jobs -d '{"kind":"report.build","payload":{"user_id":1,"seconds":2},"unique_key":"report-1"}'
{"id":3776,"duplicate":false}
$ curl -s -XPOST localhost:8080/v1/jobs -d '{"kind":"report.build","payload":{"user_id":1,"seconds":2},"unique_key":"report-1"}'
{"id":3776,"duplicate":true}
$ curl -s localhost:8080/v1/stats
{"queues":[{"queue":"default","states":{"dead":1,"succeeded":51},"lag_seconds":0}]}
```

## Running

```bash
docker compose up --build -d   # postgres, api (:8080), 2 demo workers, prometheus (:9091)
./scripts/e2e.sh               # 50 emails (20% fail randomly and get retried), a deduplicated report, one bad job → dead
```

`demo-worker` has two example handlers. `email.send` fails 20% of the time so you can watch retries happen, and `report.build` is slow so you can watch the lease heartbeat at work.

Tests:

```bash
make test                      # unit tests: backoff, validation, HTTP handlers with a fake store
make test-integration          # against real PostgreSQL (TEST_DATABASE_URL)
```

The integration tests cover these scenarios: exactly-once processing across concurrent workers, retry until success, dead after `max_attempts` followed by a manual retry, `Permanent`, `Snooze`, panic recovery, priority order and delayed jobs, kind routing, NOTIFY wake-up, reclaiming a crashed worker's job, a heartbeat keeping a 2-second job alive with a 600 ms lease, a lost lease cancelling the handler, shutdown releasing or finishing jobs, job timeout, transactional enqueue rollback, admin errors, stats, cleanup, and concurrent migrations.

## Benchmark

`cmd/bench` enqueues 20,000 no-op jobs with `EnqueueMany` and processes them with workers in the same process. These are local numbers (4 cores, 8 GB RAM, PostgreSQL 18 with default settings, `synchronous_commit = on`), so read them as rough figures:

| Setup | Throughput |
|-------|------------|
| enqueue, `EnqueueMany` in batches of 1000 | ~65,000 jobs/s |
| enqueue, one `EnqueueTx` per job, 1000 per transaction | ~3,600 jobs/s |
| process, 1 worker × 1 goroutine | ~900–1,300 jobs/s |
| process, 1 worker × 16, one `UPDATE` per completed job | ~920 jobs/s |
| process, 1 worker × 16, batched completions | ~3,200–4,900 jobs/s |
| process, 4 workers × 16, batched completions | ~4,700 jobs/s |

What the numbers show:

- With one commit per job, 16 goroutines were *slower* than 1, because every completion waited on its own WAL flush and every freed slot triggered a fetch query. Batching completions and refilling only when a quarter of the slots are free made the same setup 3–5× faster.
- Throughput drops as the backlog grows: with 50,000 queued jobs, 1 × 16 runs at ~2,500 jobs/s. Every claim and completion leaves a dead tuple in the `pending` index, and the fetch query has to step over those tuples until VACUUM removes them. This is the usual weak spot of queues built on Postgres. In production I would tune autovacuum for this table and keep the backlog short.

## Design notes and limits

- **Why not hold the row lock for the whole job?** That would need one open transaction and one pool connection per running job, and long transactions block VACUUM. Leases need only short transactions, and the fencing token keeps them safe.
- **Lease length is a trade-off.** A short lease detects crashed workers sooner but increases heartbeat traffic. The heartbeat runs every `Lease/3`, so a worker survives two missed heartbeats.
- **NOTIFY is only a hint.** Notifications are lost while a listener is disconnected, so workers still poll every `PollInterval`, and each listener wakes its worker once after reconnecting.
- **`Stats` uses `count(*)`** over the whole table. That is fine for metrics scrapes, but don't call it on a hot path.
- **Cancelling a running job is not supported**, because the handler may already have caused side effects. Only `pending` jobs can be cancelled.
- **Not yet implemented:** cron-style periodic jobs, per-queue rate limits, and a web UI.

## Tech stack

Go 1.26 · PostgreSQL (pgx/v5) · Prometheus · log/slog · Docker Compose · GitHub Actions
