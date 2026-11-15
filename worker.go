package pgqueue

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"runtime/debug"
	"sort"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Handler processes one job. Returning nil marks the job succeeded; any
// other error schedules a retry unless it is wrapped with Permanent or is
// a Snooze. The context is cancelled when the job times out, the lease is
// lost or the worker is shutting down.
type Handler func(ctx context.Context, job *Job) error

// Config tunes a Worker. Zero values are replaced with defaults.
type Config struct {
	Queue       string // default "default"
	Concurrency int    // jobs processed in parallel, default 10
	// PollInterval is the fallback when no NOTIFY arrives, default 2s.
	PollInterval time.Duration
	// Lease is how long a claimed job stays reserved without a heartbeat.
	// The heartbeat runs every Lease/3. Default 30s.
	Lease time.Duration
	// JobTimeout bounds a single handler call; 0 means no limit.
	JobTimeout time.Duration
	// ShutdownTimeout is how long Run waits for in-flight jobs after its
	// context is cancelled before cancelling them. Default 15s.
	ShutdownTimeout time.Duration
	Backoff         BackoffFunc  // default ExponentialBackoff(1s, 1h)
	Logger          *slog.Logger // default slog.Default()
	ID              string       // worker id stored in locked_by, default host-pid-random
	// OnJobDone, if set, is called after every execution. It must be fast
	// and safe for concurrent use; the metrics package uses it.
	OnJobDone func(job *Job, outcome Outcome, took time.Duration)
}

var (
	errLeaseLost = errors.New("pgqueue: lease lost")
	errShutdown  = errors.New("pgqueue: worker shutting down")
)

// Worker claims and runs jobs from one queue.
type Worker struct {
	pool     *pgxpool.Pool
	cfg      Config
	log      *slog.Logger
	handlers map[string]Handler

	mu sync.Mutex
	// Keyed by (id, attempt): after a lease expires this same worker may
	// claim the job again while the old run is still winding down.
	inflight map[jobRun]context.CancelCauseFunc
}

type jobRun struct {
	id      int64
	attempt int
}

// NewWorker creates a worker. Register handlers with Handle before Run.
func NewWorker(pool *pgxpool.Pool, cfg Config) *Worker {
	if cfg.Queue == "" {
		cfg.Queue = DefaultQueue
	}
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 10
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 2 * time.Second
	}
	if cfg.Lease <= 0 {
		cfg.Lease = 30 * time.Second
	}
	if cfg.ShutdownTimeout <= 0 {
		cfg.ShutdownTimeout = 15 * time.Second
	}
	if cfg.Backoff == nil {
		cfg.Backoff = ExponentialBackoff(time.Second, time.Hour)
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.ID == "" {
		host, _ := os.Hostname()
		cfg.ID = fmt.Sprintf("%s-%d-%04x", host, os.Getpid(), rand.N(0x10000))
	}
	return &Worker{
		pool:     pool,
		cfg:      cfg,
		log:      cfg.Logger.With("worker", cfg.ID, "queue", cfg.Queue),
		handlers: map[string]Handler{},
		inflight: map[jobRun]context.CancelCauseFunc{},
	}
}

// ID returns the worker id written to locked_by.
func (w *Worker) ID() string { return w.cfg.ID }

// Handle registers h for jobs of the given kind. The worker only claims
// kinds it has handlers for, so different binaries can share a queue.
func (w *Worker) Handle(kind string, h Handler) {
	w.handlers[kind] = h
}

// Run processes jobs until ctx is cancelled, then drains in-flight jobs
// and returns. It returns an error only for misconfiguration.
func (w *Worker) Run(ctx context.Context) error {
	if len(w.handlers) == 0 {
		return errors.New("pgqueue: worker has no handlers")
	}
	kinds := make([]string, 0, len(w.handlers))
	for k := range w.handlers {
		kinds = append(kinds, k)
	}
	sort.Strings(kinds)

	wake := make(chan struct{}, 1)
	var listener sync.WaitGroup
	listener.Go(func() { w.listen(ctx, wake) })

	// The heartbeat and the completer must outlive ctx: jobs keep running
	// and finishing during the drain.
	hbCtx, stopHB := context.WithCancel(context.Background())
	var helpers sync.WaitGroup
	helpers.Go(func() { w.heartbeat(hbCtx) })
	completions := make(chan completion, w.cfg.Concurrency)
	helpers.Go(func() { w.completer(hbCtx, completions) })

	jobsCtx, cancelJobs := context.WithCancelCause(context.Background())
	defer cancelJobs(nil)

	var (
		jobs     sync.WaitGroup
		finished = make(chan struct{}, w.cfg.Concurrency)
		active   int
		more     = true // last fetch was full, the queue probably has more
		fetchNow = true
		poll     = time.NewTicker(w.cfg.PollInterval)
	)
	defer poll.Stop()

	w.log.Info("worker started", "kinds", kinds, "concurrency", w.cfg.Concurrency)

loop:
	for {
		if fetchNow && active < w.cfg.Concurrency && ctx.Err() == nil {
			want := w.cfg.Concurrency - active
			claimed, err := w.fetch(ctx, kinds, want)
			if err != nil {
				if ctx.Err() == nil {
					w.log.Error("fetch failed", "err", err)
				}
				claimed = nil
			}
			for _, job := range claimed {
				active++
				jobs.Go(func() {
					w.execute(jobsCtx, job, completions)
					finished <- struct{}{}
				})
			}
			more = err == nil && len(claimed) == want
			fetchNow = more
		}

		select {
		case <-ctx.Done():
			break loop
		case <-finished:
			active--
			// Refill in batches rather than one query per finished job.
			fetchNow = more && (w.cfg.Concurrency-active)*4 >= w.cfg.Concurrency
		case <-wake:
			fetchNow = true
		case <-poll.C:
			fetchNow = true
		}
	}

	w.drain(&jobs, cancelJobs)
	stopHB()
	helpers.Wait()
	listener.Wait()
	w.log.Info("worker stopped")
	return nil
}

func (w *Worker) drain(jobs *sync.WaitGroup, cancelJobs context.CancelCauseFunc) {
	drained := make(chan struct{})
	go func() {
		jobs.Wait()
		close(drained)
	}()

	select {
	case <-drained:
		return
	case <-time.After(w.cfg.ShutdownTimeout):
	}

	w.log.Warn("shutdown timeout reached, cancelling in-flight jobs", "count", w.inflightCount())
	cancelJobs(errShutdown)
	select {
	case <-drained:
	case <-time.After(5 * time.Second):
		// The handler ignores its context. Stop heartbeating and let the
		// lease expire; the maintainer will return the job to the queue.
		w.log.Error("handlers did not stop, abandoning jobs to lease expiry", "count", w.inflightCount())
	}
}

const fetchSQL = `
WITH next AS (
    SELECT id FROM pgqueue_jobs
    WHERE queue = $1 AND kind = ANY($2::text[]) AND state = 'pending' AND run_at <= now()
    ORDER BY priority DESC, run_at, id
    LIMIT $3
    FOR UPDATE SKIP LOCKED
)
UPDATE pgqueue_jobs j
SET state = 'running',
    attempts = j.attempts + 1,
    locked_by = $4,
    locked_until = now() + make_interval(secs => $5::float8),
    updated_at = now()
FROM next
WHERE j.id = next.id
RETURNING j.id, j.queue, j.kind, j.payload, j.priority, j.attempts, j.max_attempts, j.run_at, j.created_at`

func (w *Worker) fetch(ctx context.Context, kinds []string, n int) ([]*Job, error) {
	rows, err := w.pool.Query(ctx, fetchSQL, w.cfg.Queue, kinds, n, w.cfg.ID, w.cfg.Lease.Seconds())
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (*Job, error) {
		var j Job
		err := row.Scan(&j.ID, &j.Queue, &j.Kind, &j.Payload, &j.Priority, &j.Attempt,
			&j.MaxAttempts, &j.RunAt, &j.CreatedAt)
		return &j, err
	})
}

func (w *Worker) execute(parent context.Context, job *Job, completions chan<- completion) {
	ctx, cancel := context.WithCancelCause(parent)
	defer cancel(nil)
	run := jobRun{job.ID, job.Attempt}
	w.track(run, cancel)
	defer w.untrack(run)

	runCtx := ctx
	if w.cfg.JobTimeout > 0 {
		var cancelTimeout context.CancelFunc
		runCtx, cancelTimeout = context.WithTimeout(ctx, w.cfg.JobTimeout)
		defer cancelTimeout()
	}

	start := time.Now()
	err := w.call(runCtx, job)
	took := time.Since(start)

	outcome, ferr := w.finish(job, err, context.Cause(ctx), completions)
	if ferr != nil {
		w.log.Error("could not record job result", "job_id", job.ID, "kind", job.Kind, "err", ferr)
	}

	log := w.log.With("job_id", job.ID, "kind", job.Kind, "attempt", job.Attempt,
		"outcome", outcome, "took", took.Round(time.Millisecond))
	switch outcome {
	case OutcomeSucceeded, OutcomeSnoozed, OutcomeReleased:
		log.Debug("job done")
	case OutcomeDead, OutcomeLeaseLost:
		log.Error("job failed", "err", err)
	default:
		log.Warn("job failed", "err", err)
	}
	if w.cfg.OnJobDone != nil {
		w.cfg.OnJobDone(job, outcome, took)
	}
}

func (w *Worker) call(ctx context.Context, job *Job) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: %v\n%s", r, debug.Stack())
		}
	}()
	return w.handlers[job.Kind](ctx, job)
}

// All result updates are fenced on (id, attempts, state): attempts grows
// on every claim, so a worker that lost its lease cannot overwrite the
// result of the worker that took the job over.
const (
	completeSQL = `
UPDATE pgqueue_jobs j
SET state = 'succeeded', locked_by = NULL, locked_until = NULL,
    finished_at = now(), updated_at = now()
FROM unnest($1::bigint[], $2::int[]) AS t(id, attempts)
WHERE j.id = t.id AND j.attempts = t.attempts AND j.state = 'running'
RETURNING j.id, j.attempts`

	rescheduleSQL = `
UPDATE pgqueue_jobs
SET state = 'pending', locked_by = NULL, locked_until = NULL,
    run_at = now() + make_interval(secs => $3::float8),
    attempts = attempts - $4::int,
    last_error = COALESCE($5, last_error),
    updated_at = now()
WHERE id = $1 AND attempts = $2 AND state = 'running'`

	killSQL = `
UPDATE pgqueue_jobs
SET state = 'dead', locked_by = NULL, locked_until = NULL, last_error = $3,
    finished_at = now(), updated_at = now()
WHERE id = $1 AND attempts = $2 AND state = 'running'`
)

const maxErrorLen = 4000

func (w *Worker) finish(job *Job, handlerErr, cause error, completions chan<- completion) (Outcome, error) {
	if handlerErr != nil && errors.Is(cause, errLeaseLost) {
		return OutcomeLeaseLost, nil
	}

	// The handler's context may be dead; the result must still be saved.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var (
		outcome Outcome
		sql     string
		args    []any
	)
	if handlerErr == nil {
		// The completer only stops after all jobs drained, except when the
		// worker abandons handlers that ignore cancellation.
		abandon := time.NewTimer(30 * time.Second)
		defer abandon.Stop()
		done := make(chan error, 1)
		select {
		case completions <- completion{run: jobRun{job.ID, job.Attempt}, done: done}:
		case <-abandon.C:
			return OutcomeLeaseLost, errors.New("pgqueue: completer stopped")
		}
		var err error
		select {
		case err = <-done:
		case <-abandon.C:
			return OutcomeLeaseLost, errors.New("pgqueue: completer stopped")
		}
		switch {
		case errors.Is(err, errNotOwned):
			return OutcomeLeaseLost, nil
		case err != nil:
			return OutcomeSucceeded, err
		}
		return OutcomeSucceeded, nil
	}

	var snooze *snoozeError
	switch {
	case errors.As(handlerErr, &snooze):
		outcome, sql = OutcomeSnoozed, rescheduleSQL
		args = []any{job.ID, job.Attempt, snooze.d.Seconds(), 1, nil}
	case errors.Is(cause, errShutdown):
		outcome, sql = OutcomeReleased, rescheduleSQL
		args = []any{job.ID, job.Attempt, 0.0, 1, nil}
	case errors.As(handlerErr, new(*permanentError)) || job.Attempt >= job.MaxAttempts:
		outcome, sql = OutcomeDead, killSQL
		args = []any{job.ID, job.Attempt, truncate(handlerErr.Error())}
	default:
		delay := w.cfg.Backoff(job.Attempt)
		outcome, sql = OutcomeRetried, rescheduleSQL
		args = []any{job.ID, job.Attempt, delay.Seconds(), 0, truncate(handlerErr.Error())}
	}

	tag, err := w.pool.Exec(ctx, sql, args...)
	if err != nil {
		return outcome, err
	}
	if tag.RowsAffected() == 0 {
		// The reaper already took the job back.
		return OutcomeLeaseLost, nil
	}
	if outcome == OutcomeReleased {
		_, _ = w.pool.Exec(ctx, "SELECT pg_notify($1, $2)", notifyChannel, job.Queue)
	}
	return outcome, nil
}

var errNotOwned = errors.New("pgqueue: job no longer owned by this run")

type completion struct {
	run  jobRun
	done chan error
}

// completer marks successful jobs as succeeded. It takes whatever has
// queued up since the last flush and writes it in one UPDATE, so under load
// many jobs share a single commit, and an idle worker adds no delay.
func (w *Worker) completer(ctx context.Context, in <-chan completion) {
	const maxBatch = 500
	batch := make([]completion, 0, maxBatch)
	for {
		select {
		case c := <-in:
			batch = append(batch[:0], c)
		case <-ctx.Done():
			return
		}
	collect:
		for len(batch) < maxBatch {
			select {
			case c := <-in:
				batch = append(batch, c)
			default:
				break collect
			}
		}
		w.flushCompletions(batch)
	}
}

func (w *Worker) flushCompletions(batch []completion) {
	ids := make([]int64, len(batch))
	attempts := make([]int32, len(batch))
	for i, c := range batch {
		ids[i], attempts[i] = c.run.id, int32(c.run.attempt)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	rows, err := w.pool.Query(ctx, completeSQL, ids, attempts)
	var done []jobRun
	if err == nil {
		done, err = pgx.CollectRows(rows, func(row pgx.CollectableRow) (jobRun, error) {
			var r jobRun
			err := row.Scan(&r.id, &r.attempt)
			return r, err
		})
	}
	ok := make(map[jobRun]bool, len(done))
	for _, r := range done {
		ok[r] = true
	}
	for _, c := range batch {
		switch {
		case err != nil:
			c.done <- err
		case ok[c.run]:
			c.done <- nil
		default:
			c.done <- errNotOwned
		}
	}
}

func truncate(s string) string {
	if len(s) <= maxErrorLen {
		return s
	}
	return s[:maxErrorLen] + "...(truncated)"
}

func (w *Worker) track(run jobRun, cancel context.CancelCauseFunc) {
	w.mu.Lock()
	w.inflight[run] = cancel
	w.mu.Unlock()
}

func (w *Worker) untrack(run jobRun) {
	w.mu.Lock()
	delete(w.inflight, run)
	w.mu.Unlock()
}

func (w *Worker) inflightCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.inflight)
}

const heartbeatSQL = `
UPDATE pgqueue_jobs j
SET locked_until = now() + make_interval(secs => $3::float8), updated_at = now()
FROM unnest($1::bigint[], $2::int[]) AS t(id, attempts)
WHERE j.id = t.id AND j.attempts = t.attempts AND j.state = 'running'
RETURNING j.id, j.attempts`

// heartbeat extends the leases of in-flight jobs. Jobs whose lease could
// not be extended were reclaimed by the maintainer, so their handlers are
// cancelled: someone else is (or soon will be) running them.
func (w *Worker) heartbeat(ctx context.Context) {
	t := time.NewTicker(w.cfg.Lease / 3)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}

		w.mu.Lock()
		ids := make([]int64, 0, len(w.inflight))
		attempts := make([]int32, 0, len(w.inflight))
		for run := range w.inflight {
			ids = append(ids, run.id)
			attempts = append(attempts, int32(run.attempt))
		}
		w.mu.Unlock()
		if len(ids) == 0 {
			continue
		}

		qctx, cancel := context.WithTimeout(ctx, w.cfg.Lease/3)
		rows, err := w.pool.Query(qctx, heartbeatSQL, ids, attempts, w.cfg.Lease.Seconds())
		var alive []jobRun
		if err == nil {
			alive, err = pgx.CollectRows(rows, func(row pgx.CollectableRow) (jobRun, error) {
				var r jobRun
				err := row.Scan(&r.id, &r.attempt)
				return r, err
			})
		}
		cancel()
		if err != nil {
			if ctx.Err() == nil {
				w.log.Error("heartbeat failed", "err", err)
			}
			continue
		}

		ok := make(map[jobRun]bool, len(alive))
		for _, r := range alive {
			ok[r] = true
		}
		w.mu.Lock()
		for i, id := range ids {
			run := jobRun{id, int(attempts[i])}
			if cancel, tracked := w.inflight[run]; tracked && !ok[run] {
				w.log.Warn("lease lost, cancelling job", "job_id", id, "attempt", run.attempt)
				cancel(errLeaseLost)
			}
		}
		w.mu.Unlock()
	}
}

// listen wakes the fetch loop on NOTIFY for this queue. It uses its own
// connection outside the pool and reconnects with backoff.
func (w *Worker) listen(ctx context.Context, wake chan<- struct{}) {
	backoff := ExponentialBackoff(500*time.Millisecond, 30*time.Second)
	for attempt := 1; ctx.Err() == nil; attempt++ {
		err := w.listenOnce(ctx, wake, func() { attempt = 0 })
		if ctx.Err() != nil {
			return
		}
		w.log.Warn("listener disconnected, relying on polling until it reconnects", "err", err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff(max(attempt, 1))):
		}
	}
}

func (w *Worker) listenOnce(ctx context.Context, wake chan<- struct{}, connected func()) error {
	conn, err := pgx.ConnectConfig(ctx, w.pool.Config().ConnConfig.Copy())
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())

	if _, err := conn.Exec(ctx, "LISTEN "+notifyChannel); err != nil {
		return err
	}
	connected()
	signal(wake) // notifications sent while we were offline are lost

	for {
		n, err := conn.WaitForNotification(ctx)
		if err != nil {
			return err
		}
		if n.Payload == w.cfg.Queue {
			signal(wake)
		}
	}
}

func signal(ch chan<- struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}
