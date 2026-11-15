//go:build integration

package pgqueue_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bekonat/pgqueue"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

func testPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Fatal("TEST_DATABASE_URL is not set")
	}
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if err := pgqueue.Migrate(context.Background(), pool); err != nil {
		t.Fatal(err)
	}
	return pool
}

// Each test works in its own queue, so tests do not see each other's jobs.
func newQueue(t *testing.T) string {
	return fmt.Sprintf("%s-%d", t.Name(), time.Now().UnixNano())
}

func startWorker(t *testing.T, pool *pgxpool.Pool, cfg pgqueue.Config, handlers map[string]pgqueue.Handler) (stop func()) {
	t.Helper()
	if cfg.Logger == nil {
		cfg.Logger = quiet
	}
	if cfg.PollInterval == 0 {
		cfg.PollInterval = 50 * time.Millisecond
	}
	if cfg.Backoff == nil {
		cfg.Backoff = func(int) time.Duration { return 10 * time.Millisecond }
	}
	w := pgqueue.NewWorker(pool, cfg)
	for kind, h := range handlers {
		w.Handle(kind, h)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	var once sync.Once
	stop = func() {
		once.Do(func() {
			cancel()
			select {
			case err := <-done:
				if err != nil {
					t.Errorf("worker.Run: %v", err)
				}
			case <-time.After(30 * time.Second):
				t.Error("worker did not stop")
			}
		})
	}
	t.Cleanup(stop)
	return stop
}

func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func waitState(t *testing.T, c *pgqueue.Client, id int64, want pgqueue.State) *pgqueue.JobInfo {
	t.Helper()
	var job *pgqueue.JobInfo
	waitFor(t, 10*time.Second, fmt.Sprintf("job %d to be %s", id, want), func() bool {
		var err error
		job, err = c.Get(context.Background(), id)
		if err != nil {
			t.Fatal(err)
		}
		return job.State == want
	})
	return job
}

func enqueue(t *testing.T, c *pgqueue.Client, p pgqueue.EnqueueParams) int64 {
	t.Helper()
	res, err := c.Enqueue(context.Background(), p)
	if err != nil {
		t.Fatal(err)
	}
	return res.ID
}

func TestEnqueueAndProcess(t *testing.T) {
	pool := testPool(t)
	c := pgqueue.NewClient(pool)
	q := newQueue(t)

	type payload struct {
		To string `json:"to"`
	}
	got := make(chan string, 1)
	startWorker(t, pool, pgqueue.Config{Queue: q}, map[string]pgqueue.Handler{
		"email": func(ctx context.Context, job *pgqueue.Job) error {
			var p payload
			if err := job.Decode(&p); err != nil {
				return err
			}
			got <- p.To
			return nil
		},
	})

	id := enqueue(t, c, pgqueue.EnqueueParams{Queue: q, Kind: "email", Payload: payload{To: "a@b.kz"}})
	if to := <-got; to != "a@b.kz" {
		t.Fatalf("payload: got %q", to)
	}
	job := waitState(t, c, id, pgqueue.StateSucceeded)
	if job.Attempts != 1 || job.FinishedAt == nil || job.LockedBy != nil {
		t.Fatalf("unexpected finished job: %+v", job)
	}
}

func TestUniqueKeyDeduplicates(t *testing.T) {
	pool := testPool(t)
	c := pgqueue.NewClient(pool)
	q := newQueue(t)
	ctx := context.Background()

	p := pgqueue.EnqueueParams{Queue: q, Kind: "sync", UniqueKey: "user-42"}
	first, err := c.Enqueue(ctx, p)
	if err != nil {
		t.Fatal(err)
	}
	second, err := c.Enqueue(ctx, p)
	if err != nil {
		t.Fatal(err)
	}
	if first.Duplicate || !second.Duplicate || first.ID != second.ID {
		t.Fatalf("dedup failed: %+v %+v", first, second)
	}

	// Once the job is finished the key is free again.
	startWorker(t, pool, pgqueue.Config{Queue: q}, map[string]pgqueue.Handler{
		"sync": func(context.Context, *pgqueue.Job) error { return nil },
	})
	waitState(t, c, first.ID, pgqueue.StateSucceeded)
	third, err := c.Enqueue(ctx, p)
	if err != nil {
		t.Fatal(err)
	}
	if third.Duplicate || third.ID == first.ID {
		t.Fatalf("expected a new job after the first finished: %+v", third)
	}
}

func TestEnqueueTxIsAtomic(t *testing.T) {
	pool := testPool(t)
	c := pgqueue.NewClient(pool)
	q := newQueue(t)
	ctx := context.Background()

	var rolledBack int64
	err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		res, err := c.EnqueueTx(ctx, tx, pgqueue.EnqueueParams{Queue: q, Kind: "k"})
		rolledBack = res.ID
		if err != nil {
			return err
		}
		return errors.New("business logic failed")
	})
	if err == nil {
		t.Fatal("expected rollback")
	}
	if _, err := c.Get(ctx, rolledBack); !errors.Is(err, pgqueue.ErrNotFound) {
		t.Fatalf("rolled back job must not exist, got %v", err)
	}

	var committed int64
	err = pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		res, err := c.EnqueueTx(ctx, tx, pgqueue.EnqueueParams{Queue: q, Kind: "k"})
		committed = res.ID
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if job, err := c.Get(ctx, committed); err != nil || job.State != pgqueue.StatePending {
		t.Fatalf("committed job: %+v, %v", job, err)
	}
}

func TestEnqueueMany(t *testing.T) {
	pool := testPool(t)
	c := pgqueue.NewClient(pool)
	q := newQueue(t)
	ctx := context.Background()

	if _, err := c.EnqueueMany(ctx, pool, []pgqueue.EnqueueParams{{Queue: q, Kind: "k", UniqueKey: "x"}}); !errors.Is(err, pgqueue.ErrInvalidParams) {
		t.Fatalf("unique key must be rejected, got %v", err)
	}
	if _, err := c.EnqueueMany(ctx, pool, []pgqueue.EnqueueParams{{Queue: q, Kind: "k"}, {Queue: q}}); !errors.Is(err, pgqueue.ErrInvalidParams) {
		t.Fatalf("missing kind must be rejected, got %v", err)
	}

	var sum atomic.Int64
	startWorker(t, pool, pgqueue.Config{Queue: q}, map[string]pgqueue.Handler{
		"add": func(ctx context.Context, job *pgqueue.Job) error {
			var p struct{ N int64 }
			if err := job.Decode(&p); err != nil {
				return err
			}
			sum.Add(p.N)
			return nil
		},
	})
	params := make([]pgqueue.EnqueueParams, 100)
	for i := range params {
		params[i] = pgqueue.EnqueueParams{Queue: q, Kind: "add", Payload: map[string]int{"N": i + 1}, Priority: i % 3}
	}
	params[0].Delay = time.Hour
	ids, err := c.EnqueueMany(ctx, pool, params)
	if err != nil || len(ids) != 100 {
		t.Fatalf("enqueue many: %d ids, %v", len(ids), err)
	}
	waitFor(t, 10*time.Second, "batch processed", func() bool { return sum.Load() == 5050-1 })
	delayed, err := c.List(ctx, pgqueue.ListFilter{Queue: q, State: pgqueue.StatePending})
	if err != nil || len(delayed) != 1 || delayed[0].RunAt.Before(time.Now().Add(50*time.Minute)) {
		t.Fatalf("delayed job: %+v %v", delayed, err)
	}
}

func TestEachJobRunsOnceAcrossWorkers(t *testing.T) {
	pool := testPool(t)
	c := pgqueue.NewClient(pool)
	q := newQueue(t)
	const n = 500

	var (
		mu    sync.Mutex
		runs  = map[int64]int{}
		total atomic.Int64
	)
	handler := func(ctx context.Context, job *pgqueue.Job) error {
		mu.Lock()
		runs[job.ID]++
		mu.Unlock()
		total.Add(1)
		return nil
	}
	for range 4 {
		startWorker(t, pool, pgqueue.Config{Queue: q, Concurrency: 8}, map[string]pgqueue.Handler{"k": handler})
	}
	for range n {
		enqueue(t, c, pgqueue.EnqueueParams{Queue: q, Kind: "k"})
	}

	waitFor(t, 30*time.Second, "all jobs", func() bool { return total.Load() >= n })
	time.Sleep(200 * time.Millisecond) // catch late duplicates
	mu.Lock()
	defer mu.Unlock()
	if len(runs) != n || total.Load() != n {
		t.Fatalf("want %d jobs run once, got %d distinct, %d runs", n, len(runs), total.Load())
	}
	for id, k := range runs {
		if k != 1 {
			t.Fatalf("job %d ran %d times", id, k)
		}
	}
}

func TestRetryUntilSuccess(t *testing.T) {
	pool := testPool(t)
	c := pgqueue.NewClient(pool)
	q := newQueue(t)

	startWorker(t, pool, pgqueue.Config{Queue: q}, map[string]pgqueue.Handler{
		"flaky": func(ctx context.Context, job *pgqueue.Job) error {
			if job.Attempt < 3 {
				return fmt.Errorf("attempt %d failed", job.Attempt)
			}
			return nil
		},
	})
	id := enqueue(t, c, pgqueue.EnqueueParams{Queue: q, Kind: "flaky"})
	job := waitState(t, c, id, pgqueue.StateSucceeded)
	if job.Attempts != 3 || job.LastError == nil || *job.LastError != "attempt 2 failed" {
		t.Fatalf("unexpected job: attempts=%d last_error=%v", job.Attempts, job.LastError)
	}
}

func TestDeadAfterMaxAttemptsAndManualRetry(t *testing.T) {
	pool := testPool(t)
	c := pgqueue.NewClient(pool)
	q := newQueue(t)

	var healthy atomic.Bool
	startWorker(t, pool, pgqueue.Config{Queue: q}, map[string]pgqueue.Handler{
		"k": func(context.Context, *pgqueue.Job) error {
			if healthy.Load() {
				return nil
			}
			return errors.New("downstream is down")
		},
	})
	id := enqueue(t, c, pgqueue.EnqueueParams{Queue: q, Kind: "k", MaxAttempts: 3})
	job := waitState(t, c, id, pgqueue.StateDead)
	if job.Attempts != 3 || job.FinishedAt == nil {
		t.Fatalf("unexpected dead job: %+v", job)
	}

	healthy.Store(true)
	if _, err := c.Retry(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	job = waitState(t, c, id, pgqueue.StateSucceeded)
	if job.Attempts != 1 {
		t.Fatalf("retry must reset attempts, got %d", job.Attempts)
	}
}

func TestPermanentErrorSkipsRetries(t *testing.T) {
	pool := testPool(t)
	c := pgqueue.NewClient(pool)
	q := newQueue(t)

	startWorker(t, pool, pgqueue.Config{Queue: q}, map[string]pgqueue.Handler{
		"k": func(context.Context, *pgqueue.Job) error { return pgqueue.Permanent(errors.New("bad payload")) },
	})
	id := enqueue(t, c, pgqueue.EnqueueParams{Queue: q, Kind: "k"})
	job := waitState(t, c, id, pgqueue.StateDead)
	if job.Attempts != 1 || !strings.Contains(*job.LastError, "bad payload") {
		t.Fatalf("unexpected job: %+v", job)
	}
}

func TestSnoozeDoesNotUseAttempt(t *testing.T) {
	pool := testPool(t)
	c := pgqueue.NewClient(pool)
	q := newQueue(t)

	var runs atomic.Int32
	startWorker(t, pool, pgqueue.Config{Queue: q}, map[string]pgqueue.Handler{
		"k": func(context.Context, *pgqueue.Job) error {
			if runs.Add(1) <= 2 {
				return pgqueue.Snooze(30 * time.Millisecond)
			}
			return nil
		},
	})
	id := enqueue(t, c, pgqueue.EnqueueParams{Queue: q, Kind: "k", MaxAttempts: 1})
	job := waitState(t, c, id, pgqueue.StateSucceeded)
	if runs.Load() != 3 || job.Attempts != 1 {
		t.Fatalf("runs=%d attempts=%d", runs.Load(), job.Attempts)
	}
}

func TestPanicIsRetried(t *testing.T) {
	pool := testPool(t)
	c := pgqueue.NewClient(pool)
	q := newQueue(t)

	startWorker(t, pool, pgqueue.Config{Queue: q}, map[string]pgqueue.Handler{
		"k": func(ctx context.Context, job *pgqueue.Job) error {
			if job.Attempt == 1 {
				var m map[string]int
				m["boom"] = 1
			}
			return nil
		},
	})
	id := enqueue(t, c, pgqueue.EnqueueParams{Queue: q, Kind: "k"})
	job := waitState(t, c, id, pgqueue.StateSucceeded)
	if job.Attempts != 2 || !strings.HasPrefix(*job.LastError, "panic: assignment to entry in nil map") {
		t.Fatalf("unexpected job: attempts=%d err=%v", job.Attempts, job.LastError)
	}
}

func TestPriorityAndSchedule(t *testing.T) {
	pool := testPool(t)
	c := pgqueue.NewClient(pool)
	q := newQueue(t)

	low := enqueue(t, c, pgqueue.EnqueueParams{Queue: q, Kind: "k", Priority: -5})
	high := enqueue(t, c, pgqueue.EnqueueParams{Queue: q, Kind: "k", Priority: 10})
	mid := enqueue(t, c, pgqueue.EnqueueParams{Queue: q, Kind: "k"})
	later := enqueue(t, c, pgqueue.EnqueueParams{Queue: q, Kind: "k", Priority: 100, Delay: 700 * time.Millisecond})

	var (
		mu    sync.Mutex
		order []int64
		start = time.Now()
		ranAt time.Duration
	)
	startWorker(t, pool, pgqueue.Config{Queue: q, Concurrency: 1}, map[string]pgqueue.Handler{
		"k": func(ctx context.Context, job *pgqueue.Job) error {
			mu.Lock()
			defer mu.Unlock()
			order = append(order, job.ID)
			if job.ID == later {
				ranAt = time.Since(start)
			}
			return nil
		},
	})
	waitState(t, c, later, pgqueue.StateSucceeded)

	mu.Lock()
	defer mu.Unlock()
	want := []int64{high, mid, low, later}
	if fmt.Sprint(order) != fmt.Sprint(want) {
		t.Fatalf("order: got %v, want %v", order, want)
	}
	if ranAt < 700*time.Millisecond {
		t.Fatalf("delayed job ran too early: %s", ranAt)
	}
}

func TestWorkerOnlyClaimsKnownKinds(t *testing.T) {
	pool := testPool(t)
	c := pgqueue.NewClient(pool)
	q := newQueue(t)

	other := enqueue(t, c, pgqueue.EnqueueParams{Queue: q, Kind: "other"})
	mine := enqueue(t, c, pgqueue.EnqueueParams{Queue: q, Kind: "mine"})
	startWorker(t, pool, pgqueue.Config{Queue: q}, map[string]pgqueue.Handler{
		"mine": func(context.Context, *pgqueue.Job) error { return nil },
	})
	waitState(t, c, mine, pgqueue.StateSucceeded)
	time.Sleep(150 * time.Millisecond)
	if job, _ := c.Get(context.Background(), other); job.State != pgqueue.StatePending {
		t.Fatalf("foreign kind must stay pending, got %s", job.State)
	}
}

func TestNotifyWakesIdleWorker(t *testing.T) {
	pool := testPool(t)
	c := pgqueue.NewClient(pool)
	q := newQueue(t)

	done := make(chan time.Time, 1)
	startWorker(t, pool, pgqueue.Config{Queue: q, PollInterval: time.Hour}, map[string]pgqueue.Handler{
		"k": func(context.Context, *pgqueue.Job) error { done <- time.Now(); return nil },
	})
	time.Sleep(300 * time.Millisecond) // let the listener connect and the first poll pass

	start := time.Now()
	enqueue(t, c, pgqueue.EnqueueParams{Queue: q, Kind: "k"})
	select {
	case at := <-done:
		t.Logf("picked up %s after enqueue", at.Sub(start))
	case <-time.After(3 * time.Second):
		t.Fatal("job was not picked up; NOTIFY did not wake the worker")
	}
}

func TestReaperReclaimsCrashedWorkerJob(t *testing.T) {
	pool := testPool(t)
	c := pgqueue.NewClient(pool)
	q := newQueue(t)
	ctx := context.Background()

	// Simulate a worker that claimed the job and died.
	id := enqueue(t, c, pgqueue.EnqueueParams{Queue: q, Kind: "k"})
	_, err := pool.Exec(ctx, `
		UPDATE pgqueue_jobs SET state = 'running', attempts = 1, locked_by = 'dead-worker',
		       locked_until = now() - interval '1 second'
		WHERE id = $1`, id)
	if err != nil {
		t.Fatal(err)
	}
	// A job that has used its last attempt goes to dead instead.
	last := enqueue(t, c, pgqueue.EnqueueParams{Queue: q, Kind: "k", MaxAttempts: 1})
	_, err = pool.Exec(ctx, `
		UPDATE pgqueue_jobs SET state = 'running', attempts = 1, locked_by = 'dead-worker',
		       locked_until = now() - interval '1 second'
		WHERE id = $1`, last)
	if err != nil {
		t.Fatal(err)
	}

	m := pgqueue.NewMaintainer(pool, pgqueue.MaintainerConfig{Logger: quiet})
	n, err := m.ReapExpired(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if n < 2 {
		t.Fatalf("expected at least 2 reaped jobs, got %d", n)
	}
	job, _ := c.Get(ctx, id)
	if job.State != pgqueue.StatePending || !strings.Contains(*job.LastError, "dead-worker") {
		t.Fatalf("unexpected reaped job: %+v", job)
	}
	if job, _ := c.Get(ctx, last); job.State != pgqueue.StateDead {
		t.Fatalf("job without attempts left must be dead, got %s", job.State)
	}

	startWorker(t, pool, pgqueue.Config{Queue: q}, map[string]pgqueue.Handler{
		"k": func(context.Context, *pgqueue.Job) error { return nil },
	})
	job = waitState(t, c, id, pgqueue.StateSucceeded)
	if job.Attempts != 2 {
		t.Fatalf("attempts: got %d, want 2", job.Attempts)
	}
}

func TestHeartbeatKeepsLongJobAlive(t *testing.T) {
	pool := testPool(t)
	c := pgqueue.NewClient(pool)
	q := newQueue(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m := pgqueue.NewMaintainer(pool, pgqueue.MaintainerConfig{Interval: 50 * time.Millisecond, Retention: -1, Logger: quiet})
	go m.Run(ctx)

	var runs atomic.Int32
	startWorker(t, pool, pgqueue.Config{Queue: q, Lease: 600 * time.Millisecond}, map[string]pgqueue.Handler{
		"slow": func(ctx context.Context, job *pgqueue.Job) error {
			runs.Add(1)
			select {
			case <-time.After(2 * time.Second): // > 3 leases
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		},
	})
	id := enqueue(t, c, pgqueue.EnqueueParams{Queue: q, Kind: "slow"})
	job := waitState(t, c, id, pgqueue.StateSucceeded)
	if job.Attempts != 1 || runs.Load() != 1 {
		t.Fatalf("job was reclaimed despite heartbeats: attempts=%d runs=%d", job.Attempts, runs.Load())
	}
}

func TestLostLeaseCancelsHandler(t *testing.T) {
	pool := testPool(t)
	c := pgqueue.NewClient(pool)
	q := newQueue(t)
	ctx := context.Background()

	started := make(chan struct{})
	cancelled := make(chan error, 1)
	startWorker(t, pool, pgqueue.Config{Queue: q, Lease: 900 * time.Millisecond}, map[string]pgqueue.Handler{
		"k": func(ctx context.Context, job *pgqueue.Job) error {
			if job.Attempt > 1 {
				return nil
			}
			close(started)
			<-ctx.Done()
			cancelled <- context.Cause(ctx)
			return ctx.Err()
		},
	})
	id := enqueue(t, c, pgqueue.EnqueueParams{Queue: q, Kind: "k"})
	<-started

	// Pretend the worker was partitioned long enough for the lease to run out.
	if _, err := pool.Exec(ctx, "UPDATE pgqueue_jobs SET locked_until = now() - interval '1 second' WHERE id = $1", id); err != nil {
		t.Fatal(err)
	}
	if _, err := pgqueue.NewMaintainer(pool, pgqueue.MaintainerConfig{Logger: quiet}).ReapExpired(ctx); err != nil {
		t.Fatal(err)
	}

	select {
	case cause := <-cancelled:
		if cause == nil || !strings.Contains(cause.Error(), "lease lost") {
			t.Fatalf("unexpected cancel cause: %v", cause)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("handler was not cancelled after losing its lease")
	}
	job := waitState(t, c, id, pgqueue.StateSucceeded)
	if job.Attempts != 2 {
		t.Fatalf("attempts: got %d, want 2", job.Attempts)
	}
}

func TestShutdownReleasesUnfinishedJob(t *testing.T) {
	pool := testPool(t)
	c := pgqueue.NewClient(pool)
	q := newQueue(t)

	started := make(chan struct{})
	stop := startWorker(t, pool, pgqueue.Config{Queue: q, ShutdownTimeout: 200 * time.Millisecond}, map[string]pgqueue.Handler{
		"k": func(ctx context.Context, job *pgqueue.Job) error {
			close(started)
			<-ctx.Done()
			return ctx.Err()
		},
	})
	id := enqueue(t, c, pgqueue.EnqueueParams{Queue: q, Kind: "k"})
	<-started
	stop()

	job, err := c.Get(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if job.State != pgqueue.StatePending || job.Attempts != 0 || job.LockedBy != nil {
		t.Fatalf("job must be released without using an attempt: %+v", job)
	}
}

func TestShutdownWaitsForQuickJobs(t *testing.T) {
	pool := testPool(t)
	c := pgqueue.NewClient(pool)
	q := newQueue(t)

	started := make(chan struct{})
	stop := startWorker(t, pool, pgqueue.Config{Queue: q, ShutdownTimeout: 5 * time.Second}, map[string]pgqueue.Handler{
		"k": func(ctx context.Context, job *pgqueue.Job) error {
			close(started)
			time.Sleep(300 * time.Millisecond)
			return nil
		},
	})
	id := enqueue(t, c, pgqueue.EnqueueParams{Queue: q, Kind: "k"})
	<-started
	stop()
	if job, _ := c.Get(context.Background(), id); job.State != pgqueue.StateSucceeded {
		t.Fatalf("job in progress at shutdown should finish, got %s", job.State)
	}
}

func TestJobTimeout(t *testing.T) {
	pool := testPool(t)
	c := pgqueue.NewClient(pool)
	q := newQueue(t)

	startWorker(t, pool, pgqueue.Config{Queue: q, JobTimeout: 50 * time.Millisecond}, map[string]pgqueue.Handler{
		"k": func(ctx context.Context, job *pgqueue.Job) error {
			<-ctx.Done()
			return ctx.Err()
		},
	})
	id := enqueue(t, c, pgqueue.EnqueueParams{Queue: q, Kind: "k", MaxAttempts: 2})
	job := waitState(t, c, id, pgqueue.StateDead)
	if job.Attempts != 2 || !strings.Contains(*job.LastError, "deadline exceeded") {
		t.Fatalf("unexpected job: %+v", job)
	}
}

func TestAdminOperations(t *testing.T) {
	pool := testPool(t)
	c := pgqueue.NewClient(pool)
	q := newQueue(t)
	ctx := context.Background()

	if _, err := c.Get(ctx, 1<<62); !errors.Is(err, pgqueue.ErrNotFound) {
		t.Fatalf("get missing: %v", err)
	}
	if _, err := c.Cancel(ctx, 1<<62); !errors.Is(err, pgqueue.ErrNotFound) {
		t.Fatalf("cancel missing: %v", err)
	}

	id := enqueue(t, c, pgqueue.EnqueueParams{Queue: q, Kind: "k", UniqueKey: "key"})
	job, err := c.Cancel(ctx, id)
	if err != nil || job.State != pgqueue.StateCancelled {
		t.Fatalf("cancel: %+v %v", job, err)
	}
	if _, err := c.Cancel(ctx, id); !errors.Is(err, pgqueue.ErrInvalidState) {
		t.Fatalf("second cancel: %v", err)
	}

	// The key is free after cancel, so a new job can take it, and then the
	// old job cannot be retried without breaking uniqueness.
	enqueue(t, c, pgqueue.EnqueueParams{Queue: q, Kind: "k", UniqueKey: "key"})
	if _, err := c.Retry(ctx, id); !errors.Is(err, pgqueue.ErrDuplicate) {
		t.Fatalf("retry with taken key: %v", err)
	}

	pending := enqueue(t, c, pgqueue.EnqueueParams{Queue: q, Kind: "k"})
	if _, err := c.Retry(ctx, pending); !errors.Is(err, pgqueue.ErrInvalidState) {
		t.Fatalf("retry pending: %v", err)
	}

	jobs, err := c.List(ctx, pgqueue.ListFilter{Queue: q, State: pgqueue.StatePending})
	if err != nil || len(jobs) != 2 {
		t.Fatalf("list pending: %d jobs, %v", len(jobs), err)
	}
	page, err := c.List(ctx, pgqueue.ListFilter{Queue: q, AfterID: jobs[0].ID, Limit: 1})
	if err != nil || len(page) != 1 || page[0].ID != jobs[1].ID {
		t.Fatalf("pagination: %+v %v", page, err)
	}
}

func TestStats(t *testing.T) {
	pool := testPool(t)
	c := pgqueue.NewClient(pool)
	q := newQueue(t)
	ctx := context.Background()

	for range 3 {
		enqueue(t, c, pgqueue.EnqueueParams{Queue: q, Kind: "k"})
	}
	id := enqueue(t, c, pgqueue.EnqueueParams{Queue: q, Kind: "k"})
	if _, err := c.Cancel(ctx, id); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)

	stats, err := c.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range stats {
		if s.Queue != q {
			continue
		}
		if s.States[pgqueue.StatePending] != 3 || s.States[pgqueue.StateCancelled] != 1 {
			t.Fatalf("counts: %+v", s.States)
		}
		if s.LagSeconds < 0.04 {
			t.Fatalf("lag should reflect waiting jobs, got %f", s.LagSeconds)
		}
		return
	}
	t.Fatal("queue missing from stats")
}

func TestCleanupDeletesOldFinishedJobs(t *testing.T) {
	pool := testPool(t)
	c := pgqueue.NewClient(pool)
	q := newQueue(t)
	ctx := context.Background()

	old := enqueue(t, c, pgqueue.EnqueueParams{Queue: q, Kind: "k"})
	fresh := enqueue(t, c, pgqueue.EnqueueParams{Queue: q, Kind: "k"})
	pending := enqueue(t, c, pgqueue.EnqueueParams{Queue: q, Kind: "k"})
	for _, id := range []int64{old, fresh} {
		if _, err := c.Cancel(ctx, id); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := pool.Exec(ctx, "UPDATE pgqueue_jobs SET finished_at = now() - interval '2 hours' WHERE id = $1", old); err != nil {
		t.Fatal(err)
	}

	m := pgqueue.NewMaintainer(pool, pgqueue.MaintainerConfig{Retention: time.Hour, Logger: quiet})
	if _, err := m.Cleanup(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Get(ctx, old); !errors.Is(err, pgqueue.ErrNotFound) {
		t.Fatalf("old finished job should be deleted, got %v", err)
	}
	for _, id := range []int64{fresh, pending} {
		if _, err := c.Get(ctx, id); err != nil {
			t.Fatalf("job %d should be kept: %v", id, err)
		}
	}
}

func TestMigrateIsIdempotentAndConcurrent(t *testing.T) {
	pool := testPool(t)
	var wg sync.WaitGroup
	errs := make(chan error, 5)
	for range 5 {
		wg.Go(func() { errs <- pgqueue.Migrate(context.Background(), pool) })
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
}
