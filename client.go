package pgqueue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

const (
	DefaultQueue       = "default"
	DefaultMaxAttempts = 10
)

// Client enqueues jobs and provides admin operations. It is safe for
// concurrent use.
type Client struct {
	db DBTX
}

// NewClient returns a Client that runs its queries on db, usually a
// *pgxpool.Pool.
func NewClient(db DBTX) *Client {
	return &Client{db: db}
}

// EnqueueParams describes a new job. Only Kind is required.
type EnqueueParams struct {
	Queue       string        // default "default"
	Kind        string        // selects the handler
	Payload     any           // marshalled to JSON; json.RawMessage is stored as is
	Priority    int           // higher runs first, range of smallint
	RunAt       time.Time     // schedule for a moment in time
	Delay       time.Duration // or schedule relative to the database clock
	MaxAttempts int           // default 10
	UniqueKey   string        // at most one pending/running job per (queue, key)
}

// EnqueueResult is returned by Enqueue.
type EnqueueResult struct {
	ID int64 `json:"id"`
	// Duplicate is true when a job with the same unique key was already
	// pending or running; ID then refers to that existing job.
	Duplicate bool `json:"duplicate"`
}

const insertSQL = `
INSERT INTO pgqueue_jobs (queue, kind, payload, priority, max_attempts, run_at, unique_key)
VALUES ($1, $2, $3, $4, $5,
        COALESCE($6::timestamptz, now() + make_interval(secs => $7::float8)),
        $8)
ON CONFLICT (queue, unique_key)
    WHERE unique_key IS NOT NULL AND state IN ('pending', 'running')
    DO NOTHING
RETURNING id, run_at <= now()`

const findActiveSQL = `
SELECT id FROM pgqueue_jobs
WHERE queue = $1 AND unique_key = $2 AND state IN ('pending', 'running')`

// Enqueue inserts a job using the client's connection.
func (c *Client) Enqueue(ctx context.Context, p EnqueueParams) (EnqueueResult, error) {
	return c.EnqueueTx(ctx, c.db, p)
}

// EnqueueTx inserts a job using db, which may be a transaction. The job
// becomes visible to workers only when the transaction commits, so
// business data and the job that processes it are written atomically.
func (c *Client) EnqueueTx(ctx context.Context, db DBTX, p EnqueueParams) (EnqueueResult, error) {
	args, err := p.normalize()
	if err != nil {
		return EnqueueResult{}, err
	}

	// A conflicting job can finish between our INSERT and SELECT, so try
	// the pair a few times before giving up.
	for range 3 {
		var (
			id       int64
			runnable bool
		)
		err := db.QueryRow(ctx, insertSQL, args...).Scan(&id, &runnable)
		switch {
		case err == nil:
			if runnable {
				// Delivered on commit, so a rolled back tx wakes nobody.
				if _, err := db.Exec(ctx, "SELECT pg_notify($1, $2)", notifyChannel, args[0]); err != nil {
					return EnqueueResult{}, fmt.Errorf("pgqueue: notify: %w", err)
				}
			}
			return EnqueueResult{ID: id}, nil
		case !errors.Is(err, pgx.ErrNoRows):
			return EnqueueResult{}, fmt.Errorf("pgqueue: enqueue: %w", err)
		}

		err = db.QueryRow(ctx, findActiveSQL, args[0], args[7]).Scan(&id)
		switch {
		case err == nil:
			return EnqueueResult{ID: id, Duplicate: true}, nil
		case !errors.Is(err, pgx.ErrNoRows):
			return EnqueueResult{}, fmt.Errorf("pgqueue: enqueue: %w", err)
		}
	}
	return EnqueueResult{}, errors.New("pgqueue: enqueue: unique key kept changing state, giving up")
}

const insertManySQL = `
INSERT INTO pgqueue_jobs (queue, kind, payload, priority, max_attempts, run_at)
SELECT q, k, p, pr, ma, COALESCE(ra, now() + make_interval(secs => d))
FROM unnest($1::text[], $2::text[], $3::jsonb[], $4::smallint[], $5::int[], $6::timestamptz[], $7::float8[])
    AS t(q, k, p, pr, ma, ra, d)
RETURNING id`

// EnqueueMany inserts many jobs with one statement, which is much faster
// than calling Enqueue in a loop for fan-out workloads. Unique keys are not
// supported here; use Enqueue for deduplicated jobs. It returns the ids of
// the new jobs.
func (c *Client) EnqueueMany(ctx context.Context, db DBTX, ps []EnqueueParams) ([]int64, error) {
	if len(ps) == 0 {
		return nil, nil
	}
	n := len(ps)
	var (
		queues   = make([]string, n)
		kinds    = make([]string, n)
		payloads = make([]string, n)
		prios    = make([]int16, n)
		attempts = make([]int32, n)
		runAts   = make([]*time.Time, n)
		delays   = make([]float64, n)
		wake     = map[string]bool{}
	)
	for i := range ps {
		if ps[i].UniqueKey != "" {
			return nil, fmt.Errorf("%w: EnqueueMany does not support unique keys", ErrInvalidParams)
		}
		args, err := ps[i].normalize()
		if err != nil {
			return nil, fmt.Errorf("job %d: %w", i, err)
		}
		queues[i], kinds[i], payloads[i] = args[0].(string), args[1].(string), string(args[2].([]byte))
		prios[i], attempts[i] = int16(args[3].(int)), int32(args[4].(int))
		runAts[i], delays[i] = args[5].(*time.Time), args[6].(float64)
		if runAts[i] == nil && delays[i] <= 0 {
			wake[queues[i]] = true
		}
	}

	rows, err := db.Query(ctx, insertManySQL, queues, kinds, payloads, prios, attempts, runAts, delays)
	if err != nil {
		return nil, fmt.Errorf("pgqueue: enqueue many: %w", err)
	}
	ids, err := pgx.CollectRows(rows, pgx.RowTo[int64])
	if err != nil {
		return nil, fmt.Errorf("pgqueue: enqueue many: %w", err)
	}
	for q := range wake {
		if _, err := db.Exec(ctx, "SELECT pg_notify($1, $2)", notifyChannel, q); err != nil {
			return nil, fmt.Errorf("pgqueue: notify: %w", err)
		}
	}
	return ids, nil
}

func (p *EnqueueParams) normalize() ([]any, error) {
	if strings.TrimSpace(p.Kind) == "" {
		return nil, fmt.Errorf("%w: kind is required", ErrInvalidParams)
	}
	queue := p.Queue
	if queue == "" {
		queue = DefaultQueue
	}
	if p.Priority < math.MinInt16 || p.Priority > math.MaxInt16 {
		return nil, fmt.Errorf("%w: priority %d out of range", ErrInvalidParams, p.Priority)
	}
	maxAttempts := p.MaxAttempts
	if maxAttempts == 0 {
		maxAttempts = DefaultMaxAttempts
	}
	if maxAttempts < 0 {
		return nil, fmt.Errorf("%w: max attempts must be positive", ErrInvalidParams)
	}

	var payload []byte
	switch v := p.Payload.(type) {
	case nil:
		payload = []byte("{}")
	case json.RawMessage:
		if !json.Valid(v) {
			return nil, fmt.Errorf("%w: payload is not valid JSON", ErrInvalidParams)
		}
		payload = v
	default:
		b, err := json.Marshal(v)
		if err != nil {
			return nil, fmt.Errorf("%w: marshal payload: %w", ErrInvalidParams, err)
		}
		payload = b
	}

	var runAt *time.Time
	if !p.RunAt.IsZero() {
		runAt = &p.RunAt
	}
	var uniqueKey *string
	if p.UniqueKey != "" {
		uniqueKey = &p.UniqueKey
	}
	return []any{queue, p.Kind, payload, p.Priority, maxAttempts, runAt, p.Delay.Seconds(), uniqueKey}, nil
}

const jobColumns = `id, queue, kind, payload, state, priority, attempts, max_attempts, run_at,
locked_by, locked_until, last_error, unique_key, created_at, updated_at, finished_at`

func scanJobInfo(row pgx.Row) (*JobInfo, error) {
	var j JobInfo
	err := row.Scan(&j.ID, &j.Queue, &j.Kind, &j.Payload, &j.State, &j.Priority, &j.Attempts,
		&j.MaxAttempts, &j.RunAt, &j.LockedBy, &j.LockedUntil, &j.LastError, &j.UniqueKey,
		&j.CreatedAt, &j.UpdatedAt, &j.FinishedAt)
	if err != nil {
		return nil, err
	}
	return &j, nil
}

// Get returns a job by id.
func (c *Client) Get(ctx context.Context, id int64) (*JobInfo, error) {
	j, err := scanJobInfo(c.db.QueryRow(ctx, "SELECT "+jobColumns+" FROM pgqueue_jobs WHERE id = $1", id))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("pgqueue: get: %w", err)
	}
	return j, nil
}

// ListFilter narrows List. Zero values mean "any".
type ListFilter struct {
	Queue   string
	Kind    string
	State   State
	AfterID int64 // keyset pagination: return jobs with id > AfterID
	Limit   int   // default 50, max 500
}

// List returns jobs ordered by id.
func (c *Client) List(ctx context.Context, f ListFilter) ([]*JobInfo, error) {
	if f.Limit <= 0 {
		f.Limit = 50
	}
	f.Limit = min(f.Limit, 500)
	rows, err := c.db.Query(ctx, `
		SELECT `+jobColumns+` FROM pgqueue_jobs
		WHERE ($1 = '' OR queue = $1)
		  AND ($2 = '' OR kind = $2)
		  AND ($3 = '' OR state = $3)
		  AND id > $4
		ORDER BY id
		LIMIT $5`, f.Queue, f.Kind, string(f.State), f.AfterID, f.Limit)
	if err != nil {
		return nil, fmt.Errorf("pgqueue: list: %w", err)
	}
	defer rows.Close()
	jobs := []*JobInfo{}
	for rows.Next() {
		j, err := scanJobInfo(rows)
		if err != nil {
			return nil, fmt.Errorf("pgqueue: list: %w", err)
		}
		jobs = append(jobs, j)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("pgqueue: list: %w", err)
	}
	return jobs, nil
}

// QueueStats summarises one queue.
type QueueStats struct {
	Queue  string          `json:"queue"`
	States map[State]int64 `json:"states"`
	// LagSeconds is how long the oldest runnable pending job has been
	// waiting past its run_at. A growing lag means workers cannot keep up.
	LagSeconds float64 `json:"lag_seconds"`
}

// Stats returns per-queue job counts and lag. It counts the whole table,
// so call it from dashboards and metrics scrapes, not from hot paths.
func (c *Client) Stats(ctx context.Context) ([]QueueStats, error) {
	rows, err := c.db.Query(ctx, `
		SELECT queue, state, count(*),
		       COALESCE(EXTRACT(EPOCH FROM now() - min(run_at)
		                FILTER (WHERE state = 'pending' AND run_at <= now())), 0)::float8
		FROM pgqueue_jobs
		GROUP BY queue, state
		ORDER BY queue, state`)
	if err != nil {
		return nil, fmt.Errorf("pgqueue: stats: %w", err)
	}
	defer rows.Close()

	stats := []QueueStats{}
	for rows.Next() {
		var (
			queue string
			state State
			n     int64
			lag   float64
		)
		if err := rows.Scan(&queue, &state, &n, &lag); err != nil {
			return nil, fmt.Errorf("pgqueue: stats: %w", err)
		}
		if len(stats) == 0 || stats[len(stats)-1].Queue != queue {
			stats = append(stats, QueueStats{Queue: queue, States: map[State]int64{}})
		}
		s := &stats[len(stats)-1]
		s.States[state] = n
		s.LagSeconds = max(s.LagSeconds, lag)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("pgqueue: stats: %w", err)
	}
	return stats, nil
}

// Retry moves a dead or cancelled job back to pending with a fresh
// attempt counter.
func (c *Client) Retry(ctx context.Context, id int64) (*JobInfo, error) {
	j, err := scanJobInfo(c.db.QueryRow(ctx, `
		UPDATE pgqueue_jobs
		SET state = 'pending', attempts = 0, run_at = now(), finished_at = NULL, updated_at = now()
		WHERE id = $1 AND state IN ('dead', 'cancelled')
		RETURNING `+jobColumns, id))
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return nil, ErrDuplicate
		}
		return nil, c.explainMiss(ctx, id, err)
	}
	if _, err := c.db.Exec(ctx, "SELECT pg_notify($1, $2)", notifyChannel, j.Queue); err != nil {
		return nil, fmt.Errorf("pgqueue: notify: %w", err)
	}
	return j, nil
}

// Cancel moves a pending job to cancelled. Running jobs cannot be
// cancelled because the handler may already have side effects.
func (c *Client) Cancel(ctx context.Context, id int64) (*JobInfo, error) {
	j, err := scanJobInfo(c.db.QueryRow(ctx, `
		UPDATE pgqueue_jobs
		SET state = 'cancelled', finished_at = now(), updated_at = now()
		WHERE id = $1 AND state = 'pending'
		RETURNING `+jobColumns, id))
	if err != nil {
		return nil, c.explainMiss(ctx, id, err)
	}
	return j, nil
}

// explainMiss turns "UPDATE matched nothing" into ErrNotFound or
// ErrInvalidState.
func (c *Client) explainMiss(ctx context.Context, id int64, err error) error {
	if !errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("pgqueue: %w", err)
	}
	var exists bool
	if err := c.db.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM pgqueue_jobs WHERE id = $1)", id).Scan(&exists); err != nil {
		return fmt.Errorf("pgqueue: %w", err)
	}
	if !exists {
		return ErrNotFound
	}
	return ErrInvalidState
}
