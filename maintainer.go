package pgqueue

import (
	"context"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// MaintainerConfig tunes a Maintainer. Zero values are replaced with defaults.
type MaintainerConfig struct {
	Interval time.Duration // how often to run, default 5s
	// Retention is how long finished jobs are kept. Default 24h;
	// a negative value disables cleanup.
	Retention time.Duration
	Logger    *slog.Logger
	// OnReap, if set, is called with the number of expired leases reclaimed.
	OnReap func(n int)
}

// Maintainer returns jobs with expired leases to the queue and deletes old
// finished jobs. Several maintainers may run at once; they skip rows the
// others have locked.
type Maintainer struct {
	pool *pgxpool.Pool
	cfg  MaintainerConfig
	log  *slog.Logger
}

// NewMaintainer creates a Maintainer.
func NewMaintainer(pool *pgxpool.Pool, cfg MaintainerConfig) *Maintainer {
	if cfg.Interval <= 0 {
		cfg.Interval = 5 * time.Second
	}
	if cfg.Retention == 0 {
		cfg.Retention = 24 * time.Hour
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &Maintainer{pool: pool, cfg: cfg, log: cfg.Logger.With("component", "maintainer")}
}

// Run performs maintenance every Interval until ctx is cancelled.
func (m *Maintainer) Run(ctx context.Context) error {
	t := time.NewTicker(m.cfg.Interval)
	defer t.Stop()
	for {
		if n, err := m.ReapExpired(ctx); err != nil {
			if ctx.Err() == nil {
				m.log.Error("reap failed", "err", err)
			}
		} else if n > 0 {
			m.log.Warn("reclaimed jobs with expired leases", "count", n)
			if m.cfg.OnReap != nil {
				m.cfg.OnReap(n)
			}
		}
		if m.cfg.Retention > 0 {
			if n, err := m.Cleanup(ctx); err != nil {
				if ctx.Err() == nil {
					m.log.Error("cleanup failed", "err", err)
				}
			} else if n > 0 {
				m.log.Info("deleted finished jobs", "count", n)
			}
		}

		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
	}
}

const reapSQL = `
WITH expired AS (
    SELECT id FROM pgqueue_jobs
    WHERE state = 'running' AND locked_until < now()
    ORDER BY locked_until
    LIMIT 1000
    FOR UPDATE SKIP LOCKED
)
UPDATE pgqueue_jobs j
SET state = CASE WHEN j.attempts >= j.max_attempts THEN 'dead' ELSE 'pending' END,
    finished_at = CASE WHEN j.attempts >= j.max_attempts THEN now() END,
    run_at = now(),
    last_error = 'lease expired: worker ' || COALESCE(j.locked_by, '?') || ' stopped sending heartbeats',
    locked_by = NULL,
    locked_until = NULL,
    updated_at = now()
FROM expired
WHERE j.id = expired.id
RETURNING j.queue, j.state`

// ReapExpired reclaims jobs whose worker stopped heartbeating (crashed,
// lost the network, was OOM-killed). A job that has used all its attempts
// becomes dead; otherwise it is pending again right away.
func (m *Maintainer) ReapExpired(ctx context.Context) (int, error) {
	rows, err := m.pool.Query(ctx, reapSQL)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	n := 0
	wake := map[string]bool{}
	for rows.Next() {
		var queue string
		var state State
		if err := rows.Scan(&queue, &state); err != nil {
			return n, err
		}
		n++
		if state == StatePending {
			wake[queue] = true
		}
	}
	if err := rows.Err(); err != nil {
		return n, err
	}
	for queue := range wake {
		if _, err := m.pool.Exec(ctx, "SELECT pg_notify($1, $2)", notifyChannel, queue); err != nil {
			return n, err
		}
	}
	return n, nil
}

const cleanupBatch = 5000

// Cleanup deletes finished jobs older than the retention period in batches,
// so a large backlog does not hold one long transaction.
func (m *Maintainer) Cleanup(ctx context.Context) (int64, error) {
	var total int64
	for {
		tag, err := m.pool.Exec(ctx, `
			DELETE FROM pgqueue_jobs WHERE id IN (
			    SELECT id FROM pgqueue_jobs
			    WHERE state IN ('succeeded', 'dead', 'cancelled')
			      AND finished_at < now() - make_interval(secs => $1::float8)
			    LIMIT $2
			    FOR UPDATE SKIP LOCKED
			)`, m.cfg.Retention.Seconds(), cleanupBatch)
		if err != nil {
			return total, err
		}
		total += tag.RowsAffected()
		if tag.RowsAffected() < cleanupBatch {
			return total, nil
		}
	}
}
