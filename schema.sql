CREATE TABLE IF NOT EXISTS pgqueue_jobs (
    id           bigserial   PRIMARY KEY,
    queue        text        NOT NULL DEFAULT 'default',
    kind         text        NOT NULL,
    payload      jsonb       NOT NULL DEFAULT '{}',
    state        text        NOT NULL DEFAULT 'pending'
                 CHECK (state IN ('pending', 'running', 'succeeded', 'dead', 'cancelled')),
    priority     smallint    NOT NULL DEFAULT 0,
    attempts     integer     NOT NULL DEFAULT 0,
    max_attempts integer     NOT NULL DEFAULT 10 CHECK (max_attempts > 0),
    run_at       timestamptz NOT NULL DEFAULT now(),
    locked_by    text,
    locked_until timestamptz,
    last_error   text,
    unique_key   text,
    created_at   timestamptz NOT NULL DEFAULT now(),
    updated_at   timestamptz NOT NULL DEFAULT now(),
    finished_at  timestamptz
);

-- Fetch path: only pending rows are indexed, so the index stays small
-- even when the table holds millions of finished jobs.
CREATE INDEX IF NOT EXISTS pgqueue_jobs_fetch_idx
    ON pgqueue_jobs (queue, priority DESC, run_at, id)
    WHERE state = 'pending';

-- Reaper path: running jobs ordered by lease expiry.
CREATE INDEX IF NOT EXISTS pgqueue_jobs_lease_idx
    ON pgqueue_jobs (locked_until)
    WHERE state = 'running';

-- Cleanup path.
CREATE INDEX IF NOT EXISTS pgqueue_jobs_finished_idx
    ON pgqueue_jobs (finished_at)
    WHERE state IN ('succeeded', 'dead', 'cancelled');

-- Deduplication: at most one active job per (queue, unique_key).
-- Finished jobs do not block a new job with the same key.
CREATE UNIQUE INDEX IF NOT EXISTS pgqueue_jobs_unique_idx
    ON pgqueue_jobs (queue, unique_key)
    WHERE unique_key IS NOT NULL AND state IN ('pending', 'running');
