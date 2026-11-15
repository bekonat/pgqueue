package pgqueue

import (
	"context"
	_ "embed"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed schema.sql
var schemaSQL string

// notifyChannel is the LISTEN/NOTIFY channel; the payload is the queue name.
const notifyChannel = "pgqueue_jobs"

// migrateLockID is an arbitrary key for pg_advisory_xact_lock so that
// several processes starting at once do not race on CREATE INDEX.
const migrateLockID = 0x7067717565756531

// DBTX is satisfied by *pgxpool.Pool, *pgx.Conn and pgx.Tx, which lets
// callers enqueue jobs inside their own transaction.
type DBTX interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Migrate creates the jobs table and its indexes. It is idempotent and safe
// to call concurrently from several processes.
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	return pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", int64(migrateLockID)); err != nil {
			return fmt.Errorf("pgqueue: migrate lock: %w", err)
		}
		// No arguments, so pgx uses the simple protocol and accepts
		// several statements in one call.
		if _, err := tx.Exec(ctx, schemaSQL); err != nil {
			return fmt.Errorf("pgqueue: migrate: %w", err)
		}
		return nil
	})
}
