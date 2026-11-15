// Command bench measures end-to-end throughput: it enqueues N no-op jobs
// and runs W workers in this process until all of them succeed.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bekonat/pgqueue"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	var (
		dsn         = flag.String("dsn", os.Getenv("DATABASE_URL"), "postgres connection string")
		jobs        = flag.Int("jobs", 20000, "jobs to enqueue")
		workers     = flag.Int("workers", 4, "worker instances")
		concurrency = flag.Int("c", 16, "concurrency per worker")
	)
	flag.Parse()
	if err := run(*dsn, *jobs, *workers, *concurrency); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(dsn string, n, workers, concurrency int) error {
	ctx := context.Background()
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return err
	}
	cfg.MaxConns = int32(workers*2 + 4)
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return err
	}
	defer pool.Close()
	if err := pgqueue.Migrate(ctx, pool); err != nil {
		return err
	}
	queue := fmt.Sprintf("bench-%d", time.Now().UnixNano())

	start := time.Now()
	if err := enqueueAll(ctx, pool, queue, n); err != nil {
		return err
	}
	enqueueTook := time.Since(start)

	var done atomic.Int64
	all := make(chan struct{})
	runCtx, cancel := context.WithCancel(ctx)
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	var wg sync.WaitGroup
	start = time.Now()
	for range workers {
		w := pgqueue.NewWorker(pool, pgqueue.Config{Queue: queue, Concurrency: concurrency, Logger: quiet})
		w.Handle("noop", func(context.Context, *pgqueue.Job) error {
			if done.Add(1) == int64(n) {
				close(all)
			}
			return nil
		})
		wg.Go(func() { _ = w.Run(runCtx) })
	}
	<-all
	processTook := time.Since(start)
	cancel()
	wg.Wait()

	fmt.Printf("enqueue: %d jobs in %s (%.0f jobs/s, EnqueueMany in batches of 1000)\n",
		n, enqueueTook.Round(time.Millisecond), float64(n)/enqueueTook.Seconds())
	fmt.Printf("process: %d jobs in %s (%.0f jobs/s, %d workers x %d)\n",
		n, processTook.Round(time.Millisecond), float64(n)/processTook.Seconds(), workers, concurrency)
	return nil
}

func enqueueAll(ctx context.Context, pool *pgxpool.Pool, queue string, n int) error {
	client := pgqueue.NewClient(pool)
	batch := make([]pgqueue.EnqueueParams, 0, 1000)
	for i := 0; i < n; i += 1000 {
		batch = batch[:0]
		for range min(1000, n-i) {
			batch = append(batch, pgqueue.EnqueueParams{Queue: queue, Kind: "noop"})
		}
		if _, err := client.EnqueueMany(ctx, pool, batch); err != nil {
			return err
		}
	}
	return nil
}
