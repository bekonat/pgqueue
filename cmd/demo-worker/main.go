// Command demo-worker processes two example job kinds so the system can be
// tried end to end: "email.send" fails randomly to show retries, and
// "report.build" is slow to show leases and heartbeats.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/bekonat/pgqueue"
	"github.com/bekonat/pgqueue/internal/metrics"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type emailPayload struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
}

type reportPayload struct {
	UserID  int64 `json:"user_id"`
	Seconds int   `json:"seconds"`
}

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(log); err != nil {
		log.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.New(ctx, env("DATABASE_URL", "postgres://pgqueue:pgqueue@localhost:5432/pgqueue?sslmode=disable"))
	if err != nil {
		return err
	}
	defer pool.Close()
	if err := pgqueue.Migrate(ctx, pool); err != nil {
		return err
	}

	concurrency, _ := strconv.Atoi(env("CONCURRENCY", "8"))
	m := metrics.New()
	reg := prometheus.NewRegistry()
	m.Register(reg)

	w := pgqueue.NewWorker(pool, pgqueue.Config{
		Queue:       env("QUEUE", pgqueue.DefaultQueue),
		Concurrency: concurrency,
		JobTimeout:  2 * time.Minute,
		Logger:      log,
		OnJobDone:   m.OnJobDone,
	})
	w.Handle("email.send", sendEmail)
	w.Handle("report.build", buildReport)

	go func() {
		addr := env("METRICS_ADDR", ":9090")
		log.Info("metrics listening", "addr", addr)
		srv := &http.Server{Addr: addr, Handler: promhttp.HandlerFor(reg, promhttp.HandlerOpts{}), ReadHeaderTimeout: 5 * time.Second}
		if err := srv.ListenAndServe(); err != nil {
			log.Error("metrics server", "err", err)
		}
	}()

	return w.Run(ctx)
}

var errSMTPUnavailable = errors.New("smtp: 421 service not available")

func sendEmail(ctx context.Context, job *pgqueue.Job) error {
	var p emailPayload
	if err := job.Decode(&p); err != nil {
		return pgqueue.Permanent(fmt.Errorf("decode payload: %w", err))
	}
	if p.To == "" {
		return pgqueue.Permanent(errors.New("payload has no recipient"))
	}
	select {
	case <-time.After(time.Duration(20+rand.IntN(80)) * time.Millisecond):
	case <-ctx.Done():
		return ctx.Err()
	}
	if rand.IntN(100) < 20 {
		return errSMTPUnavailable
	}
	return nil
}

func buildReport(ctx context.Context, job *pgqueue.Job) error {
	var p reportPayload
	if err := job.Decode(&p); err != nil {
		return pgqueue.Permanent(err)
	}
	select {
	case <-time.After(time.Duration(max(p.Seconds, 1)) * time.Second):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
