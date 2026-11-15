// Package metrics exposes pgqueue activity as Prometheus metrics.
package metrics

import (
	"context"
	"log/slog"
	"time"

	"github.com/bekonat/pgqueue"
	"github.com/prometheus/client_golang/prometheus"
)

// Metrics holds the collectors. Register it with a prometheus.Registerer.
type Metrics struct {
	processed *prometheus.CounterVec
	duration  *prometheus.HistogramVec
	reaped    prometheus.Counter
}

// New creates the job counters and histograms.
func New() *Metrics {
	return &Metrics{
		processed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "pgqueue_jobs_processed_total",
			Help: "Job executions by outcome.",
		}, []string{"queue", "kind", "outcome"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "pgqueue_job_duration_seconds",
			Help:    "Handler execution time.",
			Buckets: prometheus.ExponentialBuckets(0.005, 2.5, 12),
		}, []string{"queue", "kind"}),
		reaped: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "pgqueue_jobs_reaped_total",
			Help: "Jobs reclaimed after their lease expired.",
		}),
	}
}

// Register adds the collectors to r.
func (m *Metrics) Register(r prometheus.Registerer) {
	r.MustRegister(m.processed, m.duration, m.reaped)
}

// OnJobDone matches pgqueue.Config.OnJobDone.
func (m *Metrics) OnJobDone(job *pgqueue.Job, outcome pgqueue.Outcome, took time.Duration) {
	m.processed.WithLabelValues(job.Queue, job.Kind, string(outcome)).Inc()
	m.duration.WithLabelValues(job.Queue, job.Kind).Observe(took.Seconds())
}

// OnReap matches pgqueue.MaintainerConfig.OnReap.
func (m *Metrics) OnReap(n int) {
	m.reaped.Add(float64(n))
}

// QueueCollector reports queue depth and lag, queried on every scrape.
type QueueCollector struct {
	client *pgqueue.Client
	log    *slog.Logger
	jobs   *prometheus.Desc
	lag    *prometheus.Desc
	up     *prometheus.Desc
}

// NewQueueCollector creates a collector backed by client.Stats.
func NewQueueCollector(client *pgqueue.Client, log *slog.Logger) *QueueCollector {
	return &QueueCollector{
		client: client,
		log:    log,
		jobs:   prometheus.NewDesc("pgqueue_jobs", "Jobs in the table by queue and state.", []string{"queue", "state"}, nil),
		lag:    prometheus.NewDesc("pgqueue_queue_lag_seconds", "Age of the oldest runnable pending job.", []string{"queue"}, nil),
		up:     prometheus.NewDesc("pgqueue_stats_up", "1 if the last stats query succeeded.", nil, nil),
	}
}

func (c *QueueCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.jobs
	ch <- c.lag
	ch <- c.up
}

func (c *QueueCollector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stats, err := c.client.Stats(ctx)
	if err != nil {
		c.log.Error("stats query failed", "err", err)
		ch <- prometheus.MustNewConstMetric(c.up, prometheus.GaugeValue, 0)
		return
	}
	ch <- prometheus.MustNewConstMetric(c.up, prometheus.GaugeValue, 1)
	for _, s := range stats {
		for state, n := range s.States {
			ch <- prometheus.MustNewConstMetric(c.jobs, prometheus.GaugeValue, float64(n), s.Queue, string(state))
		}
		ch <- prometheus.MustNewConstMetric(c.lag, prometheus.GaugeValue, s.LagSeconds, s.Queue)
	}
}
