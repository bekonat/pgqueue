// Package pgqueue is a background job queue backed by PostgreSQL.
//
// Jobs are rows in a single table. Workers claim them with
// SELECT ... FOR UPDATE SKIP LOCKED, hold a time-limited lease that is
// extended by a heartbeat, and report the result with a fenced UPDATE.
// Delivery is at-least-once: a job whose worker dies is returned to the
// queue once its lease expires, so handlers must be idempotent.
package pgqueue

import (
	"encoding/json"
	"time"
)

// State is the lifecycle state of a job.
type State string

const (
	StatePending   State = "pending"
	StateRunning   State = "running"
	StateSucceeded State = "succeeded"
	StateDead      State = "dead"
	StateCancelled State = "cancelled"
)

// Valid reports whether s is a known state.
func (s State) Valid() bool {
	switch s {
	case StatePending, StateRunning, StateSucceeded, StateDead, StateCancelled:
		return true
	}
	return false
}

// Job is what a handler receives.
type Job struct {
	ID          int64
	Queue       string
	Kind        string
	Payload     json.RawMessage
	Priority    int
	Attempt     int // 1 on the first run
	MaxAttempts int
	RunAt       time.Time
	CreatedAt   time.Time
}

// Decode unmarshals the job payload into v.
func (j *Job) Decode(v any) error {
	return json.Unmarshal(j.Payload, v)
}

// JobInfo is the full row, returned by the admin methods of Client.
type JobInfo struct {
	ID          int64           `json:"id"`
	Queue       string          `json:"queue"`
	Kind        string          `json:"kind"`
	Payload     json.RawMessage `json:"payload"`
	State       State           `json:"state"`
	Priority    int             `json:"priority"`
	Attempts    int             `json:"attempts"`
	MaxAttempts int             `json:"max_attempts"`
	RunAt       time.Time       `json:"run_at"`
	LockedBy    *string         `json:"locked_by,omitempty"`
	LockedUntil *time.Time      `json:"locked_until,omitempty"`
	LastError   *string         `json:"last_error,omitempty"`
	UniqueKey   *string         `json:"unique_key,omitempty"`
	CreatedAt   time.Time       `json:"created_at"`
	UpdatedAt   time.Time       `json:"updated_at"`
	FinishedAt  *time.Time      `json:"finished_at,omitempty"`
}

// Outcome describes what happened to a job after one execution.
type Outcome string

const (
	OutcomeSucceeded Outcome = "succeeded"  // handler returned nil
	OutcomeRetried   Outcome = "retried"    // handler failed, job rescheduled with backoff
	OutcomeDead      Outcome = "dead"       // attempts exhausted or Permanent error
	OutcomeSnoozed   Outcome = "snoozed"    // handler returned Snooze
	OutcomeReleased  Outcome = "released"   // worker shut down mid-job, job returned to queue
	OutcomeLeaseLost Outcome = "lease_lost" // lease expired, another worker may own the job
)
