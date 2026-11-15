package pgqueue

import (
	"errors"
	"fmt"
	"time"
)

var (
	// ErrNotFound is returned when a job with the given id does not exist.
	ErrNotFound = errors.New("pgqueue: job not found")
	// ErrInvalidState is returned when an admin operation does not apply
	// to the job's current state (e.g. cancelling a running job).
	ErrInvalidState = errors.New("pgqueue: operation not allowed in current job state")
	// ErrDuplicate is returned by Retry when another active job already
	// holds the same unique key.
	ErrDuplicate = errors.New("pgqueue: active job with the same unique key exists")
	// ErrInvalidParams wraps validation errors from Enqueue.
	ErrInvalidParams = errors.New("pgqueue: invalid enqueue params")
)

type permanentError struct{ err error }

func (e *permanentError) Error() string { return "permanent: " + e.err.Error() }
func (e *permanentError) Unwrap() error { return e.err }

// Permanent wraps err so the job is moved to the dead state immediately,
// without further retries. Use it for errors that retrying cannot fix,
// such as a malformed payload.
func Permanent(err error) error {
	if err == nil {
		err = errors.New("permanent failure")
	}
	return &permanentError{err: err}
}

type snoozeError struct{ d time.Duration }

func (e *snoozeError) Error() string { return fmt.Sprintf("snoozed for %s", e.d) }

// Snooze reschedules the job to run again after d without counting the
// current run as a failed attempt. Useful when a downstream dependency
// asks the caller to come back later.
func Snooze(d time.Duration) error {
	return &snoozeError{d: max(d, 0)}
}
