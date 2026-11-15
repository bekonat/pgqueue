package pgqueue

import (
	"math/rand/v2"
	"time"
)

// BackoffFunc returns the delay before the next try, given the number of
// the attempt that just failed (starting at 1).
type BackoffFunc func(attempt int) time.Duration

// ExponentialBackoff doubles the delay on every attempt, starting at base
// and capped at limit. Half of the delay is randomised ("equal jitter") so
// that jobs which failed together do not all retry at the same moment.
func ExponentialBackoff(base, limit time.Duration) BackoffFunc {
	return func(attempt int) time.Duration {
		d := limit
		if attempt < 1 {
			attempt = 1
		}
		// base << 62 overflows; past ~40 doublings we are at the cap anyway.
		if attempt <= 40 {
			if exp := base << (attempt - 1); exp > 0 && exp < limit {
				d = exp
			}
		}
		half := d / 2
		if half <= 0 {
			return d
		}
		return half + rand.N(half+1)
	}
}
