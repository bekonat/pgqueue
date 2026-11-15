package pgqueue

import (
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func TestExponentialBackoff(t *testing.T) {
	b := ExponentialBackoff(time.Second, time.Minute)
	tests := []struct {
		attempt  int
		min, max time.Duration
	}{
		{0, 500 * time.Millisecond, time.Second},
		{1, 500 * time.Millisecond, time.Second},
		{2, time.Second, 2 * time.Second},
		{3, 2 * time.Second, 4 * time.Second},
		{6, 16 * time.Second, 32 * time.Second},
		{7, 30 * time.Second, time.Minute}, // 64s is over the cap
		{100, 30 * time.Second, time.Minute},
		{1 << 20, 30 * time.Second, time.Minute},
	}
	for _, tt := range tests {
		for range 200 {
			d := b(tt.attempt)
			if d < tt.min || d > tt.max {
				t.Fatalf("attempt %d: got %s, want [%s, %s]", tt.attempt, d, tt.min, tt.max)
			}
		}
	}
}

func TestExponentialBackoffHasJitter(t *testing.T) {
	b := ExponentialBackoff(time.Second, time.Hour)
	seen := map[time.Duration]bool{}
	for range 50 {
		seen[b(5)] = true
	}
	if len(seen) < 10 {
		t.Fatalf("expected jittered delays, got %d distinct values", len(seen))
	}
}

func TestNormalize(t *testing.T) {
	tests := []struct {
		name    string
		p       EnqueueParams
		wantErr bool
		check   func(t *testing.T, args []any)
	}{
		{name: "missing kind", p: EnqueueParams{}, wantErr: true},
		{name: "blank kind", p: EnqueueParams{Kind: "  "}, wantErr: true},
		{name: "priority too high", p: EnqueueParams{Kind: "k", Priority: 40000}, wantErr: true},
		{name: "negative attempts", p: EnqueueParams{Kind: "k", MaxAttempts: -1}, wantErr: true},
		{name: "invalid raw json", p: EnqueueParams{Kind: "k", Payload: json.RawMessage("{")}, wantErr: true},
		{name: "unmarshalable payload", p: EnqueueParams{Kind: "k", Payload: make(chan int)}, wantErr: true},
		{
			name: "defaults",
			p:    EnqueueParams{Kind: "k"},
			check: func(t *testing.T, args []any) {
				if args[0] != DefaultQueue || string(args[2].([]byte)) != "{}" || args[4] != DefaultMaxAttempts {
					t.Fatalf("unexpected defaults: %v", args)
				}
				if args[5].(*time.Time) != nil || args[7].(*string) != nil {
					t.Fatalf("run_at and unique_key should be NULL: %v", args)
				}
			},
		},
		{
			name: "struct payload and unique key",
			p:    EnqueueParams{Kind: "k", Payload: map[string]int{"a": 1}, UniqueKey: "u", Delay: 1500 * time.Millisecond},
			check: func(t *testing.T, args []any) {
				if string(args[2].([]byte)) != `{"a":1}` || *args[7].(*string) != "u" || args[6] != 1.5 {
					t.Fatalf("unexpected args: %v", args)
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			args, err := tt.p.normalize()
			if tt.wantErr {
				if !errors.Is(err, ErrInvalidParams) {
					t.Fatalf("want ErrInvalidParams, got %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			tt.check(t, args)
		})
	}
}

func TestErrorWrappers(t *testing.T) {
	base := errors.New("boom")
	p := Permanent(base)
	if !errors.Is(p, base) {
		t.Fatal("Permanent must unwrap to the original error")
	}
	if !errors.As(p, new(*permanentError)) {
		t.Fatal("Permanent must be detectable")
	}
	if Permanent(nil) == nil {
		t.Fatal("Permanent(nil) must still be an error")
	}
	var s *snoozeError
	if !errors.As(Snooze(-time.Second), &s) || s.d != 0 {
		t.Fatal("negative snooze must clamp to zero")
	}
}
