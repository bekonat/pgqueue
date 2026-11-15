package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/bekonat/pgqueue"
)

type fakeStore struct {
	lastEnqueue pgqueue.EnqueueParams
	lastFilter  pgqueue.ListFilter
	enqueueRes  pgqueue.EnqueueResult
	err         error
}

func (f *fakeStore) Enqueue(_ context.Context, p pgqueue.EnqueueParams) (pgqueue.EnqueueResult, error) {
	f.lastEnqueue = p
	return f.enqueueRes, f.err
}

func (f *fakeStore) Get(_ context.Context, id int64) (*pgqueue.JobInfo, error) {
	if f.err != nil {
		return nil, f.err
	}
	return &pgqueue.JobInfo{ID: id, State: pgqueue.StatePending}, nil
}

func (f *fakeStore) List(_ context.Context, flt pgqueue.ListFilter) ([]*pgqueue.JobInfo, error) {
	f.lastFilter = flt
	return []*pgqueue.JobInfo{{ID: 7}, {ID: 9}}, f.err
}

func (f *fakeStore) Stats(context.Context) ([]pgqueue.QueueStats, error) {
	return []pgqueue.QueueStats{{Queue: "default", States: map[pgqueue.State]int64{"pending": 2}}}, f.err
}

func (f *fakeStore) Retry(_ context.Context, id int64) (*pgqueue.JobInfo, error) {
	return &pgqueue.JobInfo{ID: id, State: pgqueue.StatePending}, f.err
}

func (f *fakeStore) Cancel(_ context.Context, id int64) (*pgqueue.JobInfo, error) {
	return &pgqueue.JobInfo{ID: id, State: pgqueue.StateCancelled}, f.err
}

func do(t *testing.T, h http.Handler, method, path, body string) (int, map[string]any) {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	var out map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("%s %s: invalid JSON response %q", method, path, rec.Body.String())
	}
	return rec.Code, out
}

func newHandler(s *fakeStore) http.Handler {
	return New(s, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func TestEnqueue(t *testing.T) {
	s := &fakeStore{enqueueRes: pgqueue.EnqueueResult{ID: 5}}
	h := newHandler(s)

	code, body := do(t, h, "POST", "/v1/jobs",
		`{"queue":"mail","kind":"email.send","payload":{"to":"x"},"priority":3,"delay_seconds":1.5,"unique_key":"u"}`)
	if code != http.StatusCreated || body["id"] != 5.0 {
		t.Fatalf("got %d %v", code, body)
	}
	p := s.lastEnqueue
	if p.Queue != "mail" || p.Kind != "email.send" || p.Priority != 3 || p.Delay != 1500*time.Millisecond || p.UniqueKey != "u" {
		t.Fatalf("params not passed through: %+v", p)
	}
	if string(p.Payload.(json.RawMessage)) != `{"to":"x"}` {
		t.Fatalf("payload: %s", p.Payload)
	}

	s.enqueueRes.Duplicate = true
	if code, _ := do(t, h, "POST", "/v1/jobs", `{"kind":"k"}`); code != http.StatusOK {
		t.Fatalf("duplicate should be 200, got %d", code)
	}
	if s.lastEnqueue.Payload != nil {
		t.Fatal("missing payload must stay nil so the library stores {}")
	}
}

func TestEnqueueValidation(t *testing.T) {
	tests := []struct {
		name, body string
		storeErr   error
		want       int
	}{
		{"not json", `{`, nil, 400},
		{"unknown field", `{"kind":"k","bogus":1}`, nil, 400},
		{"negative delay", `{"kind":"k","delay_seconds":-1}`, nil, 400},
		{"library validation", `{"kind":" "}`, fmt.Errorf("%w: kind is required", pgqueue.ErrInvalidParams), 400},
		{"db error", `{"kind":"k"}`, errors.New("connection refused"), 500},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			code, body := do(t, newHandler(&fakeStore{err: tt.storeErr}), "POST", "/v1/jobs", tt.body)
			if code != tt.want {
				t.Fatalf("got %d %v, want %d", code, body, tt.want)
			}
			if code == 500 && body["error"] != "internal error" {
				t.Fatalf("internal errors must not leak: %v", body)
			}
		})
	}
}

func TestGetAndErrors(t *testing.T) {
	tests := []struct {
		path string
		err  error
		want int
	}{
		{"/v1/jobs/3", nil, 200},
		{"/v1/jobs/abc", nil, 400},
		{"/v1/jobs/0", nil, 400},
		{"/v1/jobs/3", pgqueue.ErrNotFound, 404},
	}
	for _, tt := range tests {
		code, body := do(t, newHandler(&fakeStore{err: tt.err}), "GET", tt.path, "")
		if code != tt.want {
			t.Fatalf("GET %s: got %d %v, want %d", tt.path, code, body, tt.want)
		}
	}
}

func TestAdminOps(t *testing.T) {
	code, body := do(t, newHandler(&fakeStore{}), "POST", "/v1/jobs/4/cancel", "")
	if code != 200 || body["state"] != "cancelled" {
		t.Fatalf("cancel: %d %v", code, body)
	}
	code, _ = do(t, newHandler(&fakeStore{err: pgqueue.ErrInvalidState}), "POST", "/v1/jobs/4/cancel", "")
	if code != http.StatusConflict {
		t.Fatalf("cancel running: %d", code)
	}
	code, _ = do(t, newHandler(&fakeStore{err: pgqueue.ErrDuplicate}), "POST", "/v1/jobs/4/retry", "")
	if code != http.StatusConflict {
		t.Fatalf("retry duplicate: %d", code)
	}
}

func TestList(t *testing.T) {
	s := &fakeStore{}
	code, body := do(t, newHandler(s), "GET", "/v1/jobs?queue=q&state=dead&after_id=10&limit=2", "")
	if code != 200 || body["next_after_id"] != 9.0 {
		t.Fatalf("got %d %v", code, body)
	}
	if f := s.lastFilter; f.Queue != "q" || f.State != pgqueue.StateDead || f.AfterID != 10 || f.Limit != 2 {
		t.Fatalf("filter: %+v", f)
	}
	for _, q := range []string{"state=nope", "limit=0", "after_id=x"} {
		if code, _ := do(t, newHandler(s), "GET", "/v1/jobs?"+q, ""); code != 400 {
			t.Fatalf("%s: got %d", q, code)
		}
	}
}

func TestStats(t *testing.T) {
	code, body := do(t, newHandler(&fakeStore{}), "GET", "/v1/stats", "")
	if code != 200 || len(body["queues"].([]any)) != 1 {
		t.Fatalf("got %d %v", code, body)
	}
}
