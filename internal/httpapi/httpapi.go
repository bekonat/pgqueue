// Package httpapi is a small REST API for enqueueing and inspecting jobs.
package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/bekonat/pgqueue"
)

const maxBodyBytes = 1 << 20

// Store is the subset of *pgqueue.Client the API needs.
type Store interface {
	Enqueue(ctx context.Context, p pgqueue.EnqueueParams) (pgqueue.EnqueueResult, error)
	Get(ctx context.Context, id int64) (*pgqueue.JobInfo, error)
	List(ctx context.Context, f pgqueue.ListFilter) ([]*pgqueue.JobInfo, error)
	Stats(ctx context.Context) ([]pgqueue.QueueStats, error)
	Retry(ctx context.Context, id int64) (*pgqueue.JobInfo, error)
	Cancel(ctx context.Context, id int64) (*pgqueue.JobInfo, error)
}

// Handler serves the API.
type Handler struct {
	store Store
	log   *slog.Logger
	mux   *http.ServeMux
}

// New builds the routes.
func New(store Store, log *slog.Logger) *Handler {
	h := &Handler{store: store, log: log, mux: http.NewServeMux()}
	h.mux.HandleFunc("POST /v1/jobs", h.enqueue)
	h.mux.HandleFunc("GET /v1/jobs", h.list)
	h.mux.HandleFunc("GET /v1/jobs/{id}", h.get)
	h.mux.HandleFunc("POST /v1/jobs/{id}/retry", h.retry)
	h.mux.HandleFunc("POST /v1/jobs/{id}/cancel", h.cancel)
	h.mux.HandleFunc("GET /v1/stats", h.stats)
	return h
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mux.ServeHTTP(w, r)
}

type enqueueRequest struct {
	Queue        string          `json:"queue"`
	Kind         string          `json:"kind"`
	Payload      json.RawMessage `json:"payload"`
	Priority     int             `json:"priority"`
	RunAt        *time.Time      `json:"run_at"`
	DelaySeconds float64         `json:"delay_seconds"`
	MaxAttempts  int             `json:"max_attempts"`
	UniqueKey    string          `json:"unique_key"`
}

func (h *Handler) enqueue(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	var req enqueueRequest
	if err := dec.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return
	}
	if req.DelaySeconds < 0 || req.MaxAttempts < 0 {
		writeError(w, http.StatusBadRequest, "delay_seconds and max_attempts must not be negative")
		return
	}

	p := pgqueue.EnqueueParams{
		Queue:       req.Queue,
		Kind:        req.Kind,
		Priority:    req.Priority,
		Delay:       time.Duration(req.DelaySeconds * float64(time.Second)),
		MaxAttempts: req.MaxAttempts,
		UniqueKey:   req.UniqueKey,
	}
	if len(req.Payload) > 0 {
		p.Payload = req.Payload
	}
	if req.RunAt != nil {
		p.RunAt = *req.RunAt
	}

	res, err := h.store.Enqueue(r.Context(), p)
	if err != nil {
		h.fail(w, err)
		return
	}
	status := http.StatusCreated
	if res.Duplicate {
		status = http.StatusOK
	}
	writeJSON(w, status, res)
}

func (h *Handler) get(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	job, err := h.store.Get(r.Context(), id)
	if err != nil {
		h.fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, job)
}

func (h *Handler) list(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	f := pgqueue.ListFilter{Queue: q.Get("queue"), Kind: q.Get("kind"), State: pgqueue.State(q.Get("state"))}
	if f.State != "" && !f.State.Valid() {
		writeError(w, http.StatusBadRequest, "unknown state")
		return
	}
	var err error
	if v := q.Get("after_id"); v != "" {
		if f.AfterID, err = strconv.ParseInt(v, 10, 64); err != nil {
			writeError(w, http.StatusBadRequest, "after_id must be an integer")
			return
		}
	}
	if v := q.Get("limit"); v != "" {
		if f.Limit, err = strconv.Atoi(v); err != nil || f.Limit < 1 {
			writeError(w, http.StatusBadRequest, "limit must be a positive integer")
			return
		}
	}
	jobs, err := h.store.List(r.Context(), f)
	if err != nil {
		h.fail(w, err)
		return
	}
	resp := map[string]any{"jobs": jobs}
	if len(jobs) > 0 {
		resp["next_after_id"] = jobs[len(jobs)-1].ID
	}
	writeJSON(w, http.StatusOK, resp)
}

func (h *Handler) retry(w http.ResponseWriter, r *http.Request) {
	h.adminOp(w, r, h.store.Retry)
}

func (h *Handler) cancel(w http.ResponseWriter, r *http.Request) {
	h.adminOp(w, r, h.store.Cancel)
}

func (h *Handler) adminOp(w http.ResponseWriter, r *http.Request, op func(context.Context, int64) (*pgqueue.JobInfo, error)) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	job, err := op(r.Context(), id)
	if err != nil {
		h.fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, job)
}

func (h *Handler) stats(w http.ResponseWriter, r *http.Request) {
	stats, err := h.store.Stats(r.Context())
	if err != nil {
		h.fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"queues": stats})
}

func pathID(w http.ResponseWriter, r *http.Request) (int64, bool) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || id < 1 {
		writeError(w, http.StatusBadRequest, "id must be a positive integer")
		return 0, false
	}
	return id, true
}

func (h *Handler) fail(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, pgqueue.ErrNotFound):
		writeError(w, http.StatusNotFound, "job not found")
	case errors.Is(err, pgqueue.ErrInvalidState):
		writeError(w, http.StatusConflict, "operation not allowed in the job's current state")
	case errors.Is(err, pgqueue.ErrInvalidParams):
		writeError(w, http.StatusBadRequest, err.Error())
	case errors.Is(err, pgqueue.ErrDuplicate):
		writeError(w, http.StatusConflict, "an active job with the same unique key exists")
	default:
		h.log.Error("request failed", "err", err)
		writeError(w, http.StatusInternalServerError, "internal error")
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
