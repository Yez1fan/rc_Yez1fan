// Package httpapi exposes the notifier over HTTP: business systems submit
// notifications and (optionally) poll their status.
package httpapi

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"rc_Yez1fan/internal/notifier"
)

// Server adapts the notifier Store to HTTP handlers. Delivery is performed by
// the dispatcher out of band; the API's only job is to durably accept work and
// report status.
type Server struct {
	store notifier.Store
	log   *slog.Logger
}

// New builds the HTTP server handler set.
func New(store notifier.Store, log *slog.Logger) *Server {
	if log == nil {
		log = slog.Default()
	}
	return &Server{store: store, log: log}
}

// Routes returns the configured mux.
func (s *Server) Routes() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /notifications", s.submit)
	mux.HandleFunc("GET /notifications/{id}", s.status)
	mux.HandleFunc("GET /healthz", s.health)
	return mux
}

// submitResponse is returned on accept. Created lets an idempotent re-submit be
// distinguished (200 + created=false) from a fresh accept (202 + created=true).
type submitResponse struct {
	ID      string          `json:"id"`
	Status  notifier.Status `json:"status"`
	Created bool            `json:"created"`
}

func (s *Server) submit(w http.ResponseWriter, r *http.Request) {
	var in notifier.Notification
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)) // 1 MiB cap
	dec.DisallowUnknownFields()
	if err := dec.Decode(&in); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid json: "+err.Error())
		return
	}
	if strings.TrimSpace(in.URL) == "" {
		writeErr(w, http.StatusBadRequest, "url is required")
		return
	}
	if !strings.HasPrefix(in.URL, "http://") && !strings.HasPrefix(in.URL, "https://") {
		writeErr(w, http.StatusBadRequest, "url must be http(s)")
		return
	}
	method := strings.ToUpper(strings.TrimSpace(in.Method))
	if method == "" {
		method = http.MethodPost
	}

	task := &notifier.Task{
		IdempotencyKey: in.IdempotencyKey,
		URL:            in.URL,
		Method:         method,
		Headers:        in.Headers,
		Body:           []byte(in.Body),
		MaxAttempts:    in.MaxAttempts,
	}
	stored, created, err := s.store.Enqueue(r.Context(), task)
	if err != nil {
		s.log.Error("enqueue failed", "err", err)
		writeErr(w, http.StatusInternalServerError, "could not persist notification")
		return
	}

	code := http.StatusAccepted
	if !created {
		code = http.StatusOK
	}
	writeJSON(w, code, submitResponse{ID: stored.ID, Status: stored.Status, Created: created})
}

func (s *Server) status(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	t, err := s.store.Get(r.Context(), id)
	if errors.Is(err, notifier.ErrNotFound) {
		writeErr(w, http.StatusNotFound, "task not found")
		return
	}
	if err != nil {
		s.log.Error("get failed", "id", id, "err", err)
		writeErr(w, http.StatusInternalServerError, "lookup failed")
		return
	}
	writeJSON(w, http.StatusOK, t)
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "time": time.Now().Format(time.RFC3339)})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}
