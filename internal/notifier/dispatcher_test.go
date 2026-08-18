package notifier

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// TestDispatcherRetriesThenSucceeds exercises the full engine: a target that
// fails with 503 twice then returns 200 must end up succeeded, proving both the
// retry loop and durable state transitions.
func TestDispatcherRetriesThenSucceeds(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) < 3 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	s := newTestStore(t)
	task := mustEnqueue(t, s, &Task{URL: srv.URL, Method: "POST"})

	d := NewDispatcher(s, Config{
		Workers:      2,
		PollInterval: 10 * time.Millisecond,
		Lease:        time.Second,
		BaseBackoff:  5 * time.Millisecond,
		MaxBackoff:   50 * time.Millisecond,
		MaxAttempts:  5,
	}, slog.New(slog.NewTextHandler(testWriter{t}, nil)))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() { d.Run(ctx); close(done) }()

	waitFor(t, 4*time.Second, func() bool {
		got, err := s.Get(context.Background(), task.ID)
		return err == nil && got.Status == StatusSucceeded
	})
	cancel()
	<-done

	if calls.Load() < 3 {
		t.Fatalf("expected at least 3 attempts, got %d", calls.Load())
	}
}

// TestDispatcherPermanentFailureIsDead verifies a 4xx is not retried.
func TestDispatcherPermanentFailureIsDead(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer srv.Close()

	s := newTestStore(t)
	task := mustEnqueue(t, s, &Task{URL: srv.URL, Method: "POST"})

	d := NewDispatcher(s, Config{
		Workers:      1,
		PollInterval: 10 * time.Millisecond,
		Lease:        time.Second,
		BaseBackoff:  5 * time.Millisecond,
		MaxAttempts:  5,
	}, slog.New(slog.NewTextHandler(testWriter{t}, nil)))

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() { d.Run(ctx); close(done) }()

	waitFor(t, 2*time.Second, func() bool {
		got, err := s.Get(context.Background(), task.ID)
		return err == nil && got.Status == StatusDead
	})
	cancel()
	<-done

	if n := calls.Load(); n != 1 {
		t.Fatalf("expected exactly 1 attempt for permanent failure, got %d", n)
	}
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("condition not met within %s", timeout)
}

// testWriter routes dispatcher logs into the test log.
type testWriter struct{ t *testing.T }

func (w testWriter) Write(p []byte) (int, error) {
	w.t.Logf("%s", p)
	return len(p), nil
}
