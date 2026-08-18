package notifier

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func newTestStore(t *testing.T) *SQLiteStore {
	t.Helper()
	// A real on-disk DB in the test's temp dir: matches production and
	// exercises WAL/lease persistence rather than in-memory shortcuts.
	s, err := OpenSQLite(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func mustEnqueue(t *testing.T, s *SQLiteStore, task *Task) *Task {
	t.Helper()
	stored, created, err := s.Enqueue(context.Background(), task)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if !created {
		t.Fatalf("expected created=true")
	}
	return stored
}

func TestEnqueueIdempotency(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	first := mustEnqueue(t, s, &Task{URL: "http://x", Method: "POST", IdempotencyKey: "k1"})

	// Re-submitting the same key must not create a second task.
	again, created, err := s.Enqueue(ctx, &Task{URL: "http://x", Method: "POST", IdempotencyKey: "k1"})
	if err != nil {
		t.Fatalf("enqueue again: %v", err)
	}
	if created {
		t.Fatalf("expected created=false for duplicate idempotency key")
	}
	if again.ID != first.ID {
		t.Fatalf("expected same id, got %s vs %s", again.ID, first.ID)
	}
}

func TestClaimVisibilityAndSucceed(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	task := mustEnqueue(t, s, &Task{URL: "http://x", Method: "POST"})

	now := time.Now()
	claimed, err := s.Claim(ctx, now, time.Minute, 10)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if len(claimed) != 1 {
		t.Fatalf("expected 1 claimed, got %d", len(claimed))
	}
	if claimed[0].Status != StatusDelivering || claimed[0].Attempts != 1 {
		t.Fatalf("expected delivering/attempts=1, got %s/%d", claimed[0].Status, claimed[0].Attempts)
	}

	// A second claim within the lease window must return nothing.
	again, err := s.Claim(ctx, now, time.Minute, 10)
	if err != nil {
		t.Fatalf("claim 2: %v", err)
	}
	if len(again) != 0 {
		t.Fatalf("expected 0 on second claim (leased), got %d", len(again))
	}

	if err := s.Succeed(ctx, task.ID, time.Now()); err != nil {
		t.Fatalf("succeed: %v", err)
	}
	got, err := s.Get(ctx, task.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Status != StatusSucceeded {
		t.Fatalf("expected succeeded, got %s", got.Status)
	}
}

// TestClaimReclaimsExpiredLease is the crash-recovery guarantee: a task leased
// by a worker that died (its lease expires) must become claimable again.
func TestClaimReclaimsExpiredLease(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	mustEnqueue(t, s, &Task{URL: "http://x", Method: "POST"})

	t0 := time.Now()
	if _, err := s.Claim(ctx, t0, 100*time.Millisecond, 10); err != nil {
		t.Fatalf("claim: %v", err)
	}

	// Simulate the worker dying: advance past the lease without acking.
	future := t0.Add(time.Second)
	reclaimed, err := s.Claim(ctx, future, time.Minute, 10)
	if err != nil {
		t.Fatalf("reclaim: %v", err)
	}
	if len(reclaimed) != 1 {
		t.Fatalf("expected expired lease to be reclaimed, got %d", len(reclaimed))
	}
	if reclaimed[0].Attempts != 2 {
		t.Fatalf("expected attempts=2 after reclaim, got %d", reclaimed[0].Attempts)
	}
}

func TestRetryReschedules(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	task := mustEnqueue(t, s, &Task{URL: "http://x", Method: "POST"})

	now := time.Now()
	if _, err := s.Claim(ctx, now, time.Minute, 10); err != nil {
		t.Fatalf("claim: %v", err)
	}
	if err := s.Retry(ctx, task.ID, now.Add(time.Hour), "boom", time.Now()); err != nil {
		t.Fatalf("retry: %v", err)
	}

	// Not visible yet.
	if got, _ := s.Claim(ctx, now, time.Minute, 10); len(got) != 0 {
		t.Fatalf("expected task not visible before backoff, got %d", len(got))
	}
	// Visible after the backoff window.
	if got, _ := s.Claim(ctx, now.Add(2*time.Hour), time.Minute, 10); len(got) != 1 {
		t.Fatalf("expected task visible after backoff, got %d", len(got))
	}
	stored, _ := s.Get(ctx, task.ID)
	if stored.LastError != "boom" {
		t.Fatalf("expected last_error recorded, got %q", stored.LastError)
	}
}
