package notifier

import (
	"context"
	"errors"
	"time"
)

// ErrNotFound is returned by Store.Get when no task matches the id.
var ErrNotFound = errors.New("notifier: task not found")

// Store is the durable backend for tasks. It is the single seam that decides
// the reliability guarantee: everything is persisted before acknowledgement,
// and in-flight work is modelled as a lease so that a crashed worker's tasks
// become visible again automatically. Swapping SQLite for MySQL or an external
// MQ later means reimplementing this interface, nothing else.
type Store interface {
	// Enqueue durably persists a new pending task. If the task carries an
	// IdempotencyKey that already exists, the stored task is returned with
	// created=false and no new row is written.
	Enqueue(ctx context.Context, t *Task) (stored *Task, created bool, err error)

	// Claim atomically leases up to limit tasks that are ready to run: pending
	// tasks whose NextVisibleAt has passed, plus delivering tasks whose lease
	// expired (i.e. their previous worker died). Claimed tasks are moved to
	// StatusDelivering with a fresh lease and their Attempts incremented.
	Claim(ctx context.Context, now time.Time, lease time.Duration, limit int) ([]*Task, error)

	// Succeed marks a leased task as delivered.
	Succeed(ctx context.Context, id string, now time.Time) error

	// Retry reschedules a leased task to become visible again at visibleAt,
	// recording the failure reason.
	Retry(ctx context.Context, id string, visibleAt time.Time, lastErr string, now time.Time) error

	// Kill moves a leased task to the dead state after exhausting retries.
	Kill(ctx context.Context, id string, lastErr string, now time.Time) error

	// Get returns a task by id, or ErrNotFound.
	Get(ctx context.Context, id string) (*Task, error)

	// Close releases underlying resources.
	Close() error
}
