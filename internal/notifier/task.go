// Package notifier contains the core domain model and delivery engine for the
// API notification system: durable task types, the Store abstraction, the
// outbound HTTP sender, and the retrying dispatcher.
package notifier

import "time"

// Status is the lifecycle state of a notification task.
type Status string

const (
	// StatusPending means the task is waiting to be delivered (or retried).
	StatusPending Status = "pending"
	// StatusDelivering means a worker currently holds a lease on the task.
	StatusDelivering Status = "delivering"
	// StatusSucceeded means the target accepted the notification (2xx).
	StatusSucceeded Status = "succeeded"
	// StatusDead means the task exhausted its retries or failed permanently.
	StatusDead Status = "dead"
)

// Notification is the request submitted by a business system. It fully
// describes an opaque outbound HTTP(S) call; the notifier never interprets the
// response body, it only cares that the target accepted it.
type Notification struct {
	// URL is the target endpoint. Required.
	URL string `json:"url"`
	// Method defaults to POST when empty.
	Method string `json:"method,omitempty"`
	// Headers are sent verbatim on the outbound request.
	Headers map[string]string `json:"headers,omitempty"`
	// Body is the raw request body, sent verbatim.
	Body string `json:"body,omitempty"`
	// MaxAttempts overrides the server default when > 0.
	MaxAttempts int `json:"max_attempts,omitempty"`
	// IdempotencyKey, when set, deduplicates submissions: re-submitting the
	// same key returns the already-accepted task instead of creating a new one.
	IdempotencyKey string `json:"idempotency_key,omitempty"`
}

// Task is the durably-stored unit of work derived from a Notification.
type Task struct {
	ID             string            `json:"id"`
	IdempotencyKey string            `json:"idempotency_key,omitempty"`
	URL            string            `json:"url"`
	Method         string            `json:"method"`
	Headers        map[string]string `json:"headers,omitempty"`
	Body           []byte            `json:"-"`
	Status         Status            `json:"status"`
	Attempts       int               `json:"attempts"`
	MaxAttempts    int               `json:"max_attempts"`
	NextVisibleAt  time.Time         `json:"next_visible_at"`
	LeaseExpiresAt time.Time         `json:"lease_expires_at,omitempty"`
	LastError      string            `json:"last_error,omitempty"`
	CreatedAt      time.Time         `json:"created_at"`
	UpdatedAt      time.Time         `json:"updated_at"`
}
