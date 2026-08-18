package notifier

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"time"
)

// Sender performs the outbound HTTP(S) notification. The business systems do
// not care about the response body, so we only classify the outcome into
// success / retryable / permanent.
type Sender struct {
	client *http.Client
	// maxRespPeek bounds how much of an error response we read for logging.
	maxRespPeek int64
}

// NewSender builds a Sender with a per-request timeout.
func NewSender(timeout time.Duration) *Sender {
	return &Sender{
		client:      &http.Client{Timeout: timeout},
		maxRespPeek: 512,
	}
}

// sendOutcome is the classified result of one delivery attempt.
type sendOutcome int

const (
	outcomeSuccess   sendOutcome = iota // 2xx: target accepted the notification
	outcomeRetryable                    // transient: network error, 5xx, 429
	outcomePermanent                    // 4xx (except 429): request will never succeed as-is
)

// Send performs one delivery attempt and classifies the result. The returned
// error is descriptive (for LastError); the outcome drives retry policy.
func (s *Sender) Send(ctx context.Context, t *Task) (sendOutcome, error) {
	req, err := http.NewRequestWithContext(ctx, t.Method, t.URL, bytes.NewReader(t.Body))
	if err != nil {
		// A malformed URL/method never becomes valid on retry.
		return outcomePermanent, fmt.Errorf("build request: %w", err)
	}
	for k, v := range t.Headers {
		req.Header.Set(k, v)
	}

	resp, err := s.client.Do(req)
	if err != nil {
		// Timeouts, DNS, connection refused, TLS: all transient by default.
		return outcomeRetryable, fmt.Errorf("http do: %w", err)
	}
	defer resp.Body.Close()
	peek, _ := io.ReadAll(io.LimitReader(resp.Body, s.maxRespPeek))
	// Drain the rest so the connection can be reused.
	_, _ = io.Copy(io.Discard, resp.Body)

	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		return outcomeSuccess, nil
	case resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500:
		return outcomeRetryable, fmt.Errorf("status %d: %s", resp.StatusCode, peek)
	default:
		return outcomePermanent, fmt.Errorf("status %d: %s", resp.StatusCode, peek)
	}
}
