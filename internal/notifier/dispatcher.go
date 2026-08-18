package notifier

import (
	"context"
	"log/slog"
	"math"
	"math/rand"
	"sync"
	"time"
)

// Config tunes the dispatcher. Zero values fall back to sane defaults.
type Config struct {
	Workers        int           // concurrent delivery workers
	PollInterval   time.Duration // how often to poll the store when idle
	Lease          time.Duration // visibility timeout for a claimed task
	RequestTimeout time.Duration // per-attempt HTTP timeout
	MaxAttempts    int           // default cap when a task does not set its own
	BaseBackoff    time.Duration // first retry delay
	MaxBackoff     time.Duration // cap on exponential backoff
}

func (c *Config) withDefaults() {
	if c.Workers <= 0 {
		c.Workers = 4
	}
	if c.PollInterval <= 0 {
		c.PollInterval = 500 * time.Millisecond
	}
	if c.Lease <= 0 {
		c.Lease = 30 * time.Second
	}
	if c.RequestTimeout <= 0 {
		c.RequestTimeout = 10 * time.Second
	}
	if c.MaxAttempts <= 0 {
		c.MaxAttempts = 8
	}
	if c.BaseBackoff <= 0 {
		c.BaseBackoff = 1 * time.Second
	}
	if c.MaxBackoff <= 0 {
		c.MaxBackoff = 5 * time.Minute
	}
}

// Dispatcher is the delivery engine. It repeatedly claims ready tasks from the
// Store and hands them to a bounded worker pool. Reliability comes from the
// store's lease: the dispatcher never holds task state in memory beyond the
// in-flight attempt, so a crash simply leaves leased tasks to expire and be
// re-claimed after restart. Delivery semantics are therefore at-least-once.
type Dispatcher struct {
	store  Store
	sender *Sender
	cfg    Config
	log    *slog.Logger
	rnd    *rand.Rand
	rndMu  sync.Mutex
}

// NewDispatcher wires a dispatcher. Config is copied and defaulted.
func NewDispatcher(store Store, cfg Config, log *slog.Logger) *Dispatcher {
	cfg.withDefaults()
	if log == nil {
		log = slog.Default()
	}
	return &Dispatcher{
		store:  store,
		sender: NewSender(cfg.RequestTimeout),
		cfg:    cfg,
		log:    log,
		rnd:    rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// Run blocks until ctx is cancelled, claiming and delivering tasks. A single
// claim loop feeds a buffered channel drained by Workers goroutines; this keeps
// the number of in-flight deliveries bounded regardless of backlog size.
func (d *Dispatcher) Run(ctx context.Context) {
	jobs := make(chan *Task)
	var wg sync.WaitGroup
	for i := 0; i < d.cfg.Workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for t := range jobs {
				d.deliver(ctx, t)
			}
		}()
	}

	ticker := time.NewTicker(d.cfg.PollInterval)
	defer ticker.Stop()

	d.log.Info("dispatcher started", "workers", d.cfg.Workers, "lease", d.cfg.Lease)
	for {
		// Drain everything ready before sleeping, so a burst is worked off
		// promptly instead of one-per-tick.
		for {
			select {
			case <-ctx.Done():
				close(jobs)
				wg.Wait()
				d.log.Info("dispatcher stopped")
				return
			default:
			}
			tasks, err := d.store.Claim(ctx, time.Now(), d.cfg.Lease, d.cfg.Workers)
			if err != nil {
				if ctx.Err() != nil {
					break
				}
				d.log.Error("claim failed", "err", err)
				break
			}
			if len(tasks) == 0 {
				break
			}
			for _, t := range tasks {
				select {
				case jobs <- t:
				case <-ctx.Done():
					close(jobs)
					wg.Wait()
					d.log.Info("dispatcher stopped")
					return
				}
			}
		}

		select {
		case <-ctx.Done():
			close(jobs)
			wg.Wait()
			d.log.Info("dispatcher stopped")
			return
		case <-ticker.C:
		}
	}
}

// deliver performs one attempt and updates durable state accordingly.
func (d *Dispatcher) deliver(ctx context.Context, t *Task) {
	// Use a background-derived context so an in-flight attempt is not aborted
	// mid-flight by shutdown; the per-request timeout still bounds it. This
	// avoids sending a request we then fail to record the result of.
	attemptCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), d.cfg.RequestTimeout)
	defer cancel()

	outcome, sendErr := d.sender.Send(attemptCtx, t)
	now := time.Now()
	maxAttempts := t.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = d.cfg.MaxAttempts
	}

	switch outcome {
	case outcomeSuccess:
		if err := d.store.Succeed(ctx, t.ID, now); err != nil {
			d.log.Error("mark succeeded failed", "id", t.ID, "err", err)
			return
		}
		d.log.Info("delivered", "id", t.ID, "url", t.URL, "attempts", t.Attempts)

	case outcomePermanent:
		if err := d.store.Kill(ctx, t.ID, sendErr.Error(), now); err != nil {
			d.log.Error("mark dead failed", "id", t.ID, "err", err)
			return
		}
		d.log.Warn("dead (permanent)", "id", t.ID, "url", t.URL, "err", sendErr)

	default: // retryable
		if t.Attempts >= maxAttempts {
			if err := d.store.Kill(ctx, t.ID, sendErr.Error(), now); err != nil {
				d.log.Error("mark dead failed", "id", t.ID, "err", err)
				return
			}
			d.log.Warn("dead (exhausted)", "id", t.ID, "attempts", t.Attempts, "err", sendErr)
			return
		}
		delay := d.backoff(t.Attempts)
		if err := d.store.Retry(ctx, t.ID, now.Add(delay), sendErr.Error(), now); err != nil {
			d.log.Error("reschedule failed", "id", t.ID, "err", err)
			return
		}
		d.log.Info("retry scheduled", "id", t.ID, "attempts", t.Attempts, "in", delay, "err", sendErr)
	}
}

// backoff returns an exponential delay with full jitter, capped at MaxBackoff.
// attempt is the number of attempts already made (>=1 here).
func (d *Dispatcher) backoff(attempt int) time.Duration {
	exp := float64(d.cfg.BaseBackoff) * math.Pow(2, float64(attempt-1))
	if exp > float64(d.cfg.MaxBackoff) {
		exp = float64(d.cfg.MaxBackoff)
	}
	d.rndMu.Lock()
	jittered := d.rnd.Float64() * exp
	d.rndMu.Unlock()
	return time.Duration(jittered)
}
