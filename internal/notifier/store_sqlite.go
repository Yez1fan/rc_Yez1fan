package notifier

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

// SQLiteStore is a durable Store backed by an embedded SQLite database (pure-Go
// driver, no cgo). It is the reliability core of the MVP: a task is persisted
// before the HTTP submit is acknowledged, and in-flight work is tracked with a
// visibility lease so a crashed process automatically re-exposes its tasks on
// restart. Writes are serialised through a mutex because SQLite is a single
// writer; that is more than fast enough for a first-version internal service
// and removes a whole class of "database is locked" flakiness.
type SQLiteStore struct {
	db    *sql.DB
	mu    sync.Mutex // serialises writers
	genID func() string
}

const schema = `
CREATE TABLE IF NOT EXISTS tasks (
    id               TEXT PRIMARY KEY,
    idempotency_key  TEXT,
    url              TEXT NOT NULL,
    method           TEXT NOT NULL,
    headers          TEXT NOT NULL,
    body             BLOB,
    status           TEXT NOT NULL,
    attempts         INTEGER NOT NULL,
    max_attempts     INTEGER NOT NULL,
    next_visible_at  INTEGER NOT NULL, -- unix nanos
    lease_expires_at INTEGER NOT NULL, -- unix nanos, 0 when not leased
    last_error       TEXT NOT NULL,
    created_at       INTEGER NOT NULL,
    updated_at       INTEGER NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_tasks_idem
    ON tasks(idempotency_key) WHERE idempotency_key <> '';
CREATE INDEX IF NOT EXISTS idx_tasks_ready
    ON tasks(status, next_visible_at);
`

// OpenSQLite opens (creating if needed) the SQLite database at path and applies
// the schema. Pass ":memory:" for an ephemeral store (tests).
func OpenSQLite(path string) (*SQLiteStore, error) {
	// _txlock=immediate makes write transactions grab the reserved lock up
	// front, avoiding mid-transaction upgrade deadlocks under concurrency.
	dsn := fmt.Sprintf("file:%s?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_txlock=immediate", path)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	// A single connection sidesteps SQLite's writer contention entirely; the
	// mutex already serialises us, and reads are fast enough on one conn.
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("apply schema: %w", err)
	}
	return &SQLiteStore{db: db, genID: newID}, nil
}

func (s *SQLiteStore) Close() error { return s.db.Close() }

// Enqueue persists a new pending task, honouring the idempotency key.
func (s *SQLiteStore) Enqueue(ctx context.Context, t *Task) (*Task, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if t.IdempotencyKey != "" {
		if existing, err := s.getByIdemLocked(ctx, t.IdempotencyKey); err == nil {
			return existing, false, nil
		} else if err != ErrNotFound {
			return nil, false, err
		}
	}

	now := time.Now()
	t.ID = s.genID()
	t.Status = StatusPending
	t.Attempts = 0
	t.NextVisibleAt = now
	t.LeaseExpiresAt = time.Time{}
	t.CreatedAt = now
	t.UpdatedAt = now

	hdr, err := json.Marshal(t.Headers)
	if err != nil {
		return nil, false, fmt.Errorf("marshal headers: %w", err)
	}
	_, err = s.db.ExecContext(ctx, `
        INSERT INTO tasks (id, idempotency_key, url, method, headers, body,
            status, attempts, max_attempts, next_visible_at, lease_expires_at,
            last_error, created_at, updated_at)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		t.ID, t.IdempotencyKey, t.URL, t.Method, string(hdr), t.Body,
		string(t.Status), t.Attempts, t.MaxAttempts, t.NextVisibleAt.UnixNano(),
		int64(0), "", t.CreatedAt.UnixNano(), t.UpdatedAt.UnixNano())
	if err != nil {
		return nil, false, fmt.Errorf("insert task: %w", err)
	}
	return t, true, nil
}

// Claim atomically leases ready tasks. "Ready" = pending & visible, OR
// delivering with an expired lease (the previous worker crashed). The two-step
// SELECT-then-UPDATE runs in one immediate transaction so concurrent workers
// never grab the same task.
func (s *SQLiteStore) Claim(ctx context.Context, now time.Time, lease time.Duration, limit int) ([]*Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	nowNs := now.UnixNano()
	rows, err := tx.QueryContext(ctx, `
        SELECT id FROM tasks
        WHERE (status = ? AND next_visible_at <= ?)
           OR (status = ? AND lease_expires_at <= ?)
        ORDER BY next_visible_at ASC
        LIMIT ?`,
		string(StatusPending), nowNs, string(StatusDelivering), nowNs, limit)
	if err != nil {
		return nil, err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return nil, tx.Commit()
	}

	leaseUntil := now.Add(lease).UnixNano()
	claimed := make([]*Task, 0, len(ids))
	for _, id := range ids {
		if _, err := tx.ExecContext(ctx, `
            UPDATE tasks
            SET status = ?, attempts = attempts + 1,
                lease_expires_at = ?, updated_at = ?
            WHERE id = ?`,
			string(StatusDelivering), leaseUntil, nowNs, id); err != nil {
			return nil, err
		}
		t, err := s.getLocked(ctx, tx, id)
		if err != nil {
			return nil, err
		}
		claimed = append(claimed, t)
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return claimed, nil
}

// Succeed marks a leased task delivered.
func (s *SQLiteStore) Succeed(ctx context.Context, id string, now time.Time) error {
	return s.finish(ctx, id, StatusSucceeded, 0, "", now)
}

// Kill moves an exhausted task to the dead-letter state.
func (s *SQLiteStore) Kill(ctx context.Context, id, lastErr string, now time.Time) error {
	return s.finish(ctx, id, StatusDead, 0, lastErr, now)
}

func (s *SQLiteStore) finish(ctx context.Context, id string, st Status, visibleAt int64, lastErr string, now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.ExecContext(ctx, `
        UPDATE tasks
        SET status = ?, next_visible_at = ?, lease_expires_at = 0,
            last_error = ?, updated_at = ?
        WHERE id = ?`,
		string(st), visibleAt, lastErr, now.UnixNano(), id)
	return err
}

// Retry reschedules a leased task back to pending, visible at visibleAt.
func (s *SQLiteStore) Retry(ctx context.Context, id string, visibleAt time.Time, lastErr string, now time.Time) error {
	return s.finish(ctx, id, StatusPending, visibleAt.UnixNano(), lastErr, now)
}

// Get returns a task by id.
func (s *SQLiteStore) Get(ctx context.Context, id string) (*Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.getLocked(ctx, s.db, id)
}

// rowQuerier is satisfied by both *sql.DB and *sql.Tx.
type rowQuerier interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

func (s *SQLiteStore) getLocked(ctx context.Context, q rowQuerier, id string) (*Task, error) {
	return scanTask(q.QueryRowContext(ctx, selectCols+` WHERE id = ?`, id))
}

func (s *SQLiteStore) getByIdemLocked(ctx context.Context, key string) (*Task, error) {
	return scanTask(s.db.QueryRowContext(ctx, selectCols+` WHERE idempotency_key = ?`, key))
}

const selectCols = `
    SELECT id, idempotency_key, url, method, headers, body, status, attempts,
           max_attempts, next_visible_at, lease_expires_at, last_error,
           created_at, updated_at
    FROM tasks`

func scanTask(row *sql.Row) (*Task, error) {
	var (
		t                         Task
		hdr                       string
		nextNs, leaseNs, cNs, uNs int64
		status                    string
	)
	err := row.Scan(&t.ID, &t.IdempotencyKey, &t.URL, &t.Method, &hdr, &t.Body,
		&status, &t.Attempts, &t.MaxAttempts, &nextNs, &leaseNs, &t.LastError,
		&cNs, &uNs)
	if err == sql.ErrNoRows {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	t.Status = Status(status)
	if hdr != "" && hdr != "null" {
		if err := json.Unmarshal([]byte(hdr), &t.Headers); err != nil {
			return nil, fmt.Errorf("unmarshal headers: %w", err)
		}
	}
	t.NextVisibleAt = time.Unix(0, nextNs)
	if leaseNs != 0 {
		t.LeaseExpiresAt = time.Unix(0, leaseNs)
	}
	t.CreatedAt = time.Unix(0, cNs)
	t.UpdatedAt = time.Unix(0, uNs)
	return &t, nil
}
