// Package reports owns report task records (CONTEXT.md: Report task, Report
// item, Submitter) and the reading of uploaded collector ZIPs. The web
// package's report lifecycle is the single writer of these records
// (ADR 0001); functions take a store.Querier so they work inside a
// transaction too.
package reports

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"dbcheck/reporter/internal/apierr"
	"dbcheck/reporter/internal/store"
	"dbcheck/reporter/internal/users"
)

// Status is a task's or an item's stored status.
type Status string

const (
	// StatusQueued: accepted and waiting for the worker.
	StatusQueued     Status = "queued"
	StatusProcessing Status = "processing"
	StatusDone       Status = "done"
	StatusFailed     Status = "failed"
)

// Wire is the status the console contract shows: processing, done, or
// failed. A task still queued on the server reports processing.
func (s Status) Wire() string {
	if s == StatusQueued {
		return string(StatusProcessing)
	}
	return string(s)
}

// Finished reports whether the status is final (done or failed).
func (s Status) Finished() bool { return s == StatusDone || s == StatusFailed }

// Item is one report item of a task.
type Item struct {
	// Position is 1-based, in submission order.
	Position int
	FileName string
	DBType   string
	// CollectorVersion is what the server read from the ZIP; nil when unknown.
	CollectorVersion *string
	Status           Status
	// Reason says why a failed item failed.
	Reason string
}

// Task is a stored report task with its items in submission order.
type Task struct {
	ID          string
	SubmitterID string
	Status      Status
	// Error says why a failed task failed.
	Error     string
	CreatedAt time.Time
	Items     []Item
}

// RetentionDays is how long a report task keeps its files (ADR 0003):
// that many days after its creation the task expires (已过期). Its uploaded
// inputs, extracted data, and generated reports are deleted; its record
// stays forever.
const RetentionDays = 30

// Retention is RetentionDays as a duration.
const Retention = RetentionDays * 24 * time.Hour

// Expired reports whether the task is past retention at now.
func (t Task) Expired(now time.Time) bool { return !now.Before(t.CreatedAt.Add(Retention)) }

// Downloadable reports whether the task's reports can be downloaded at now:
// it is done and not expired.
func (t Task) Downloadable(now time.Time) bool { return t.Status == StatusDone && !t.Expired(now) }

// ErrExpired refuses to download an expired task's reports.
var ErrExpired = apierr.Conflict(fmt.Sprintf("报告已超过 %d 天保留期，文件已清理", RetentionDays))

// ErrNotFound answers a task that does not exist or that the caller may not see.
var ErrNotFound = apierr.NotFound("报告任务不存在")

// VisibleTo reports whether u may read the task: its submitter and admins.
func (t Task) VisibleTo(u users.User) bool { return visibleTo(t.SubmitterID, u) }

func visibleTo(submitterID string, u users.User) bool {
	return u.IsAdmin() || submitterID == u.ID
}

// Insert stores a new task and its items.
func Insert(ctx context.Context, q store.Querier, t Task) error {
	if _, err := q.ExecContext(ctx,
		"INSERT INTO report_tasks (id, submitter_id, status, error, created_at) VALUES (?, ?, ?, ?, ?)",
		t.ID, t.SubmitterID, t.Status, t.Error, store.FormatTime(t.CreatedAt)); err != nil {
		return fmt.Errorf("insert report task %s: %w", t.ID, err)
	}
	for _, it := range t.Items {
		if _, err := q.ExecContext(ctx, `INSERT INTO report_items
			(task_id, position, file_name, db_type, collector_version, status, reason) VALUES (?, ?, ?, ?, ?, ?, ?)`,
			t.ID, it.Position, it.FileName, it.DBType, it.CollectorVersion, it.Status, it.Reason); err != nil {
			return fmt.Errorf("insert report item %s/%d: %w", t.ID, it.Position, err)
		}
	}
	return nil
}

// Get loads a task with its items; ErrNotFound when there is none.
func Get(ctx context.Context, q store.Querier, id string) (Task, error) {
	var t Task
	var createdAt string
	err := q.QueryRowContext(ctx, "SELECT id, submitter_id, status, error, created_at FROM report_tasks WHERE id = ?", id).
		Scan(&t.ID, &t.SubmitterID, &t.Status, &t.Error, &createdAt)
	if errors.Is(err, sql.ErrNoRows) {
		return Task{}, ErrNotFound
	}
	if err != nil {
		return Task{}, err
	}
	if t.CreatedAt, err = store.ParseTime(createdAt); err != nil {
		return Task{}, err
	}
	rows, err := q.QueryContext(ctx, `SELECT position, file_name, db_type, collector_version, status, reason
		FROM report_items WHERE task_id = ? ORDER BY position`, id)
	if err != nil {
		return Task{}, err
	}
	defer rows.Close()
	for rows.Next() {
		var it Item
		if err := rows.Scan(&it.Position, &it.FileName, &it.DBType, &it.CollectorVersion, &it.Status, &it.Reason); err != nil {
			return Task{}, err
		}
		t.Items = append(t.Items, it)
	}
	return t, rows.Err()
}

// ClaimNext moves the oldest queued task to processing and returns its id;
// ok is false when nothing is queued.
func ClaimNext(ctx context.Context, db *store.DB) (id string, ok bool, err error) {
	err = db.Tx(ctx, func(tx store.Querier) error {
		err := tx.QueryRowContext(ctx,
			"SELECT id FROM report_tasks WHERE status = ? ORDER BY created_at, rowid LIMIT 1", StatusQueued).Scan(&id)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		ok = true
		return setTaskStatus(ctx, tx, TaskUpdate{TaskID: id, Status: StatusProcessing})
	})
	return id, ok, err
}

// RequeueInterrupted puts every processing task, and every processing item,
// back to queued. Run it at startup, before the worker claims anything: with
// a single writer, whatever is processing then was interrupted.
func RequeueInterrupted(ctx context.Context, db *store.DB) error {
	return db.Tx(ctx, func(tx store.Querier) error {
		if _, err := tx.ExecContext(ctx, "UPDATE report_tasks SET status = ? WHERE status = ?", StatusQueued, StatusProcessing); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, "UPDATE report_items SET status = ? WHERE status = ?", StatusQueued, StatusProcessing)
		return err
	})
}

// ItemUpdate is an item's new status, with the reason when it failed.
type ItemUpdate struct {
	TaskID   string
	Position int
	Status   Status
	Reason   string
}

// SetItemStatus records an item's status.
func SetItemStatus(ctx context.Context, q store.Querier, u ItemUpdate) error {
	res, err := q.ExecContext(ctx, "UPDATE report_items SET status = ?, reason = ? WHERE task_id = ? AND position = ?",
		u.Status, u.Reason, u.TaskID, u.Position)
	return expectOneRow(res, err)
}

// TaskUpdate is a task's new status, with the error when it failed.
type TaskUpdate struct {
	TaskID string
	Status Status
	Error  string
}

// Finish records a task's final status.
func Finish(ctx context.Context, q store.Querier, u TaskUpdate) error {
	return setTaskStatus(ctx, q, u)
}

func setTaskStatus(ctx context.Context, q store.Querier, u TaskUpdate) error {
	res, err := q.ExecContext(ctx, "UPDATE report_tasks SET status = ?, error = ? WHERE id = ?", u.Status, u.Error, u.TaskID)
	return expectOneRow(res, err)
}

func expectOneRow(res sql.Result, err error) error {
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrNotFound
	}
	return nil
}
