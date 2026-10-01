package reports

import (
	"context"

	"dbcheck/reporter/internal/store"
	"dbcheck/reporter/internal/users"
)

// Listed is a report task as the console reads it: the contract's
// ReportTask (web/src/lib/api/reports/contract.ts). toListed is the one
// place a stored task becomes this shape.
type Listed struct {
	ID        string    `json:"id"`
	Submitter Submitter `json:"submitter"`
	Status    string    `json:"status"`
	CreatedAt string    `json:"createdAt"`
	// Expired is not computed yet: always false.
	Expired bool         `json:"expired"`
	Items   []ListedItem `json:"items"`
}

// Submitter names a task's submitter. Disabled users keep their tasks and
// stay named.
type Submitter struct {
	ID          string `json:"id"`
	DisplayName string `json:"displayName"`
}

// ListedItem is the contract's ReportItem.
type ListedItem struct {
	FileName         string  `json:"fileName"`
	DBType           string  `json:"dbType"`
	CollectorVersion *string `json:"collectorVersion"`
	// CollectorNotice is not computed yet: always null.
	CollectorNotice any     `json:"collectorNotice"`
	Outcome         Outcome `json:"outcome"`
}

// Outcome is an item's outcome; a failed item carries its reason.
type Outcome struct {
	Status string `json:"status"`
	Reason string `json:"reason,omitempty"`
}

// Filter narrows a listing; empty fields match everything.
type Filter struct {
	SubmitterID string
	TaskID      string
}

// List returns the tasks matching f, newest first (among tasks created at
// the same instant, the last submitted first). Filtering runs in SQL; lists
// are not paginated.
func List(ctx context.Context, q store.Querier, f Filter) ([]Listed, error) {
	tasks, err := listTasks(ctx, q, f)
	if err != nil {
		return nil, err
	}
	if err := listItems(ctx, q, f, tasks); err != nil {
		return nil, err
	}
	out := make([]Listed, 0, len(tasks))
	for _, t := range tasks {
		out = append(out, toListed(t))
	}
	return out, nil
}

// Read returns one task as the console reads it, if u may see it;
// ErrNotFound otherwise.
func Read(ctx context.Context, q store.Querier, id string, u users.User) (Listed, error) {
	if id == "" { // an empty TaskID would match every task
		return Listed{}, ErrNotFound
	}
	tasks, err := List(ctx, q, Filter{TaskID: id})
	if err != nil {
		return Listed{}, err
	}
	if len(tasks) == 0 || !visibleTo(tasks[0].Submitter.ID, u) {
		return Listed{}, ErrNotFound
	}
	return tasks[0], nil
}

// toListed maps a stored task onto the contract.
func toListed(t taskRow) Listed {
	out := Listed{
		ID:        t.ID,
		Submitter: Submitter{ID: t.SubmitterID, DisplayName: t.SubmitterName},
		Status:    t.Status.Wire(),
		CreatedAt: store.FormatTime(t.CreatedAt),
		Items:     make([]ListedItem, 0, len(t.Items)),
	}
	for _, it := range t.Items {
		out.Items = append(out.Items, ListedItem{
			FileName:         it.FileName,
			DBType:           it.DBType,
			CollectorVersion: it.CollectorVersion,
			Outcome:          outcome(it),
		})
	}
	return out
}

func outcome(it Item) Outcome {
	if it.Status == StatusFailed {
		return Outcome{Status: string(StatusFailed), Reason: it.Reason}
	}
	return Outcome{Status: it.Status.Wire()}
}

// taskRow is a stored task with its submitter's display name.
type taskRow struct {
	Task
	SubmitterName string
}

// filterSQL applies a Filter to report_tasks t, with filterArgs.
const filterSQL = `(?1 = '' OR t.submitter_id = ?1) AND (?2 = '' OR t.id = ?2)`

func filterArgs(f Filter) []any { return []any{f.SubmitterID, f.TaskID} }

func listTasks(ctx context.Context, q store.Querier, f Filter) ([]taskRow, error) {
	rows, err := q.QueryContext(ctx, `SELECT t.id, t.submitter_id, u.display_name, t.status, t.created_at
		FROM report_tasks t JOIN users u ON u.id = t.submitter_id
		WHERE `+filterSQL+` ORDER BY t.created_at DESC, t.rowid DESC`, filterArgs(f)...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var tasks []taskRow
	for rows.Next() {
		var t taskRow
		var createdAt string
		if err := rows.Scan(&t.ID, &t.SubmitterID, &t.SubmitterName, &t.Status, &createdAt); err != nil {
			return nil, err
		}
		if t.CreatedAt, err = store.ParseTime(createdAt); err != nil {
			return nil, err
		}
		tasks = append(tasks, t)
	}
	return tasks, rows.Err()
}

// listItems fills in the tasks' items with one query over the same filter.
func listItems(ctx context.Context, q store.Querier, f Filter, tasks []taskRow) error {
	byID := make(map[string]*taskRow, len(tasks))
	for i := range tasks {
		byID[tasks[i].ID] = &tasks[i]
	}
	rows, err := q.QueryContext(ctx, `SELECT i.task_id, i.position, i.file_name, i.db_type, i.collector_version, i.status, i.reason
		FROM report_items i JOIN report_tasks t ON t.id = i.task_id
		WHERE `+filterSQL+` ORDER BY i.task_id, i.position`, filterArgs(f)...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var taskID string
		var it Item
		if err := rows.Scan(&taskID, &it.Position, &it.FileName, &it.DBType, &it.CollectorVersion, &it.Status, &it.Reason); err != nil {
			return err
		}
		if t, ok := byID[taskID]; ok {
			t.Items = append(t.Items, it)
		}
	}
	return rows.Err()
}
