package web

import (
	"context"
	"errors"
	"fmt"
	"time"

	"dbcheck/reporter/internal/reports"
	"dbcheck/reporter/internal/store"
)

// work runs the legacy backlog, then claims queued tasks from the store
// oldest first, sleeping until a submission wakes it when none is left.
// When claiming fails it tries again after a backoff, or at the next
// submission if that comes first.
func (l *reportLifecycle) work(ctx context.Context, legacy []queuedTask) {
	for _, q := range legacy {
		l.runLegacy(ctx, q)
	}
	var wait time.Duration
	for ctx.Err() == nil {
		id, ok, err := l.records.ClaimNext(ctx)
		switch {
		case err != nil:
			wait = l.backoff.after(wait)
			l.platform.Log.Printf("[ERROR] claiming a report task failed, retrying in %s: %v", wait, err)
			l.sleep(ctx, wait)
		case !ok:
			wait = 0
			l.sleep(ctx, 0)
		default:
			wait = 0
			l.run(ctx, id)
		}
	}
}

// sleep waits for a submission, for d when d > 0, or until ctx ends.
func (l *reportLifecycle) sleep(ctx context.Context, d time.Duration) {
	var timeout <-chan time.Time
	if d > 0 {
		timer := time.NewTimer(d)
		defer timer.Stop()
		timeout = timer.C
	}
	select {
	case <-l.wake:
	case <-timeout:
	case <-ctx.Done():
	}
}

// run generates a claimed task's unfinished items, then its result ZIP.
// Items finished in an earlier run keep their outcome.
func (l *reportLifecycle) run(ctx context.Context, id string) {
	var task reports.Task
	if !l.retry(ctx, id, func() (err error) { task, err = l.records.Get(ctx, id); return err }) {
		return
	}
	dir := l.taskDir(id)
	inputs, err := loadTaskInputs(dir, uploadItems(task.Items))
	if err != nil {
		l.fail(ctx, id, fmt.Sprintf("读取上传文件失败：%v", err))
		return
	}
	results := make([]ItemResult, 0, len(task.Items))
	for i, item := range task.Items {
		if !item.Status.Finished() {
			var ok bool
			if item, ok = l.runItem(ctx, itemRun{task: task, item: item, input: inputs[i]}); !ok {
				return
			}
			task.Items[i] = item
			l.hub.emitProgress(id, finishedItems(task.Items), len(task.Items), inputs[i].Name)
		}
		results = append(results, savedResult(dir, item, inputs[i].ID))
	}
	if err := buildResultZip(ResultZipPath(dir, id), results, inputs); err != nil {
		l.fail(ctx, id, err.Error())
		return
	}
	if l.finish(ctx, reports.TaskUpdate{TaskID: id, Status: reports.StatusDone}) {
		l.hub.emitDone(id, reportDownloadURL(id))
	}
}

// itemRun is one item of a claimed task, with its pipeline input.
type itemRun struct {
	task  reports.Task
	item  reports.Item
	input ItemInput
}

// runItem generates one item and returns it with its saved outcome; false
// when saving failed.
func (l *reportLifecycle) runItem(ctx context.Context, run itemRun) (reports.Item, bool) {
	id, input := run.task.ID, run.input
	update := reports.ItemUpdate{TaskID: id, Position: run.item.Position, Status: reports.StatusProcessing}
	if !l.setItemStatus(ctx, update) {
		return run.item, false
	}
	l.hub.emitLog(id, "info", "开始处理 "+input.Name)
	job := ItemJob{TaskID: id, TaskDir: l.taskDir(id), TaskCreatedAt: run.task.CreatedAt, Input: input}
	res := l.platform.Pipeline.RunItem(ctx, job, func(ev LogEvent) {
		level := "info"
		if ev.Stream == LogStderr {
			level = "error"
		}
		l.hub.emitLog(id, level, fmt.Sprintf("[%s] %s", input.Name, ev.Line))
	})
	update.Status, update.Reason = reports.StatusDone, ""
	if res.Status != ItemDone {
		update.Status, update.Reason = reports.StatusFailed, res.Error
	}
	if !l.setItemStatus(ctx, update) {
		return run.item, false
	}
	if update.Status == reports.StatusFailed {
		l.hub.emitLog(id, "error", fmt.Sprintf("[%s] 处理失败: %s", input.Name, update.Reason))
	}
	done := run.item
	done.Status, done.Reason = update.Status, update.Reason
	return done, true
}

func (l *reportLifecycle) fail(ctx context.Context, id, reason string) {
	if l.finish(ctx, reports.TaskUpdate{TaskID: id, Status: reports.StatusFailed, Error: reason}) {
		l.hub.emitError(id, reason)
	}
}

func (l *reportLifecycle) finish(ctx context.Context, u reports.TaskUpdate) bool {
	return l.retry(ctx, u.TaskID, func() error { return l.records.Finish(ctx, u) })
}

func (l *reportLifecycle) setItemStatus(ctx context.Context, u reports.ItemUpdate) bool {
	return l.retry(ctx, u.TaskID, func() error { return l.records.SetItemStatus(ctx, u) })
}

// retry runs op, a store call on task id, until it succeeds, waiting a
// backoff between tries, so no task stays processing while the server runs.
// It gives up, returning false, when the task is gone or ctx ends.
func (l *reportLifecycle) retry(ctx context.Context, id string, op func() error) bool {
	var wait time.Duration
	for {
		err := op()
		switch {
		case err == nil:
			return true
		case errors.Is(err, reports.ErrNotFound):
			l.platform.Log.Printf("[WARN] report task %s is gone; dropping it", id)
			return false
		}
		wait = l.backoff.after(wait)
		l.platform.Log.Printf("[ERROR] report task %s: store call failed, retrying in %s: %v", id, wait, err)
		select {
		case <-time.After(wait):
		case <-ctx.Done():
			return false
		}
	}
}

// savedResult is an item's saved outcome as the result ZIP needs it.
func savedResult(taskDir string, item reports.Item, itemID string) ItemResult {
	if item.Status == reports.StatusDone {
		return ItemResult{ID: itemID, Status: ItemDone, ReportDocx: ItemReportPath(taskDir, itemID)}
	}
	return ItemResult{ID: itemID, Status: ItemFailed, Error: item.Reason}
}

func finishedItems(items []reports.Item) int {
	return countFinished(items, func(it reports.Item) reports.Status { return it.Status })
}

// currentItem names the item being generated, if any.
func currentItem(items []reports.Item) string {
	for _, it := range items {
		if it.Status == reports.StatusProcessing {
			return it.FileName
		}
	}
	return ""
}

// taskRecords is what the report worker reads and saves. storeRecords keeps
// them in the platform store; tests wrap it to inject store failures.
type taskRecords interface {
	ClaimNext(ctx context.Context) (id string, ok bool, err error)
	Get(ctx context.Context, id string) (reports.Task, error)
	SetItemStatus(ctx context.Context, u reports.ItemUpdate) error
	Finish(ctx context.Context, u reports.TaskUpdate) error
}

type storeRecords struct{ db *store.DB }

func (r storeRecords) ClaimNext(ctx context.Context) (string, bool, error) {
	return reports.ClaimNext(ctx, r.db)
}

func (r storeRecords) Get(ctx context.Context, id string) (reports.Task, error) {
	return reports.Get(ctx, r.db, id)
}

func (r storeRecords) SetItemStatus(ctx context.Context, u reports.ItemUpdate) error {
	return reports.SetItemStatus(ctx, r.db, u)
}

func (r storeRecords) Finish(ctx context.Context, u reports.TaskUpdate) error {
	return reports.Finish(ctx, r.db, u)
}

// retryBackoff is how long the worker waits before retrying a failed store
// call: first, then twice as long each time, up to max.
type retryBackoff struct {
	first, max time.Duration
}

var defaultRetryBackoff = retryBackoff{first: time.Second, max: time.Minute}

// after returns the wait that follows prev; prev is zero before the first retry.
func (b retryBackoff) after(prev time.Duration) time.Duration {
	if prev <= 0 {
		return b.first
	}
	return min(2*prev, b.max)
}
