package web

import (
	"context"
	"fmt"
	"time"
)

// Report tasks from before the store live in tasks/<id>/task.json and
// are not migrated. Those left queued or processing finish once at startup,
// ahead of the store's tasks, and the retention loop deletes them under the
// old rule: RetentionTTL (24 hours by default) after their last update.
// Nobody can reach them through the API any more: they had no submitter.

type queuedTask struct {
	TaskID string
	Items  []ItemInput
}

// legacyBacklog lists the unfinished legacy tasks in id order, marking those
// whose uploads cannot be read as failed.
func (l *reportLifecycle) legacyBacklog() []queuedTask {
	ids, err := l.legacy.ListIDs()
	if err != nil {
		return nil
	}
	var backlog []queuedTask
	for _, id := range ids {
		task, err := l.legacy.Load(id)
		if err != nil || (task.Status != TaskQueued && task.Status != TaskProcessing) {
			continue
		}
		items, err := loadTaskInputs(l.legacy.taskDir(task.ID), task.Items)
		if err != nil {
			task.Status = TaskFailed
			task.Error = fmt.Sprintf("resume failed: %v", err)
			task.CurrentFile = ""
			_, _ = l.legacy.Update(task)
			continue
		}
		backlog = append(backlog, queuedTask{TaskID: task.ID, Items: items})
	}
	return backlog
}

// runLegacy finishes one legacy task, writing its task.json as before.
func (l *reportLifecycle) runLegacy(ctx context.Context, queued queuedTask) {
	store := l.legacy
	task, err := store.Load(queued.TaskID)
	if err != nil {
		return
	}
	task.Status = TaskProcessing
	task.Total = len(queued.Items)
	task.Error = ""

	prev := make(map[string]TaskItem, len(task.Items))
	for _, item := range task.Items {
		prev[item.ID] = item
	}
	task.Items = make([]TaskItem, 0, len(queued.Items))
	for _, input := range queued.Items {
		item, ok := prev[input.ID]
		if !ok {
			item = TaskItem{ID: input.ID, Name: input.Name, Status: string(TaskQueued)}
		}
		task.Items = append(task.Items, item)
	}
	task.Completed = countProcessed(task.Items)
	_, _ = store.Update(task)

	taskDir := store.taskDir(task.ID)
	for i, input := range queued.Items {
		if task.Items[i].Status == string(ItemDone) || task.Items[i].Status == string(ItemFailed) {
			continue
		}
		task.CurrentFile = input.Name
		_, _ = store.Update(task)
		job := ItemJob{TaskID: task.ID, TaskDir: taskDir, TaskCreatedAt: task.CreatedAt, Input: input}
		result := l.platform.Pipeline.RunItem(ctx, job, func(LogEvent) {})
		task.Items[i].Status = string(result.Status)
		task.Items[i].Error = result.Error
		task.Items[i].ReportDocx = result.ReportDocx
		task.Completed = countProcessed(task.Items)
		_, _ = store.Update(task)
	}

	results := make([]ItemResult, 0, len(task.Items))
	for _, item := range task.Items {
		results = append(results, ItemResult{ID: item.ID, Status: ItemStatus(item.Status), ReportDocx: item.ReportDocx, Error: item.Error})
	}
	task.Status = TaskDone
	task.CurrentFile = ""
	if err := buildResultZip(ResultZipPath(taskDir, task.ID), results, queued.Items); err != nil {
		task.Status = TaskFailed
		task.Error = err.Error()
	}
	_, _ = store.Update(task)
}

func countProcessed(items []TaskItem) int {
	n := 0
	for _, item := range items {
		if item.Status == string(ItemDone) || item.Status == string(ItemFailed) {
			n++
		}
	}
	return n
}

// startLegacyRetention deletes finished legacy tasks RetentionTTL after
// their last update.
func (l *reportLifecycle) startLegacyRetention() {
	ttl := l.cfg.RetentionTTL
	if ttl <= 0 {
		return
	}
	interval := min(time.Hour, ttl/2)
	interval = max(interval, time.Minute)
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for range ticker.C {
			_, _ = cleanupExpiredTasks(l.legacy, ttl, time.Now)
		}
	}()
}
