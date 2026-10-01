package web

import (
	"context"
	"fmt"

	"dbcheck/reporter/internal/reports"
)

// Report tasks from before the store live in tasks/<id>/task.json and
// are not migrated. Those left queued or processing finish once at startup,
// ahead of the store's tasks, and the retention sweep deletes them under the
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
		l.platform.Log.Printf("[ERROR] listing legacy report tasks: %v", err)
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
			l.saveLegacy(task)
			continue
		}
		backlog = append(backlog, queuedTask{TaskID: task.ID, Items: items})
	}
	return backlog
}

// saveLegacy writes a legacy task's task.json, logging a failure: the task
// then stays as last written, and resumes or is retried at the next start.
func (l *reportLifecycle) saveLegacy(task Task) {
	if _, err := l.legacy.Update(task); err != nil {
		l.platform.Log.Printf("[ERROR] legacy report task %s: saving task.json failed: %v", task.ID, err)
	}
}

// runLegacy finishes one legacy task, writing its task.json as before.
func (l *reportLifecycle) runLegacy(ctx context.Context, queued queuedTask) {
	task, err := l.legacy.Load(queued.TaskID)
	if err != nil {
		l.platform.Log.Printf("[ERROR] loading legacy report task %s: %v", queued.TaskID, err)
		return
	}
	task = resumedLegacyTask(task, queued.Items)
	l.saveLegacy(task)

	taskDir := l.legacy.taskDir(task.ID)
	for i, input := range queued.Items {
		if legacyItemStatus(task.Items[i]).Finished() {
			continue
		}
		task.CurrentFile = input.Name
		l.saveLegacy(task)
		job := ItemJob{TaskID: task.ID, TaskDir: taskDir, TaskCreatedAt: task.CreatedAt, Input: input}
		result := l.platform.Pipeline.RunItem(ctx, job, func(LogEvent) {})
		task.Items[i].Status = string(result.Status)
		task.Items[i].Error = result.Error
		task.Items[i].ReportDocx = result.ReportDocx
		task.Completed = countFinished(task.Items, legacyItemStatus)
		l.saveLegacy(task)
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
	l.saveLegacy(task)
}

// resumedLegacyTask is task as processing again, with one item per input:
// the item it already had, or a new queued one.
func resumedLegacyTask(task Task, inputs []ItemInput) Task {
	prev := make(map[string]TaskItem, len(task.Items))
	for _, item := range task.Items {
		prev[item.ID] = item
	}
	task.Status = TaskProcessing
	task.Total = len(inputs)
	task.Error = ""
	task.Items = make([]TaskItem, 0, len(inputs))
	for _, input := range inputs {
		item, ok := prev[input.ID]
		if !ok {
			item = TaskItem{ID: input.ID, Name: input.Name, Status: string(TaskQueued)}
		}
		task.Items = append(task.Items, item)
	}
	task.Completed = countFinished(task.Items, legacyItemStatus)
	return task
}

// legacyItemStatus reads a legacy item's status; it uses the same words as
// the store's.
func legacyItemStatus(item TaskItem) reports.Status { return reports.Status(item.Status) }

// countFinished counts the items whose status is final.
func countFinished[T any](items []T, status func(T) reports.Status) int {
	n := 0
	for _, it := range items {
		if status(it).Finished() {
			n++
		}
	}
	return n
}
