package web

import "fmt"

// TaskSnapshot is the authoritative client representation of a persisted task.
// Extend this type when task-state fields become available so HTTP and WebSocket
// clients receive the same contract.
type TaskSnapshot struct {
	TaskID         string        `json:"task_id"`
	Status         TaskStatus    `json:"status"`
	Total          int           `json:"total"`
	Completed      int           `json:"completed"`
	SucceededCount int           `json:"succeeded_count"`
	FailedCount    int           `json:"failed_count"`
	CurrentFile    string        `json:"current_file"`
	Error          string        `json:"error,omitempty"`
	Items          []TaskItem    `json:"items,omitempty"`
	DownloadURL    string        `json:"download_url,omitempty"`
	Version        int64         `json:"version"`
	StorageFault   *StorageFault `json:"storage_fault,omitempty"`
}

func newTaskSnapshot(task Task, version int64, storageFault *StorageFault) TaskSnapshot {
	succeeded, failed := countItemOutcomes(task.Items)
	snapshot := TaskSnapshot{
		TaskID:         task.ID,
		Status:         task.Status,
		Total:          task.Total,
		Completed:      task.Completed,
		SucceededCount: succeeded,
		FailedCount:    failed,
		CurrentFile:    task.CurrentFile,
		Error:          task.Error,
		Version:        version,
		StorageFault:   storageFault,
	}
	if len(task.Items) > 0 {
		snapshot.Items = append([]TaskItem(nil), task.Items...)
	}
	if task.Status == TaskDone {
		snapshot.DownloadURL = fmt.Sprintf("/api/reports/download/%s", task.ID)
	}
	return snapshot
}

func countItemOutcomes(items []TaskItem) (succeeded, failed int) {
	for _, item := range items {
		switch item.Status {
		case string(ItemDone):
			succeeded++
		case string(ItemFailed):
			failed++
		}
	}
	return succeeded, failed
}
