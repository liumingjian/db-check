package web

import (
	"fmt"
	"path/filepath"
	"strings"
)

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

func newTaskSnapshot(task Task, taskDir string, storageFault *StorageFault) TaskSnapshot {
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
		Version:        task.Version,
		StorageFault:   storageFault,
	}
	if len(task.Items) > 0 {
		snapshot.Items = append([]TaskItem(nil), task.Items...)
		for i := range snapshot.Items {
			snapshot.Items[i].ReportDocx = taskRelativeArtifactPath(taskDir, snapshot.Items[i].ReportDocx)
		}
	}
	if task.Status == TaskDone {
		snapshot.DownloadURL = fmt.Sprintf("/api/reports/download/%s", task.ID)
	}
	return snapshot
}

func taskRelativeArtifactPath(taskDir string, artifactPath string) string {
	artifactPath = strings.TrimSpace(artifactPath)
	if artifactPath == "" {
		return ""
	}
	if !filepath.IsAbs(artifactPath) {
		return cleanRelativeArtifactPath(artifactPath)
	}
	relative, err := filepath.Rel(taskDir, artifactPath)
	if err != nil {
		return ""
	}
	return cleanRelativeArtifactPath(relative)
}

func cleanRelativeArtifactPath(path string) string {
	path = filepath.Clean(path)
	if path == "." || path == ".." || strings.HasPrefix(path, ".."+string(filepath.Separator)) {
		return ""
	}
	return filepath.ToSlash(path)
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
