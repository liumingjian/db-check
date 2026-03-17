package web

import (
	"os"
	"path/filepath"
	"testing"
)

func TestResumeTasksEnqueuesQueuedOrProcessing(t *testing.T) {
	cfg := Config{
		DataDir:        t.TempDir(),
		AllowedOrigins: []string{"http://example.com"},
		APIToken:       "secret",
	}
	h, err := newAPIHandler(cfg, false)
	if err != nil {
		t.Fatalf("newAPIHandler failed: %v", err)
	}

	// Create a processing task with a persisted upload.
	task, err := h.store.Create(Task{ID: "t1", Status: TaskProcessing})
	if err != nil {
		t.Fatalf("Create task failed: %v", err)
	}
	uploadsDir := filepath.Join(h.store.taskDir(task.ID), "uploads")
	if err := os.MkdirAll(uploadsDir, 0o755); err != nil {
		t.Fatalf("mkdir uploads failed: %v", err)
	}
	zipName := "zip-1-demo.zip"
	if err := os.WriteFile(filepath.Join(uploadsDir, zipName), []byte("x"), 0o644); err != nil {
		t.Fatalf("write zip failed: %v", err)
	}

	h.resumeTasks()

	select {
	case job := <-h.queue:
		if job.TaskID != "t1" {
			t.Fatalf("unexpected task id: %q", job.TaskID)
		}
		if len(job.Items) != 1 {
			t.Fatalf("expected 1 item got %d", len(job.Items))
		}
		if job.Items[0].ID != "1" || job.Items[0].Name != "demo.zip" {
			t.Fatalf("unexpected item: %#v", job.Items[0])
		}
		if job.Items[0].ZipPath == "" {
			t.Fatalf("expected ZipPath")
		}
	default:
		t.Fatalf("expected a resumed task to be enqueued")
	}
}
