package web

import (
	"os"
	"path/filepath"
	"testing"
)

func TestTaskLifecycleResumeTasksEnqueuesQueuedOrProcessing(t *testing.T) {
	cfg := Config{
		DataDir:        t.TempDir(),
		AllowedOrigins: []string{"http://example.com"},
		APIToken:       defaultAPIToken,
	}
	lifecycle, err := NewTaskLifecycle(cfg)
	if err != nil {
		t.Fatalf("NewTaskLifecycle failed: %v", err)
	}

	// Create a processing task with a persisted upload.
	task, err := lifecycle.store.Create(Task{ID: "t1", Status: TaskProcessing})
	if err != nil {
		t.Fatalf("Create task failed: %v", err)
	}
	uploadsDir := filepath.Join(lifecycle.store.taskDir(task.ID), "uploads")
	if err := os.MkdirAll(uploadsDir, 0o755); err != nil {
		t.Fatalf("mkdir uploads failed: %v", err)
	}
	zipName := "zip-1-demo.zip"
	if err := os.WriteFile(filepath.Join(uploadsDir, zipName), []byte("x"), 0o644); err != nil {
		t.Fatalf("write zip failed: %v", err)
	}

	lifecycle.resumeTasks()

	select {
	case job := <-lifecycle.queue:
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
