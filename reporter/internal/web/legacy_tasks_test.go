package web

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLegacyBacklogListsUnfinishedFileBasedTasks(t *testing.T) {
	cfg := Config{DataDir: t.TempDir(), AllowedOrigins: []string{"http://example.com"}}
	h, err := newAPIHandler(cfg, testPlatform(t, cfg.DataDir), false)
	if err != nil {
		t.Fatalf("newAPIHandler failed: %v", err)
	}
	legacy := h.reports.legacy

	// A processing task.json from before the store, with its upload.
	task, err := legacy.Create(Task{ID: "t1", Status: TaskProcessing})
	if err != nil {
		t.Fatalf("Create task failed: %v", err)
	}
	if _, err := legacy.Create(Task{ID: "t2", Status: TaskDone}); err != nil {
		t.Fatalf("Create task failed: %v", err)
	}
	uploadsDir := filepath.Join(legacy.taskDir(task.ID), "uploads")
	if err := os.MkdirAll(uploadsDir, 0o755); err != nil {
		t.Fatalf("mkdir uploads failed: %v", err)
	}
	if err := os.WriteFile(filepath.Join(uploadsDir, "zip-1-demo.zip"), []byte("x"), 0o644); err != nil {
		t.Fatalf("write zip failed: %v", err)
	}

	backlog := h.reports.legacyBacklog()
	if len(backlog) != 1 || backlog[0].TaskID != "t1" {
		t.Fatalf("backlog = %#v, want only t1", backlog)
	}
	items := backlog[0].Items
	if len(items) != 1 || items[0].ID != "1" || items[0].Name != "demo.zip" || items[0].ZipPath == "" {
		t.Fatalf("unexpected items: %#v", items)
	}
}
