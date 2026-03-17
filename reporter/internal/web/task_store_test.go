package web

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestTaskStoreCreateLoadUpdatePersistsJSON(t *testing.T) {
	dataDir := t.TempDir()
	store, err := NewTaskStore(dataDir)
	if err != nil {
		t.Fatalf("NewTaskStore failed: %v", err)
	}
	baseNow := time.Date(2026, 3, 16, 12, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return baseNow }

	created, err := store.Create(Task{
		ID:     "t1",
		Status: TaskQueued,
	})
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	taskPath := filepath.Join(dataDir, "tasks", "t1", "task.json")
	if _, err := os.Stat(taskPath); err != nil {
		t.Fatalf("expected task.json to exist: %v", err)
	}

	loaded, err := store.Load("t1")
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if loaded.ID != created.ID || loaded.Status != created.Status {
		t.Fatalf("unexpected loaded task: %#v", loaded)
	}
	if !loaded.CreatedAt.Equal(baseNow) || !loaded.UpdatedAt.Equal(baseNow) {
		t.Fatalf("unexpected timestamps: created=%s updated=%s", loaded.CreatedAt, loaded.UpdatedAt)
	}

	// Update and verify it is persisted on disk.
	store.now = func() time.Time { return baseNow.Add(5 * time.Minute) }
	updated, err := store.Update(Task{
		ID:        "t1",
		Status:    TaskDone,
		CreatedAt: loaded.CreatedAt,
	})
	if err != nil {
		t.Fatalf("Update failed: %v", err)
	}
	if !updated.UpdatedAt.Equal(baseNow.Add(5 * time.Minute)) {
		t.Fatalf("unexpected updated_at: %s", updated.UpdatedAt)
	}

	reloaded, err := store.Load("t1")
	if err != nil {
		t.Fatalf("Load after update failed: %v", err)
	}
	if reloaded.Status != TaskDone {
		t.Fatalf("expected status=%q got %q", TaskDone, reloaded.Status)
	}
	if !reloaded.UpdatedAt.Equal(baseNow.Add(5 * time.Minute)) {
		t.Fatalf("unexpected reloaded updated_at: %s", reloaded.UpdatedAt)
	}
}

func TestTaskStoreCreateRejectsDuplicate(t *testing.T) {
	store, err := NewTaskStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewTaskStore failed: %v", err)
	}
	if _, err := store.Create(Task{ID: "t1", Status: TaskQueued}); err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	if _, err := store.Create(Task{ID: "t1", Status: TaskQueued}); err != ErrTaskAlreadyExists {
		t.Fatalf("expected ErrTaskAlreadyExists, got %v", err)
	}
}

func TestTaskStoreLoadNotFound(t *testing.T) {
	store, err := NewTaskStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewTaskStore failed: %v", err)
	}
	if _, err := store.Load("missing"); err != ErrTaskNotFound {
		t.Fatalf("expected ErrTaskNotFound, got %v", err)
	}
}

func TestTaskStoreRejectsInvalidID(t *testing.T) {
	store, err := NewTaskStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewTaskStore failed: %v", err)
	}
	if _, err := store.Create(Task{ID: "../x", Status: TaskQueued}); err != ErrInvalidTaskID {
		t.Fatalf("expected ErrInvalidTaskID, got %v", err)
	}
	if _, err := store.Load("../x"); err != ErrInvalidTaskID {
		t.Fatalf("expected ErrInvalidTaskID, got %v", err)
	}
	if _, err := store.Update(Task{ID: "../x", Status: TaskDone}); err != ErrInvalidTaskID {
		t.Fatalf("expected ErrInvalidTaskID, got %v", err)
	}
}
