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
	if loaded.Version != 1 {
		t.Fatalf("expected initial version 1, got %d", loaded.Version)
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
	if updated.Version != 2 {
		t.Fatalf("expected updated version 2, got %d", updated.Version)
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
	if reloaded.Version != 2 {
		t.Fatalf("expected reloaded version 2, got %d", reloaded.Version)
	}
}

func TestTaskStoreUpdateUsesPersistedVersion(t *testing.T) {
	store, err := NewTaskStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewTaskStore failed: %v", err)
	}
	created, err := store.Create(Task{ID: "t1", Status: TaskQueued})
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	first, err := store.Update(Task{ID: "t1", Status: TaskProcessing, Version: 100, CreatedAt: created.CreatedAt})
	if err != nil {
		t.Fatalf("first Update failed: %v", err)
	}
	second, err := store.Update(Task{ID: "t1", Status: TaskDone, Version: -1, CreatedAt: created.CreatedAt})
	if err != nil {
		t.Fatalf("second Update failed: %v", err)
	}
	if first.Version != 2 || second.Version != 3 {
		t.Fatalf("updates did not derive versions from disk: first=%d second=%d", first.Version, second.Version)
	}
}

func TestTaskStoreUpdateInitializesLegacyVersion(t *testing.T) {
	store, err := NewTaskStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewTaskStore failed: %v", err)
	}
	legacy := Task{ID: "legacy", Status: TaskQueued, CreatedAt: time.Now(), UpdatedAt: time.Now()}
	if err := os.MkdirAll(store.taskDir(legacy.ID), 0o755); err != nil {
		t.Fatalf("create legacy task directory: %v", err)
	}
	if err := writeJSONFileAtomic(store.taskPath(legacy.ID), legacy); err != nil {
		t.Fatalf("write legacy task: %v", err)
	}

	updated, err := store.Update(Task{ID: legacy.ID, Status: TaskDone})
	if err != nil {
		t.Fatalf("Update legacy task failed: %v", err)
	}
	if updated.Version != 1 {
		t.Fatalf("expected legacy task update to start versioning at 1, got %d", updated.Version)
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
