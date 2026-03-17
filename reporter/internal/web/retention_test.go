package web

import (
	"os"
	"testing"
	"time"
)

func TestCleanupExpiredTasksDeletesOnlyDoneOrFailed(t *testing.T) {
	store, err := NewTaskStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewTaskStore failed: %v", err)
	}

	baseNow := time.Date(2026, 3, 16, 12, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return baseNow.Add(-2 * time.Hour) }
	if _, err := store.Create(Task{ID: "t1", Status: TaskDone}); err != nil {
		t.Fatalf("Create t1 failed: %v", err)
	}
	if _, err := store.Create(Task{ID: "t2", Status: TaskProcessing}); err != nil {
		t.Fatalf("Create t2 failed: %v", err)
	}

	deleted, err := cleanupExpiredTasks(store, time.Hour, func() time.Time { return baseNow })
	if err != nil {
		t.Fatalf("cleanupExpiredTasks failed: %v", err)
	}
	if deleted != 1 {
		t.Fatalf("expected deleted=1 got %d", deleted)
	}

	if _, err := os.Stat(store.taskDir("t1")); !os.IsNotExist(err) {
		t.Fatalf("expected t1 removed, got err=%v", err)
	}
	if _, err := os.Stat(store.taskDir("t2")); err != nil {
		t.Fatalf("expected t2 kept, got err=%v", err)
	}
}
