package web

import (
	"errors"
	"fmt"
	"os"
	"time"
)

func cleanupExpiredTasks(store *TaskStore, ttl time.Duration, now func() time.Time) (int, error) {
	if ttl <= 0 {
		return 0, nil
	}
	if now == nil {
		now = time.Now
	}

	ids, err := store.ListIDs()
	if err != nil {
		return 0, err
	}
	deleted := 0
	cutoff := now().Add(-ttl)
	for _, id := range ids {
		task, err := store.Load(id)
		if err != nil {
			if errors.Is(err, ErrTaskNotFound) {
				continue
			}
			return deleted, err
		}
		if task.Status != TaskDone && task.Status != TaskFailed {
			continue
		}
		if task.UpdatedAt.After(cutoff) {
			continue
		}
		if err := os.RemoveAll(store.taskDir(id)); err != nil {
			return deleted, fmt.Errorf("remove task dir failed: %w", err)
		}
		deleted++
	}
	return deleted, nil
}
