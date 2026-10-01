package web

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"dbcheck/reporter/internal/reports"
)

// retentionSweepInterval is how often the retention loop looks for expired
// report tasks. A task reads as expired the moment it passes retention; the
// loop deletes its files at the next sweep.
const retentionSweepInterval = time.Hour

// startRetention removes expired tasks' files now and then every
// retentionSweepInterval.
func (l *reportLifecycle) startRetention(ctx context.Context) {
	sweep := func() {
		if err := l.removeExpiredFiles(ctx); err != nil {
			l.platform.Log.Printf("[ERROR] report retention: %v", err)
		}
	}
	sweep()
	go func() {
		ticker := time.NewTicker(retentionSweepInterval)
		defer ticker.Stop()
		for range ticker.C {
			sweep()
		}
	}()
}

// removeExpiredFiles deletes the files of every finished report task past
// retention (reports.Retention): its uploads, extracted data, and reports.
// The record stays. Directories without a record, such as legacy task.json
// tasks, are left to their own rule; an unfinished task waits until it
// finishes, so the worker never loses its inputs mid-run.
func (l *reportLifecycle) removeExpiredFiles(ctx context.Context) error {
	entries, err := os.ReadDir(l.tasksDir)
	if err != nil {
		return err
	}
	now := l.platform.Now()
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		task, err := reports.Get(ctx, l.platform.DB, e.Name())
		if errors.Is(err, reports.ErrNotFound) {
			continue
		}
		if err != nil {
			return err
		}
		if !task.Status.Finished() || !task.Expired(now) {
			continue
		}
		if err := os.RemoveAll(l.taskDir(task.ID)); err != nil {
			return fmt.Errorf("remove files of expired task %s: %w", task.ID, err)
		}
	}
	return nil
}
