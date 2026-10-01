package web

import (
	"context"
	"fmt"
	"mime/multipart"
	"os"
	"path/filepath"
	"strconv"
	"sync"

	"dbcheck/reporter/internal/apierr"
	"dbcheck/reporter/internal/reports"
	"dbcheck/reporter/internal/store"
)

// reportLifecycle is the single writer of report tasks (ADR 0001). It admits
// submissions, schedules them from the store, runs their items through the
// pipeline, saves every state change, resumes interrupted tasks at startup,
// and sends each notification only after the state it announces is saved.
//
// Records live in the store (package reports, ADR 0003); each task's files
// live in tasks/<id>/: uploads/, items/<position>/, and the result ZIP.
type reportLifecycle struct {
	cfg      Config
	platform Platform
	tasksDir string
	hub      *taskHub
	// legacy holds the task.json tasks from before the store (legacy_tasks.go).
	legacy *TaskStore
	// records are the store's report task records; backoff paces the
	// worker's retries when the store fails.
	records taskRecords
	backoff retryBackoff
	// wake only wakes the worker; the store says what there is to do.
	wake chan struct{}
	once sync.Once
}

func newReportLifecycle(cfg Config, p Platform, hub *taskHub) (*reportLifecycle, error) {
	legacy, err := NewTaskStore(cfg.DataDir)
	if err != nil {
		return nil, err
	}
	return &reportLifecycle{
		cfg:      cfg,
		platform: p,
		tasksDir: legacy.tasksDir,
		hub:      hub,
		legacy:   legacy,
		records:  storeRecords{p.DB},
		backoff:  defaultRetryBackoff,
		wake:     make(chan struct{}, 1),
	}, nil
}

func (l *reportLifecycle) taskDir(id string) string { return filepath.Join(l.tasksDir, id) }

// ResultZipPath is where a finished task's downloadable result ZIP lives.
func ResultZipPath(taskDir, id string) string {
	return filepath.Join(taskDir, fmt.Sprintf("reports-%s.zip", id))
}

// reportDownloadURL is where a finished task's result ZIP downloads from.
func reportDownloadURL(id string) string { return "/api/reports/download/" + id }

// submit admits a report task: it saves the uploads, reads each ZIP, and
// stores the task as queued before returning, so an acknowledged task is
// never lost. A refused submission leaves nothing behind.
func (l *reportLifecycle) submit(ctx context.Context, submitterID string, form *multipart.Form) (reports.Task, error) {
	zips := filesForKey(form, "zips")
	if len(zips) == 0 {
		zips = filesForKey(form, "zip")
	}
	if len(zips) == 0 {
		return reports.Task{}, apierr.Invalid("请至少上传一个 ZIP 文件")
	}
	id, err := newTaskID()
	if err != nil {
		return reports.Task{}, err
	}
	task := reports.Task{ID: id, SubmitterID: submitterID, Status: reports.StatusQueued, CreatedAt: l.platform.Now()}
	dir := l.taskDir(id)
	task.Items, err = uploadForm{form: form, zips: zips}.stage(filepath.Join(dir, "uploads"))
	if err == nil {
		err = l.platform.DB.Tx(ctx, func(tx store.Querier) error { return reports.Insert(ctx, tx, task) })
	}
	if err != nil {
		_ = os.RemoveAll(dir)
		return reports.Task{}, err
	}
	l.notify()
	return task, nil
}

func (l *reportLifecycle) notify() {
	select {
	case l.wake <- struct{}{}:
	default:
	}
}

// start resumes interrupted work and starts the worker, once.
func (l *reportLifecycle) start() {
	l.once.Do(func() {
		if l.platform.Pipeline == nil {
			exe, err := os.Executable()
			if err != nil {
				l.platform.Log.Printf("[ERROR] report worker not started: %v", err)
				return
			}
			l.platform.Pipeline = NewPipeline(exe, l.cfg.PythonBin)
		}
		ctx := context.Background()
		if err := reports.RequeueInterrupted(ctx, l.platform.DB); err != nil {
			l.platform.Log.Printf("[ERROR] resuming report tasks: %v", err)
		}
		legacy := l.legacyBacklog()
		l.startRetention(ctx)
		go l.work(ctx, legacy)
	})
}

// uploadItems names the items as loadTaskInputs expects them.
func uploadItems(items []reports.Item) []TaskItem {
	out := make([]TaskItem, 0, len(items))
	for _, it := range items {
		out = append(out, TaskItem{ID: strconv.Itoa(it.Position), Name: it.FileName})
	}
	return out
}
