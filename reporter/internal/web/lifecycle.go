package web

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

var (
	ErrLifecycleClosed   = errors.New("task lifecycle is closed")
	ErrTaskNotFinished   = errors.New("task not finished")
	ErrInvalidSubmission = errors.New("invalid report submission")
)

type queuedTask struct {
	TaskID string
	Items  []ItemInput
}

// SubmissionFile represents one uploaded input without depending on HTTP transport types.
type SubmissionFile struct {
	Name string
	Open func() (io.ReadCloser, error)
}

type ReportItemSubmission struct {
	Zip  SubmissionFile
	AWR  *SubmissionFile
	WDRs []SubmissionFile
}

type ReportSubmission struct {
	Items []ReportItemSubmission
}

type TaskWatch struct {
	Task         Task
	LastSequence int64
	Logs         [][]byte
	Updates      <-chan []byte

	closeOnce sync.Once
	cancel    func()
}

func (w *TaskWatch) Close() {
	if w == nil || w.cancel == nil {
		return
	}
	w.closeOnce.Do(w.cancel)
}

type pipelineFactory func() (*Pipeline, error)

// TaskLifecycle owns report task submission, execution, recovery, retention, and live updates.
type TaskLifecycle struct {
	cfg   Config
	store *TaskStore
	hub   *taskHub
	queue chan queuedTask

	newPipeline pipelineFactory

	mu        sync.Mutex
	started   bool
	closed    bool
	startOnce sync.Once
	startErr  error
	closeOnce sync.Once

	stop          chan struct{}
	workerDone    chan struct{}
	retentionDone chan struct{}
	workerOnce    sync.Once
	retentionOnce sync.Once
}

func NewTaskLifecycle(cfg Config) (*TaskLifecycle, error) {
	return newTaskLifecycle(cfg, nil)
}

func newTaskLifecycle(cfg Config, newPipeline pipelineFactory) (*TaskLifecycle, error) {
	store, err := NewTaskStore(cfg.DataDir)
	if err != nil {
		return nil, err
	}
	if newPipeline == nil {
		newPipeline = func() (*Pipeline, error) {
			executable, err := os.Executable()
			if err != nil {
				return nil, err
			}
			return NewPipeline(executable, cfg.PythonBin), nil
		}
	}

	return &TaskLifecycle{
		cfg:           cfg,
		store:         store,
		hub:           newTaskHub(cfg.LogReplayLines),
		queue:         make(chan queuedTask, 32),
		newPipeline:   newPipeline,
		stop:          make(chan struct{}),
		workerDone:    make(chan struct{}),
		retentionDone: make(chan struct{}),
	}, nil
}

func (l *TaskLifecycle) Start(ctx context.Context) error {
	l.startOnce.Do(func() {
		if err := ctx.Err(); err != nil {
			l.startErr = err
			l.finishWorker()
			l.finishRetention()
			return
		}

		l.mu.Lock()
		if l.closed {
			l.mu.Unlock()
			l.startErr = ErrLifecycleClosed
			l.finishWorker()
			l.finishRetention()
			return
		}
		l.started = true
		l.mu.Unlock()

		pipeline, err := l.newPipeline()
		if err != nil {
			l.startErr = fmt.Errorf("create report pipeline: %w", err)
			l.finishWorker()
			l.finishRetention()
			return
		}

		l.resumeTasks()
		l.startRetentionCleanup()
		go l.workerLoop(pipeline)
	})
	return l.startErr
}

func (l *TaskLifecycle) Close(ctx context.Context) error {
	l.closeOnce.Do(func() {
		l.mu.Lock()
		l.closed = true
		started := l.started
		close(l.stop)
		close(l.queue)
		l.mu.Unlock()

		if !started {
			l.finishWorker()
			l.finishRetention()
		}
	})

	if err := waitForLifecycle(ctx, l.workerDone); err != nil {
		return err
	}
	return waitForLifecycle(ctx, l.retentionDone)
}

func waitForLifecycle(ctx context.Context, done <-chan struct{}) error {
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (l *TaskLifecycle) Submit(ctx context.Context, submission ReportSubmission) (Task, error) {
	if err := ctx.Err(); err != nil {
		return Task{}, err
	}
	if len(submission.Items) == 0 {
		return Task{}, invalidSubmission(errors.New("missing zip files (field: zips)"))
	}

	l.mu.Lock()
	closed := l.closed
	l.mu.Unlock()
	if closed {
		return Task{}, ErrLifecycleClosed
	}

	taskID, err := newTaskID()
	if err != nil {
		return Task{}, err
	}
	task, err := l.store.Create(Task{
		ID:        taskID,
		Status:    TaskProcessing,
		Total:     len(submission.Items),
		Completed: 0,
	})
	if err != nil {
		return Task{}, err
	}

	uploadsDir := filepath.Join(l.store.taskDir(task.ID), "uploads")
	if err := os.MkdirAll(uploadsDir, 0o755); err != nil {
		return Task{}, fmt.Errorf("create uploads dir failed: %w", err)
	}

	items := make([]ItemInput, 0, len(submission.Items))
	for i, submitted := range submission.Items {
		itemID := fmt.Sprintf("%d", i+1)
		zipPath, name, err := saveSubmissionFile(uploadsDir, "zip", itemID, submitted.Zip)
		if err != nil {
			return Task{}, err
		}

		var awrPath string
		if submitted.AWR != nil {
			awrPath, _, err = saveSubmissionFile(uploadsDir, "awr", itemID, *submitted.AWR)
			if err != nil {
				return Task{}, err
			}
		}

		wdrPaths := make([]string, 0, len(submitted.WDRs))
		for _, wdr := range submitted.WDRs {
			path, _, err := saveSubmissionFile(uploadsDir, "wdr", wdrUploadID(itemID, len(wdrPaths)), wdr)
			if err != nil {
				return Task{}, err
			}
			wdrPaths = append(wdrPaths, path)
		}

		items = append(items, ItemInput{
			ID:       itemID,
			Name:     name,
			ZipPath:  zipPath,
			AWRPath:  awrPath,
			WDRPaths: wdrPaths,
		})
	}

	l.enqueue(queuedTask{TaskID: task.ID, Items: items})
	return task, nil
}

func (l *TaskLifecycle) Get(ctx context.Context, taskID string) (Task, error) {
	if err := ctx.Err(); err != nil {
		return Task{}, err
	}
	return l.store.Load(taskID)
}

func (l *TaskLifecycle) Download(ctx context.Context, taskID string) (path string, size int64, err error) {
	task, err := l.Get(ctx, taskID)
	if err != nil {
		return "", 0, err
	}
	if task.Status != TaskDone {
		return "", 0, ErrTaskNotFinished
	}

	path = filepath.Join(l.store.taskDir(task.ID), fmt.Sprintf("reports-%s.zip", task.ID))
	info, err := os.Stat(path)
	if err != nil {
		return "", 0, fmt.Errorf("result zip not found: %w", err)
	}
	return path, info.Size(), nil
}

func (l *TaskLifecycle) Watch(ctx context.Context, taskID string) (*TaskWatch, error) {
	task, err := l.Get(ctx, taskID)
	if err != nil {
		return nil, err
	}

	lastSequence, logs := l.hub.snapshot(taskID)
	updates, cancel := l.hub.subscribe(taskID)
	if err := ctx.Err(); err != nil {
		cancel()
		return nil, err
	}
	return &TaskWatch{
		Task:         task,
		LastSequence: lastSequence,
		Logs:         logs,
		Updates:      updates,
		cancel:       cancel,
	}, nil
}

func (l *TaskLifecycle) enqueue(task queuedTask) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return
	}
	select {
	case l.queue <- task:
	default:
		// Queue full: drop on the floor but keep the task record.
		// This should be rare under single-worker design.
	}
}

func (l *TaskLifecycle) workerLoop(pipeline *Pipeline) {
	defer l.finishWorker()
	for {
		select {
		case <-l.stop:
			return
		default:
		}

		select {
		case <-l.stop:
			return
		case task, ok := <-l.queue:
			if !ok {
				return
			}
			l.runTask(pipeline, task)
		}
	}
}

func (l *TaskLifecycle) startRetentionCleanup() {
	if l.cfg.RetentionTTL <= 0 {
		l.finishRetention()
		return
	}

	interval := time.Hour
	if l.cfg.RetentionTTL < interval {
		interval = l.cfg.RetentionTTL / 2
	}
	if interval < time.Minute {
		interval = time.Minute
	}

	go func() {
		defer l.finishRetention()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-l.stop:
				return
			case <-ticker.C:
				_, _ = cleanupExpiredTasks(l.store, l.cfg.RetentionTTL, time.Now)
			}
		}
	}()
}

func (l *TaskLifecycle) runTask(pipeline *Pipeline, queued queuedTask) {
	task, err := l.store.Load(queued.TaskID)
	if err != nil {
		return
	}
	task.Status = TaskProcessing
	task.Total = len(queued.Items)
	task.Error = ""

	// Rebuild Items in the input order, preserving previous status/error/report paths.
	prev := make(map[string]TaskItem, len(task.Items))
	for _, item := range task.Items {
		prev[item.ID] = item
	}
	task.Items = make([]TaskItem, 0, len(queued.Items))
	for _, input := range queued.Items {
		item, ok := prev[input.ID]
		if !ok {
			item = TaskItem{ID: input.ID, Status: string(TaskQueued)}
		}
		if item.Name == "" {
			item.Name = input.Name
		}
		task.Items = append(task.Items, item)
	}
	task.Completed = countProcessed(task.Items)
	task.CurrentFile = ""
	_, _ = l.store.Update(task)

	taskDir := l.store.taskDir(task.ID)
	for i, input := range queued.Items {
		// Resume: skip items already completed in a previous run.
		if i < len(task.Items) {
			if task.Items[i].Status == string(ItemDone) || task.Items[i].Status == string(ItemFailed) {
				continue
			}
		}

		task.CurrentFile = input.Name
		_, _ = l.store.Update(task)

		l.hub.emitLog(task.ID, "info", fmt.Sprintf("开始处理 %s", input.Name))
		result := pipeline.runOne(taskDir, input, func(itemID string, ev LogEvent) {
			level := "info"
			if ev.Stream == LogStderr {
				level = "error"
			}
			msg := ev.Line
			if input.Name != "" {
				msg = fmt.Sprintf("[%s] %s", input.Name, msg)
			}
			l.hub.emitLog(task.ID, level, msg)
		})
		if i < len(task.Items) && task.Items[i].ID == result.ID {
			task.Items[i].Status = string(result.Status)
			task.Items[i].Error = result.Error
			task.Items[i].ReportDocx = result.ReportDocx
		}
		if result.Status == ItemFailed {
			l.hub.emitLog(task.ID, "error", fmt.Sprintf("[%s] 处理失败: %s", input.Name, result.Error))
		}
		task.Completed = countProcessed(task.Items)
		_, _ = l.store.Update(task)

		l.hub.emitProgress(task.ID, task.Completed, task.Total, task.CurrentFile)
	}

	// Build download zip from all completed items (including previous runs).
	results := make([]ItemResult, 0, len(task.Items))
	for _, item := range task.Items {
		switch item.Status {
		case string(ItemDone):
			results = append(results, ItemResult{ID: item.ID, Status: ItemDone, ReportDocx: item.ReportDocx})
		case string(ItemFailed):
			results = append(results, ItemResult{ID: item.ID, Status: ItemFailed, Error: item.Error})
		}
	}

	zipPath := filepath.Join(taskDir, fmt.Sprintf("reports-%s.zip", task.ID))
	if err := buildResultZip(zipPath, results, queued.Items); err != nil {
		task.Status = TaskFailed
		task.Error = err.Error()
		task.CurrentFile = ""
		_, _ = l.store.Update(task)
		l.hub.emitError(task.ID, err.Error())
		return
	}

	task.Status = TaskDone
	task.CurrentFile = ""
	_, _ = l.store.Update(task)
	l.hub.emitDone(task.ID, fmt.Sprintf("/api/reports/download/%s", task.ID))
}

func (l *TaskLifecycle) finishWorker() {
	l.workerOnce.Do(func() {
		close(l.workerDone)
	})
}

func (l *TaskLifecycle) finishRetention() {
	l.retentionOnce.Do(func() {
		close(l.retentionDone)
	})
}

func countProcessed(items []TaskItem) int {
	n := 0
	for _, item := range items {
		if item.Status == string(ItemDone) || item.Status == string(ItemFailed) {
			n++
		}
	}
	return n
}

func newTaskID() (string, error) {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf[:]), nil
}

func wdrUploadID(itemID string, existing int) string {
	return fmt.Sprintf("%s-%d", itemID, existing+1)
}

func saveSubmissionFile(dir string, kind string, itemID string, file SubmissionFile) (string, string, error) {
	name := filepath.Base(file.Name)
	if file.Open == nil {
		return "", "", invalidSubmission(errors.New("missing file"))
	}
	if strings.TrimSpace(name) == "" {
		return "", "", invalidSubmission(errors.New("invalid filename"))
	}

	ext := strings.ToLower(filepath.Ext(name))
	if kind == "zip" && ext != ".zip" {
		return "", "", invalidSubmission(fmt.Errorf("invalid zip filename: %q", name))
	}
	if (kind == "awr" || kind == "wdr") && ext != ".html" && ext != ".htm" {
		return "", "", invalidSubmission(fmt.Errorf("invalid %s filename: %q", kind, name))
	}

	src, err := file.Open()
	if err != nil {
		return "", "", invalidSubmission(fmt.Errorf("open upload failed: %w", err))
	}
	defer src.Close()

	dstName := fmt.Sprintf("%s-%s-%s", kind, itemID, name)
	dstPath := filepath.Join(dir, dstName)
	dst, err := os.OpenFile(dstPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return "", "", invalidSubmission(fmt.Errorf("save upload failed: %w", err))
	}
	defer dst.Close()
	if _, err := io.Copy(dst, src); err != nil {
		return "", "", invalidSubmission(fmt.Errorf("save upload failed: %w", err))
	}
	return dstPath, name, nil
}

type invalidSubmissionError struct {
	err error
}

func (e invalidSubmissionError) Error() string {
	return e.err.Error()
}

func (e invalidSubmissionError) Unwrap() error {
	return e.err
}

func (e invalidSubmissionError) Is(target error) bool {
	return target == ErrInvalidSubmission
}

func invalidSubmission(err error) error {
	return invalidSubmissionError{err: err}
}
