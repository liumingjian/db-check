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
	ErrTaskCapacity      = errors.New("report task capacity exhausted")
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

// SubmissionRequest keeps request metadata available before an input body is read.
// Key is reserved for the keyed-submission flow and has no behavior in this release.
type SubmissionRequest struct {
	Key         string
	Materialize func(context.Context) (ReportSubmission, error)
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
	wake  chan struct{}

	newPipeline pipelineFactory

	mu                 sync.Mutex
	started            bool
	closed             bool
	reservations       int
	acceptedUnfinished int
	nextAcceptedOrder  uint64
	startOnce          sync.Once
	startErr           error
	closeOnce          sync.Once

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
	if cfg.MaxAcceptedTasks == 0 {
		cfg.MaxAcceptedTasks = defaultMaxAcceptedTasks
	}
	if cfg.MaxAcceptedTasks < 0 {
		return nil, errors.New("max accepted tasks must be greater than zero")
	}
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
		cfg:               cfg,
		store:             store,
		hub:               newTaskHub(cfg.LogReplayLines),
		wake:              make(chan struct{}, 1),
		newPipeline:       newPipeline,
		nextAcceptedOrder: 1,
		stop:              make(chan struct{}),
		workerDone:        make(chan struct{}),
		retentionDone:     make(chan struct{}),
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

func (l *TaskLifecycle) Submit(ctx context.Context, request SubmissionRequest) (task Task, err error) {
	if err := ctx.Err(); err != nil {
		return Task{}, err
	}
	if request.Materialize == nil {
		return Task{}, invalidSubmission(errors.New("missing submission materializer"))
	}
	if err := l.reserveSubmission(); err != nil {
		return Task{}, err
	}
	reserved := true
	stagingDir := ""
	published := false
	defer func() {
		if published {
			return
		}
		if stagingDir != "" {
			_ = os.RemoveAll(stagingDir)
		}
		if reserved {
			l.releaseReservation()
		}
	}()

	taskID, err := newTaskID()
	if err != nil {
		return Task{}, err
	}
	stagingDir, err = l.store.CreateStaging(taskID)
	if err != nil {
		return Task{}, err
	}

	submission, err := request.Materialize(ctx)
	if err != nil {
		return Task{}, err
	}
	if err := ctx.Err(); err != nil {
		return Task{}, err
	}
	taskItems, err := stageSubmission(ctx, stagingDir, submission)
	if err != nil {
		return Task{}, err
	}
	if err := ctx.Err(); err != nil {
		return Task{}, err
	}

	task = Task{
		ID:        taskID,
		Status:    TaskQueued,
		Total:     len(taskItems),
		Completed: 0,
		Items:     taskItems,
	}
	task, err = l.publishSubmission(ctx, stagingDir, task)
	if err != nil {
		return Task{}, err
	}
	reserved = false
	published = true
	return task, nil
}

func (l *TaskLifecycle) reserveSubmission() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return ErrLifecycleClosed
	}
	if l.acceptedUnfinished+l.reservations >= l.cfg.MaxAcceptedTasks {
		return ErrTaskCapacity
	}
	l.reservations++
	return nil
}

func (l *TaskLifecycle) releaseReservation() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.reservations > 0 {
		l.reservations--
	}
}

func (l *TaskLifecycle) publishSubmission(ctx context.Context, stagingDir string, task Task) (Task, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return Task{}, ErrLifecycleClosed
	}
	if err := ctx.Err(); err != nil {
		return Task{}, err
	}

	acceptedAt := l.store.now()
	task.AcceptedOrder = l.nextAcceptedOrder
	task.AcceptedAt = acceptedAt
	task.CreatedAt = acceptedAt
	task.UpdatedAt = acceptedAt
	if err := l.store.WriteStagedTask(stagingDir, task); err != nil {
		return Task{}, err
	}
	if err := ctx.Err(); err != nil {
		return Task{}, err
	}
	if err := l.store.Publish(stagingDir, task.ID); err != nil {
		return Task{}, err
	}

	l.nextAcceptedOrder++
	l.reservations--
	l.acceptedUnfinished++
	l.signalDispatcherLocked()
	return task, nil
}

func stageSubmission(ctx context.Context, stagingDir string, submission ReportSubmission) ([]TaskItem, error) {
	if len(submission.Items) == 0 {
		return nil, invalidSubmission(errors.New("missing zip files (field: zips)"))
	}
	uploadsDir := filepath.Join(stagingDir, "uploads")
	if err := os.MkdirAll(uploadsDir, 0o755); err != nil {
		return nil, fmt.Errorf("create uploads dir failed: %w", err)
	}

	items := make([]TaskItem, 0, len(submission.Items))
	for i, submitted := range submission.Items {
		itemID := fmt.Sprintf("%d", i+1)
		_, name, err := saveSubmissionFile(ctx, uploadsDir, "zip", itemID, submitted.Zip)
		if err != nil {
			return nil, err
		}
		if submitted.AWR != nil {
			if _, _, err := saveSubmissionFile(ctx, uploadsDir, "awr", itemID, *submitted.AWR); err != nil {
				return nil, err
			}
		}
		for wdrIndex, wdr := range submitted.WDRs {
			if _, _, err := saveSubmissionFile(ctx, uploadsDir, "wdr", wdrUploadID(itemID, wdrIndex), wdr); err != nil {
				return nil, err
			}
		}
		items = append(items, TaskItem{ID: itemID, Name: name, Status: string(TaskQueued)})
	}
	return items, nil
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

func (l *TaskLifecycle) workerLoop(pipeline *Pipeline) {
	defer l.finishWorker()
	for {
		task, found := l.nextQueuedTask()
		if found {
			if task.TaskID != "" {
				l.runTask(pipeline, task)
			}
			continue
		}

		select {
		case <-l.stop:
			return
		case <-l.wake:
		}
	}
}

func (l *TaskLifecycle) nextQueuedTask() (queuedTask, bool) {
	task, found, err := l.store.FindNextQueued()
	if err != nil || !found {
		return queuedTask{}, false
	}
	items, err := loadTaskInputs(l.store.taskDir(task.ID), task)
	if err != nil {
		task.Status = TaskFailed
		task.Error = fmt.Sprintf("queued task inputs unavailable: %v", err)
		task.CurrentFile = ""
		if _, updateErr := l.store.Update(task); updateErr == nil {
			l.releaseAcceptedTask()
			l.hub.emitError(task.ID, task.Error)
		}
		return queuedTask{}, true
	}
	return queuedTask{TaskID: task.ID, Items: items}, true
}

func (l *TaskLifecycle) signalDispatcher() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.signalDispatcherLocked()
}

func (l *TaskLifecycle) signalDispatcherLocked() {
	if l.closed {
		return
	}
	select {
	case l.wake <- struct{}{}:
	default:
	}
}

func (l *TaskLifecycle) releaseAcceptedTask() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.acceptedUnfinished > 0 {
		l.acceptedUnfinished--
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
		if _, updateErr := l.store.Update(task); updateErr != nil {
			return
		}
		l.releaseAcceptedTask()
		l.hub.emitError(task.ID, err.Error())
		return
	}

	task.Status = TaskDone
	task.CurrentFile = ""
	if _, err := l.store.Update(task); err != nil {
		return
	}
	l.releaseAcceptedTask()
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

func saveSubmissionFile(ctx context.Context, dir string, kind string, itemID string, file SubmissionFile) (string, string, error) {
	if err := ctx.Err(); err != nil {
		return "", "", err
	}
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
	if err := copySubmissionFile(ctx, dst, src); err != nil {
		_ = dst.Close()
		if ctxErr := ctx.Err(); ctxErr != nil {
			return "", "", ctxErr
		}
		return "", "", invalidSubmission(fmt.Errorf("save upload failed: %w", err))
	}
	if err := dst.Close(); err != nil {
		return "", "", invalidSubmission(fmt.Errorf("save upload failed: %w", err))
	}
	return dstPath, name, nil
}

func copySubmissionFile(ctx context.Context, dst io.Writer, src io.Reader) error {
	buf := make([]byte, 32*1024)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		n, readErr := src.Read(buf)
		if n > 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
			if _, err := dst.Write(buf[:n]); err != nil {
				return err
			}
		}
		if readErr == io.EOF {
			return nil
		}
		if readErr != nil {
			return readErr
		}
	}
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
