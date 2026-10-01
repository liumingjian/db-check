package web

import (
	"context"
	"errors"
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
		wake:     make(chan struct{}, 1),
	}, nil
}

func (l *reportLifecycle) taskDir(id string) string { return filepath.Join(l.tasksDir, id) }

// ResultZipPath is where a finished task's downloadable result ZIP lives.
func ResultZipPath(taskDir, id string) string {
	return filepath.Join(taskDir, fmt.Sprintf("reports-%s.zip", id))
}

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
	task.Items, err = stageUploads(form, filepath.Join(dir, "uploads"), zips)
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

// stageUploads saves every uploaded file under uploadsDir, named as
// loadTaskInputs reads them back, and returns the report items with what
// the server read from each ZIP.
func stageUploads(form *multipart.Form, uploadsDir string, zips []*multipart.FileHeader) ([]reports.Item, error) {
	awrs, wdrs := filesForKey(form, "awrs"), filesForKey(form, "wdrs")
	if len(awrs) != 0 && len(awrs) != len(zips) {
		return nil, apierr.Invalid("AWR 文件数量与 ZIP 不一致，请按序号上传 awr_<n>")
	}
	if len(wdrs) != 0 && len(wdrs) != len(zips) {
		return nil, apierr.Invalid("WDR 文件数量与 ZIP 不一致，请按序号上传 wdr_<n>")
	}
	if err := os.MkdirAll(uploadsDir, 0o755); err != nil {
		return nil, err
	}
	items := make([]reports.Item, 0, len(zips))
	for i, zipHeader := range zips {
		itemID := strconv.Itoa(i + 1)
		zipPath, name, err := saveUpload(uploadsDir, "zip", itemID, zipHeader)
		if err != nil {
			return nil, err
		}
		read, err := reports.ReadCollectorZip(zipPath)
		if err != nil {
			return nil, apierr.Invalid(name + "：" + err.Error())
		}
		awrFiles := filesForKey(form, "awr_"+itemID)
		if len(awrs) == len(zips) {
			awrFiles = awrs[i : i+1]
		}
		if len(awrFiles) > 1 {
			return nil, apierr.Invalid(name + "：每个 ZIP 只能搭配一个 Oracle AWR 文件")
		}
		for _, hdr := range awrFiles {
			if _, _, err := saveUpload(uploadsDir, "awr", itemID, hdr); err != nil {
				return nil, err
			}
		}
		wdrFiles := filesForKey(form, "wdr_"+itemID)
		if len(wdrs) == len(zips) {
			wdrFiles = wdrs[i : i+1]
		}
		for k, hdr := range wdrFiles {
			if _, _, err := saveUpload(uploadsDir, "wdr", wdrUploadID(itemID, k), hdr); err != nil {
				return nil, err
			}
		}
		items = append(items, reports.Item{
			Position: i + 1, FileName: name, DBType: read.DBType,
			CollectorVersion: read.CollectorVersion, Status: reports.StatusQueued,
		})
	}
	return items, nil
}

func wdrUploadID(itemID string, existing int) string {
	return fmt.Sprintf("%s-%d", itemID, existing+1)
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
		l.startLegacyRetention()
		l.startRetention(ctx)
		go l.work(ctx, legacy)
	})
}

// work runs the legacy backlog, then claims queued tasks from the store
// oldest first, sleeping until a submission wakes it when none is left.
func (l *reportLifecycle) work(ctx context.Context, legacy []queuedTask) {
	for _, q := range legacy {
		l.runLegacy(ctx, q)
	}
	for {
		id, ok, err := reports.ClaimNext(ctx, l.platform.DB)
		if err != nil {
			l.platform.Log.Printf("[ERROR] claiming a report task: %v", err)
		}
		if !ok {
			<-l.wake
			continue
		}
		l.run(ctx, id)
	}
}

// run generates a claimed task's unfinished items, then its result ZIP.
// Items finished in an earlier run keep their outcome. When saving state
// fails the task is left as it is, to resume at the next start.
func (l *reportLifecycle) run(ctx context.Context, id string) {
	task, err := reports.Get(ctx, l.platform.DB, id)
	if !l.saved(id, err) {
		return
	}
	dir := l.taskDir(id)
	inputs, err := loadTaskInputs(dir, uploadItems(task.Items))
	if err != nil {
		l.fail(ctx, id, fmt.Sprintf("读取上传文件失败：%v", err))
		return
	}
	results := make([]ItemResult, 0, len(task.Items))
	for i := range task.Items {
		item := &task.Items[i]
		if !item.Status.Finished() && !l.runItem(ctx, task, item, inputs[i]) {
			return
		}
		results = append(results, savedResult(dir, *item, inputs[i].ID))
	}
	if err := buildResultZip(ResultZipPath(dir, id), results, inputs); err != nil {
		l.fail(ctx, id, err.Error())
		return
	}
	if l.saved(id, reports.Finish(ctx, l.platform.DB, id, reports.StatusDone, "")) {
		l.hub.emitDone(id, "/api/reports/download/"+id)
	}
}

// runItem generates one item and saves its outcome into item; false when
// saving failed.
func (l *reportLifecycle) runItem(ctx context.Context, task reports.Task, item *reports.Item, input ItemInput) bool {
	db := l.platform.DB
	if !l.saved(task.ID, reports.SetItemStatus(ctx, db, task.ID, item.Position, reports.StatusProcessing, "")) {
		return false
	}
	l.hub.emitLog(task.ID, "info", "开始处理 "+input.Name)
	job := ItemJob{TaskID: task.ID, TaskDir: l.taskDir(task.ID), TaskCreatedAt: task.CreatedAt, Input: input}
	res := l.platform.Pipeline.RunItem(ctx, job, func(ev LogEvent) {
		level := "info"
		if ev.Stream == LogStderr {
			level = "error"
		}
		l.hub.emitLog(task.ID, level, fmt.Sprintf("[%s] %s", input.Name, ev.Line))
	})
	item.Status, item.Reason = reports.StatusDone, ""
	if res.Status != ItemDone {
		item.Status, item.Reason = reports.StatusFailed, res.Error
	}
	if !l.saved(task.ID, reports.SetItemStatus(ctx, db, task.ID, item.Position, item.Status, item.Reason)) {
		return false
	}
	if item.Status == reports.StatusFailed {
		l.hub.emitLog(task.ID, "error", fmt.Sprintf("[%s] 处理失败: %s", input.Name, item.Reason))
	}
	l.hub.emitProgress(task.ID, finishedItems(task.Items), len(task.Items), input.Name)
	return true
}

func (l *reportLifecycle) fail(ctx context.Context, id, reason string) {
	if l.saved(id, reports.Finish(ctx, l.platform.DB, id, reports.StatusFailed, reason)) {
		l.hub.emitError(id, reason)
	}
}

// saved logs a failed save; the task then stays as last saved.
func (l *reportLifecycle) saved(id string, err error) bool {
	if err == nil {
		return true
	}
	if errors.Is(err, reports.ErrNotFound) {
		l.platform.Log.Printf("[WARN] report task %s is gone; dropping it", id)
	} else {
		l.platform.Log.Printf("[ERROR] report task %s: saving state failed, it resumes at the next start: %v", id, err)
	}
	return false
}

// savedResult is an item's saved outcome as the result ZIP needs it.
func savedResult(taskDir string, item reports.Item, itemID string) ItemResult {
	if item.Status == reports.StatusDone {
		return ItemResult{ID: itemID, Status: ItemDone, ReportDocx: ItemReportPath(taskDir, itemID)}
	}
	return ItemResult{ID: itemID, Status: ItemFailed, Error: item.Reason}
}

func finishedItems(items []reports.Item) int {
	n := 0
	for _, it := range items {
		if it.Status.Finished() {
			n++
		}
	}
	return n
}

// currentItem names the item being generated, if any.
func currentItem(items []reports.Item) string {
	for _, it := range items {
		if it.Status == reports.StatusProcessing {
			return it.FileName
		}
	}
	return ""
}

// uploadItems names the items as loadTaskInputs expects them.
func uploadItems(items []reports.Item) []TaskItem {
	out := make([]TaskItem, 0, len(items))
	for _, it := range items {
		out = append(out, TaskItem{ID: strconv.Itoa(it.Position), Name: it.FileName})
	}
	return out
}
