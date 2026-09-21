package web

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestStorageFaultDuringPublicationRejectsAdmissionAndKeepsSnapshotsReadable(t *testing.T) {
	cfg := Config{
		DataDir:          t.TempDir(),
		APIToken:         defaultAPIToken,
		MaxAcceptedTasks: 1,
		RetentionTTL:     0,
	}
	lifecycle := newStorageFaultLifecycle(t, cfg, controlledLifecyclePipeline)
	if _, err := lifecycle.store.Create(Task{ID: "persisted", Status: TaskFailed, Error: "existing failure"}); err != nil {
		t.Fatalf("create persisted task: %v", err)
	}
	if err := lifecycle.Start(context.Background()); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	closeStorageFaultLifecycle(t, lifecycle)

	watch, err := lifecycle.Watch(context.Background(), "persisted")
	if err != nil {
		t.Fatalf("Watch failed: %v", err)
	}
	defer watch.Close()
	if _, err := lifecycle.Submit(context.Background(), materializedSubmission(testSubmission("not-a-zip.txt", "invalid"))); !errors.Is(err, ErrInvalidSubmission) {
		t.Fatalf("malformed submission error=%v, want ErrInvalidSubmission", err)
	}
	if lifecycle.hasStorageFault() {
		t.Fatal("malformed submission paused the lifecycle")
	}

	lifecycle.store.writeJSON = func(string, any) error {
		return errors.New("disk full")
	}
	if _, err := lifecycle.Submit(context.Background(), materializedSubmission(testSubmission("valid.zip", "content"))); !errors.Is(err, ErrStorageUnavailable) {
		t.Fatalf("publication failure error=%v, want ErrStorageUnavailable", err)
	}

	select {
	case <-watch.Overflow:
	case <-time.After(time.Second):
		t.Fatal("storage fault did not disconnect active watcher")
	}

	snapshot, err := lifecycle.Read(context.Background(), "persisted")
	if err != nil {
		t.Fatalf("Read failed after storage fault: %v", err)
	}
	if snapshot.Status != TaskFailed || snapshot.Error != "existing failure" {
		t.Fatalf("storage fault changed persisted task state: %#v", snapshot)
	}
	if snapshot.StorageFault == nil || snapshot.StorageFault.Code != storageFaultCode || snapshot.StorageFault.Message != storageFaultMessage {
		t.Fatalf("missing storage fault in snapshot: %#v", snapshot)
	}

	materialized := false
	if _, err := lifecycle.Submit(context.Background(), SubmissionRequest{
		Materialize: func(context.Context) (ReportSubmission, error) {
			materialized = true
			return testSubmission("later.zip", "content"), nil
		},
	}); !errors.Is(err, ErrStorageUnavailable) {
		t.Fatalf("admission after storage fault error=%v, want ErrStorageUnavailable", err)
	}
	if materialized {
		t.Fatal("admission after storage fault materialized a submission")
	}

	handler := newAPIHandlerWithLifecycle(cfg, lifecycle).handler()
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, multipartRequest(t, map[string]string{"zips": "later.zip"}))
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("storage fault generate status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	var response struct {
		Code  string `json:"code"`
		Error string `json:"error"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode storage fault response: %v", err)
	}
	if response.Code != storageFaultCode || response.Error != storageFaultMessage {
		t.Fatalf("unexpected storage fault response: %#v", response)
	}
}

func TestStorageFaultBeforeItemStartDoesNotRunPipeline(t *testing.T) {
	starts := make(chan string, 1)
	cfg := Config{DataDir: t.TempDir(), MaxAcceptedTasks: 1, RetentionTTL: 0}
	lifecycle, err := newTaskLifecycle(cfg, func() (*Pipeline, error) {
		pipeline := controlledLifecyclePipeline()
		pipeline.ExtractZip = func(zipPath, _ string) error {
			starts <- strings.TrimPrefix(filepath.Base(zipPath), "zip-1-")
			return nil
		}
		return pipeline, nil
	})
	if err != nil {
		t.Fatalf("newTaskLifecycle failed: %v", err)
	}
	closeStorageFaultLifecycle(t, lifecycle)

	task := queuedStorageFaultTask("queued", "first.zip", "second.zip")
	createRecoveryTask(t, lifecycle.store, task)
	writeRecoveryUpload(t, lifecycle.store, task.ID, "1", "first.zip")
	writeRecoveryUpload(t, lifecycle.store, task.ID, "2", "second.zip")

	watch, err := lifecycle.Watch(context.Background(), task.ID)
	if err != nil {
		t.Fatalf("Watch before Start failed: %v", err)
	}
	defer watch.Close()
	lifecycle.store.writeJSON = func(string, any) error {
		return errors.New("disk full")
	}
	if err := lifecycle.Start(context.Background()); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	waitForStorageFault(t, lifecycle)
	select {
	case name := <-starts:
		t.Fatalf("pipeline started %q after processing state failed to persist", name)
	case <-time.After(150 * time.Millisecond):
	}
	select {
	case <-watch.Overflow:
	case <-time.After(time.Second):
		t.Fatal("storage fault did not disconnect watcher")
	}

	persisted, err := lifecycle.store.Load(task.ID)
	if err != nil {
		t.Fatalf("load persisted task: %v", err)
	}
	if persisted.Status != TaskQueued || persisted.Completed != 0 || persisted.CurrentFile != "" {
		t.Fatalf("failed processing transition changed persisted task: %#v", persisted)
	}
}

func TestStorageFaultAfterItemStopsFurtherWorkAndWithholdsProgress(t *testing.T) {
	starts := make(chan string, 2)
	releaseFirst := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(releaseFirst) })
	})
	cfg := Config{DataDir: t.TempDir(), MaxAcceptedTasks: 1, RetentionTTL: 0}
	lifecycle, err := newTaskLifecycle(cfg, func() (*Pipeline, error) {
		pipeline := controlledLifecyclePipeline()
		pipeline.ExtractZip = func(zipPath, _ string) error {
			name := strings.TrimPrefix(filepath.Base(zipPath), "zip-1-")
			starts <- name
			if name == "first.zip" {
				<-releaseFirst
			}
			return nil
		}
		return pipeline, nil
	})
	if err != nil {
		t.Fatalf("newTaskLifecycle failed: %v", err)
	}
	closeStorageFaultLifecycle(t, lifecycle)

	task := queuedStorageFaultTask("two-items", "first.zip", "second.zip")
	createRecoveryTask(t, lifecycle.store, task)
	writeRecoveryUpload(t, lifecycle.store, task.ID, "1", "first.zip")
	writeRecoveryUpload(t, lifecycle.store, task.ID, "2", "second.zip")

	watch, err := lifecycle.Watch(context.Background(), task.ID)
	if err != nil {
		t.Fatalf("Watch before Start failed: %v", err)
	}
	defer watch.Close()

	var failWrites atomic.Bool
	writeJSON := lifecycle.store.writeJSON
	lifecycle.store.writeJSON = func(path string, value any) error {
		if failWrites.Load() {
			return errors.New("disk full")
		}
		return writeJSON(path, value)
	}
	if err := lifecycle.Start(context.Background()); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	if got := waitForStart(t, starts); got != "first.zip" {
		t.Fatalf("first item started as %q", got)
	}

	failWrites.Store(true)
	releaseOnce.Do(func() { close(releaseFirst) })
	waitForStorageFault(t, lifecycle)

	select {
	case name := <-starts:
		t.Fatalf("started another item after storage fault: %q", name)
	case <-time.After(150 * time.Millisecond):
	}
	assertNoTerminalOrProgressUpdate(t, watch.Updates)

	persisted, err := lifecycle.store.Load(task.ID)
	if err != nil {
		t.Fatalf("load persisted task: %v", err)
	}
	if persisted.Status != TaskProcessing || persisted.Completed != 0 || persisted.CurrentFile != "first.zip" {
		t.Fatalf("failed item result changed persisted task: %#v", persisted)
	}
	for _, item := range persisted.Items {
		if item.Status != string(TaskQueued) {
			t.Fatalf("failed item result persisted item state: %#v", persisted.Items)
		}
	}
}

func TestStorageFaultDuringTerminalSaveWithholdsDoneNotification(t *testing.T) {
	cfg := Config{DataDir: t.TempDir(), MaxAcceptedTasks: 1, RetentionTTL: 0}
	lifecycle := newStorageFaultLifecycle(t, cfg, controlledLifecyclePipeline)
	task := queuedStorageFaultTask("terminal", "first.zip")
	createRecoveryTask(t, lifecycle.store, task)
	writeRecoveryUpload(t, lifecycle.store, task.ID, "1", "first.zip")

	watch, err := lifecycle.Watch(context.Background(), task.ID)
	if err != nil {
		t.Fatalf("Watch before Start failed: %v", err)
	}
	defer watch.Close()

	writeJSON := lifecycle.store.writeJSON
	lifecycle.store.writeJSON = func(path string, value any) error {
		if updated, ok := value.(Task); ok && updated.ID == task.ID && updated.Status == TaskDone {
			return errors.New("disk full")
		}
		return writeJSON(path, value)
	}
	if err := lifecycle.Start(context.Background()); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	waitForStorageFault(t, lifecycle)
	assertNoDoneUpdate(t, watch.Updates)

	persisted, err := lifecycle.store.Load(task.ID)
	if err != nil {
		t.Fatalf("load persisted task: %v", err)
	}
	if persisted.Status != TaskProcessing || persisted.Completed != 1 || persisted.Items[0].Status != string(ItemDone) {
		t.Fatalf("terminal save failure changed persisted task outcome: %#v", persisted)
	}
	if _, _, err := lifecycle.Download(context.Background(), task.ID); !errors.Is(err, ErrTaskNotFinished) {
		t.Fatalf("terminal save failure exposed download as complete: %v", err)
	}
}

func TestStorageFaultDuringRetentionRequiresRestart(t *testing.T) {
	dataDir := t.TempDir()
	cfg := Config{DataDir: dataDir, MaxAcceptedTasks: 1, RetentionTTL: time.Hour}
	lifecycle := newStorageFaultLifecycle(t, cfg, controlledLifecyclePipeline)
	if _, err := lifecycle.store.Create(Task{ID: "expired", Status: TaskFailed}); err != nil {
		t.Fatalf("create expired task: %v", err)
	}
	if err := lifecycle.Start(context.Background()); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	closeStorageFaultLifecycle(t, lifecycle)

	removeAll := lifecycle.store.removeAll
	lifecycle.store.removeAll = func(string) error {
		return errors.New("disk full")
	}
	now := time.Now().Add(2 * time.Hour)
	if _, err := lifecycle.cleanupExpiredRetainedTasks(func() time.Time { return now }); !errors.Is(err, ErrStorageUnavailable) {
		t.Fatalf("retention failure error=%v, want ErrStorageUnavailable", err)
	}
	if _, err := lifecycle.store.Load("expired"); err != nil {
		t.Fatalf("retention failure removed persisted task: %v", err)
	}

	lifecycle.store.removeAll = removeAll
	if _, err := lifecycle.cleanupExpiredRetainedTasks(func() time.Time { return now }); !errors.Is(err, ErrStorageUnavailable) {
		t.Fatalf("retention retried while paused: %v", err)
	}
	if _, err := lifecycle.store.Load("expired"); err != nil {
		t.Fatalf("paused retention removed persisted task: %v", err)
	}
	closeStorageFaultLifecycleNow(t, lifecycle)

	restarted := newStartedStorageFaultLifecycle(t, cfg, controlledLifecyclePipeline)
	if deleted, err := restarted.cleanupExpiredRetainedTasks(func() time.Time { return now }); err != nil || deleted != 1 {
		t.Fatalf("retention after restart deleted=%d err=%v, want 1 and nil", deleted, err)
	}
	if _, err := restarted.store.Load("expired"); !errors.Is(err, ErrTaskNotFound) {
		t.Fatalf("restart retention left expired task: %v", err)
	}
}

func newStorageFaultLifecycle(t *testing.T, cfg Config, factory func() *Pipeline) *TaskLifecycle {
	t.Helper()
	lifecycle, err := newTaskLifecycle(cfg, func() (*Pipeline, error) {
		return factory(), nil
	})
	if err != nil {
		t.Fatalf("newTaskLifecycle failed: %v", err)
	}
	return lifecycle
}

func newStartedStorageFaultLifecycle(t *testing.T, cfg Config, factory func() *Pipeline) *TaskLifecycle {
	t.Helper()
	lifecycle := newStorageFaultLifecycle(t, cfg, factory)
	if err := lifecycle.Start(context.Background()); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	closeStorageFaultLifecycle(t, lifecycle)
	return lifecycle
}

func closeStorageFaultLifecycle(t *testing.T, lifecycle *TaskLifecycle) {
	t.Helper()
	t.Cleanup(func() {
		closeStorageFaultLifecycleNow(t, lifecycle)
	})
}

func closeStorageFaultLifecycleNow(t *testing.T, lifecycle *TaskLifecycle) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := lifecycle.Close(ctx); err != nil {
		t.Errorf("Close failed: %v", err)
	}
}

func queuedStorageFaultTask(id string, names ...string) Task {
	items := make([]TaskItem, 0, len(names))
	for index, name := range names {
		items = append(items, TaskItem{
			ID:     string(rune('1' + index)),
			Name:   name,
			Status: string(TaskQueued),
		})
	}
	return Task{
		ID:            id,
		Status:        TaskQueued,
		AcceptedOrder: 1,
		AcceptedAt:    time.Date(2026, 9, 21, 12, 0, 0, 0, time.UTC),
		Total:         len(items),
		Items:         items,
	}
}

func waitForStorageFault(t *testing.T, lifecycle *TaskLifecycle) {
	t.Helper()
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		if lifecycle.hasStorageFault() {
			return
		}
		select {
		case <-ticker.C:
		case <-deadline.C:
			t.Fatal("timed out waiting for storage fault")
		}
	}
}

func assertNoTerminalOrProgressUpdate(t *testing.T, updates <-chan []byte) {
	t.Helper()
	for update := range updates {
		var message struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal(update, &message); err != nil {
			t.Fatalf("decode watcher update: %v", err)
		}
		switch message.Type {
		case "progress", "done", "error":
			t.Fatalf("received %q update after its task state was not persisted", message.Type)
		}
	}
}

func assertNoDoneUpdate(t *testing.T, updates <-chan []byte) {
	t.Helper()
	for update := range updates {
		var message struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal(update, &message); err != nil {
			t.Fatalf("decode watcher update: %v", err)
		}
		if message.Type == "done" {
			t.Fatal("received done update before the terminal task state persisted")
		}
	}
}
