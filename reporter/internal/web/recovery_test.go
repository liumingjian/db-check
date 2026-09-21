package web

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestTaskLifecycleStartRejectsSecondWriterBeforeRecovery(t *testing.T) {
	dataDir := t.TempDir()
	first := newRecoveryTestLifecycle(t, dataDir, 1, controlledRecoveryPipeline)
	if err := first.Start(context.Background()); err != nil {
		t.Fatalf("start first lifecycle: %v", err)
	}

	abandoned := filepath.Join(first.store.stagingDir(), "abandoned")
	if err := os.MkdirAll(abandoned, 0o755); err != nil {
		t.Fatalf("create abandoned staging: %v", err)
	}
	if err := os.WriteFile(filepath.Join(abandoned, "partial"), []byte("partial"), 0o644); err != nil {
		t.Fatalf("write abandoned staging: %v", err)
	}

	second := newRecoveryTestLifecycle(t, dataDir, 1, controlledRecoveryPipeline)
	err := second.Start(context.Background())
	if !errors.Is(err, ErrDataDirInUse) {
		t.Fatalf("second writer error=%v, want ErrDataDirInUse", err)
	}
	if _, err := os.Stat(abandoned); err != nil {
		t.Fatalf("second writer cleaned staging before rejecting startup: %v", err)
	}
	closeRecoveryTestLifecycle(t, second)
	closeRecoveryTestLifecycle(t, first)

	third := newRecoveryTestLifecycle(t, dataDir, 1, controlledRecoveryPipeline)
	if err := third.Start(context.Background()); err != nil {
		t.Fatalf("start lifecycle after writer release: %v", err)
	}
	defer closeRecoveryTestLifecycle(t, third)
	if _, err := os.Stat(abandoned); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stale staging remains after recovery: %v", err)
	}
}

func TestTaskLifecycleRecoveryRebuildsOversizedFIFOBacklog(t *testing.T) {
	const taskCount = 35
	dataDir := t.TempDir()
	starts := make(chan string, taskCount+1)
	releases := make(chan struct{})

	lifecycle := newRecoveryTestLifecycle(t, dataDir, 1, func() (*Pipeline, error) {
		pipeline := controlledLifecyclePipeline()
		pipeline.ExtractZip = func(zipPath, _ string) error {
			starts <- strings.TrimPrefix(filepath.Base(zipPath), "zip-1-")
			<-releases
			return nil
		}
		return pipeline, nil
	})
	defer closeRecoveryTestLifecycle(t, lifecycle)
	defer close(releases)

	acceptedAt := time.Date(2026, 9, 21, 12, 0, 0, 0, time.UTC)
	for i := 1; i <= taskCount; i++ {
		name := fmt.Sprintf("task-%02d.zip", i)
		task := Task{
			ID:            fmt.Sprintf("queued-%02d", i),
			Status:        TaskQueued,
			AcceptedOrder: uint64(i),
			AcceptedAt:    acceptedAt.Add(time.Duration(i) * time.Second),
			Total:         1,
			Items: []TaskItem{{
				ID:     "1",
				Name:   name,
				Status: string(TaskQueued),
			}},
		}
		createRecoveryTask(t, lifecycle.store, task)
		writeRecoveryUpload(t, lifecycle.store, task.ID, "1", name)
	}

	if err := lifecycle.Start(context.Background()); err != nil {
		t.Fatalf("start lifecycle: %v", err)
	}
	if lifecycle.acceptedUnfinished != taskCount {
		t.Fatalf("recovered unfinished=%d, want %d", lifecycle.acceptedUnfinished, taskCount)
	}
	if _, err := lifecycle.Submit(context.Background(), materializedSubmission(testSubmission("new.zip", "new"))); !errors.Is(err, ErrTaskCapacity) {
		t.Fatalf("submission while recovered backlog is over capacity error=%v, want ErrTaskCapacity", err)
	}

	for i := 1; i <= taskCount; i++ {
		want := fmt.Sprintf("task-%02d.zip", i)
		if got := waitForStart(t, starts); got != want {
			t.Fatalf("recovered task %d started as %q, want %q", i, got, want)
		}
		releases <- struct{}{}
	}
	lastID := fmt.Sprintf("queued-%02d", taskCount)
	waitForTaskStatus(t, lifecycle, lastID, TaskDone)

	next, err := lifecycle.Submit(context.Background(), materializedSubmission(testSubmission("new.zip", "new")))
	if err != nil {
		t.Fatalf("submit after recovered backlog completed: %v", err)
	}
	if next.AcceptedOrder != taskCount+1 {
		t.Fatalf("new accepted order=%d, want %d", next.AcceptedOrder, taskCount+1)
	}
	if got := waitForStart(t, starts); got != "new.zip" {
		t.Fatalf("new task started as %q, want new.zip", got)
	}
	releases <- struct{}{}
	waitForTaskStatus(t, lifecycle, next.ID, TaskDone)
}

func TestTaskLifecycleRecoveryPreservesTerminalItemsAndRerunsOnlyUnfinished(t *testing.T) {
	dataDir := t.TempDir()
	starts := make(chan string, 3)
	lifecycle := newRecoveryTestLifecycle(t, dataDir, 3, func() (*Pipeline, error) {
		pipeline := controlledLifecyclePipeline()
		pipeline.ExtractZip = func(zipPath, _ string) error {
			starts <- strings.TrimPrefix(filepath.Base(zipPath), "zip-")
			return nil
		}
		return pipeline, nil
	})
	defer closeRecoveryTestLifecycle(t, lifecycle)

	savedReport := filepath.Join(lifecycle.store.taskDir("resume"), "items", "1", "attempts", "saved", "report.docx")
	task := Task{
		ID:            "resume",
		Status:        TaskProcessing,
		AcceptedOrder: 1,
		AcceptedAt:    time.Date(2026, 9, 21, 12, 0, 0, 0, time.UTC),
		Total:         3,
		Completed:     2,
		Items: []TaskItem{
			{ID: "1", Name: "done.zip", Status: string(ItemDone), ReportDocx: savedReport},
			{ID: "2", Name: "failed.zip", Status: string(ItemFailed), Error: "original item failure"},
			{ID: "3", Name: "retry.zip", Status: string(TaskQueued)},
		},
	}
	createRecoveryTask(t, lifecycle.store, task)
	for _, item := range task.Items {
		writeRecoveryUpload(t, lifecycle.store, task.ID, item.ID, item.Name)
	}
	if err := os.MkdirAll(filepath.Dir(savedReport), 0o755); err != nil {
		t.Fatalf("create saved report dir: %v", err)
	}
	if err := writeTestReportDocx(savedReport); err != nil {
		t.Fatalf("write saved report: %v", err)
	}

	if err := lifecycle.Start(context.Background()); err != nil {
		t.Fatalf("start lifecycle: %v", err)
	}
	waitForTaskStatus(t, lifecycle, task.ID, TaskDone)
	if got := waitForStart(t, starts); got != "3-retry.zip" {
		t.Fatalf("only unfinished item should rerun; got %q", got)
	}
	select {
	case got := <-starts:
		t.Fatalf("unexpected rerun of preserved item: %q", got)
	case <-time.After(100 * time.Millisecond):
	}

	recovered, err := lifecycle.Get(context.Background(), task.ID)
	if err != nil {
		t.Fatalf("get recovered task: %v", err)
	}
	if recovered.Items[0].Status != string(ItemDone) || recovered.Items[0].ReportDocx != savedReport {
		t.Fatalf("completed item was not preserved: %#v", recovered.Items[0])
	}
	if recovered.Items[1].Status != string(ItemFailed) || recovered.Items[1].Error != "original item failure" {
		t.Fatalf("failed item was not preserved: %#v", recovered.Items[1])
	}
	if recovered.Items[2].Status != string(ItemDone) {
		t.Fatalf("unfinished item was not completed: %#v", recovered.Items[2])
	}
}

func TestTaskLifecycleRecoveryFaultsAreTruthful(t *testing.T) {
	t.Run("corrupt metadata blocks startup", func(t *testing.T) {
		pipelineCreated := false
		lifecycle := newRecoveryTestLifecycle(t, t.TempDir(), 1, func() (*Pipeline, error) {
			pipelineCreated = true
			return controlledLifecyclePipeline(), nil
		})
		defer closeRecoveryTestLifecycle(t, lifecycle)
		if err := os.MkdirAll(lifecycle.store.taskDir("corrupt"), 0o755); err != nil {
			t.Fatalf("create corrupt task dir: %v", err)
		}
		if err := os.WriteFile(lifecycle.store.taskPath("corrupt"), []byte("{"), 0o644); err != nil {
			t.Fatalf("write corrupt task metadata: %v", err)
		}

		err := lifecycle.Start(context.Background())
		if err == nil || !strings.Contains(err.Error(), "decode task") {
			t.Fatalf("startup error=%v, want corrupt metadata fault", err)
		}
		if pipelineCreated {
			t.Fatal("worker pipeline was created after corrupt metadata")
		}
	})

	t.Run("missing required input persists task failure", func(t *testing.T) {
		lifecycle := newRecoveryTestLifecycle(t, t.TempDir(), 1, controlledRecoveryPipeline)
		defer closeRecoveryTestLifecycle(t, lifecycle)
		createRecoveryTask(t, lifecycle.store, Task{
			ID:     "missing-input",
			Status: TaskQueued,
			Items:  []TaskItem{{ID: "1", Name: "missing.zip", Status: string(TaskQueued)}},
		})

		if err := lifecycle.Start(context.Background()); err != nil {
			t.Fatalf("start lifecycle: %v", err)
		}
		task, err := lifecycle.Get(context.Background(), "missing-input")
		if err != nil {
			t.Fatalf("get recovered task: %v", err)
		}
		if task.Status != TaskFailed || !strings.Contains(task.Error, "recovery failed") {
			t.Fatalf("missing input result=%#v", task)
		}
	})

	t.Run("missing completed report persists task failure without rerun", func(t *testing.T) {
		starts := make(chan string, 1)
		lifecycle := newRecoveryTestLifecycle(t, t.TempDir(), 1, func() (*Pipeline, error) {
			pipeline := controlledLifecyclePipeline()
			pipeline.ExtractZip = func(zipPath, _ string) error {
				starts <- zipPath
				return nil
			}
			return pipeline, nil
		})
		defer closeRecoveryTestLifecycle(t, lifecycle)
		task := Task{
			ID:     "missing-report",
			Status: TaskProcessing,
			Items: []TaskItem{{
				ID:         "1",
				Name:       "done.zip",
				Status:     string(ItemDone),
				ReportDocx: filepath.Join(lifecycle.store.taskDir("missing-report"), "items", "1", "attempts", "lost", "report.docx"),
			}},
		}
		createRecoveryTask(t, lifecycle.store, task)
		writeRecoveryUpload(t, lifecycle.store, task.ID, "1", "done.zip")

		if err := lifecycle.Start(context.Background()); err != nil {
			t.Fatalf("start lifecycle: %v", err)
		}
		recovered, err := lifecycle.Get(context.Background(), task.ID)
		if err != nil {
			t.Fatalf("get recovered task: %v", err)
		}
		if recovered.Status != TaskFailed || !strings.Contains(recovered.Error, "completed report") {
			t.Fatalf("missing completed report result=%#v", recovered)
		}
		if recovered.Items[0].Status != string(ItemDone) {
			t.Fatalf("completed item outcome changed: %#v", recovered.Items[0])
		}
		select {
		case got := <-starts:
			t.Fatalf("completed item was rerun: %q", got)
		case <-time.After(100 * time.Millisecond):
		}
	})

	t.Run("failed recovery save keeps startup unavailable", func(t *testing.T) {
		lifecycle := newRecoveryTestLifecycle(t, t.TempDir(), 1, controlledRecoveryPipeline)
		defer closeRecoveryTestLifecycle(t, lifecycle)
		createRecoveryTask(t, lifecycle.store, Task{
			ID:     "save-failure",
			Status: TaskQueued,
			Items:  []TaskItem{{ID: "1", Name: "missing.zip", Status: string(TaskQueued)}},
		})
		lifecycle.store.writeJSON = func(string, any) error {
			return errors.New("disk full")
		}

		err := lifecycle.Start(context.Background())
		if err == nil || !strings.Contains(err.Error(), "persist recovery failure") {
			t.Fatalf("startup error=%v, want failed recovery persistence", err)
		}
		task, err := lifecycle.Get(context.Background(), "save-failure")
		if err != nil {
			t.Fatalf("get unsaved task: %v", err)
		}
		if task.Status != TaskQueued {
			t.Fatalf("task reported an unsaved failure: %#v", task)
		}
	})
}

func TestTaskLifecycleCloseLeavesUnfinishedTaskForRecovery(t *testing.T) {
	dataDir := t.TempDir()
	firstStart := make(chan string, 1)
	releaseFirst := make(chan struct{})
	var releaseOnce sync.Once
	release := func() {
		releaseOnce.Do(func() {
			close(releaseFirst)
		})
	}

	first := newRecoveryTestLifecycle(t, dataDir, 2, func() (*Pipeline, error) {
		pipeline := controlledLifecyclePipeline()
		pipeline.ExtractZip = func(zipPath, _ string) error {
			firstStart <- strings.TrimPrefix(filepath.Base(zipPath), "zip-1-")
			<-releaseFirst
			return nil
		}
		return pipeline, nil
	})
	if err := first.Start(context.Background()); err != nil {
		t.Fatalf("start first lifecycle: %v", err)
	}
	defer closeRecoveryTestLifecycle(t, first)
	defer release()
	task, err := first.Submit(context.Background(), materializedSubmission(twoItemSubmission("first.zip", "second.zip")))
	if err != nil {
		t.Fatalf("submit task: %v", err)
	}
	if got := waitForStart(t, firstStart); got != "first.zip" {
		t.Fatalf("first item started as %q", got)
	}

	closed := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		closed <- first.Close(ctx)
	}()
	waitForLifecycleStop(t, first)
	release()
	if err := <-closed; err != nil {
		t.Fatalf("close first lifecycle: %v", err)
	}

	persisted, err := first.Get(context.Background(), task.ID)
	if err != nil {
		t.Fatalf("get stopped task: %v", err)
	}
	if persisted.Status != TaskProcessing || persisted.Completed != 1 {
		t.Fatalf("shutdown did not leave recoverable progress: %#v", persisted)
	}

	resumedStarts := make(chan string, 2)
	second := newRecoveryTestLifecycle(t, dataDir, 2, func() (*Pipeline, error) {
		pipeline := controlledLifecyclePipeline()
		pipeline.ExtractZip = func(zipPath, _ string) error {
			_, name, ok := parseUploadName(filepath.Base(zipPath), "zip-")
			if !ok {
				return fmt.Errorf("unexpected staged upload path %q", zipPath)
			}
			resumedStarts <- name
			return nil
		}
		return pipeline, nil
	})
	defer closeRecoveryTestLifecycle(t, second)
	if err := second.Start(context.Background()); err != nil {
		t.Fatalf("start recovery lifecycle: %v", err)
	}
	waitForTaskStatus(t, second, task.ID, TaskDone)
	if got := waitForStart(t, resumedStarts); got != "second.zip" {
		t.Fatalf("restart reran %q, want only second.zip", got)
	}
	select {
	case got := <-resumedStarts:
		t.Fatalf("restart ran an extra item: %q", got)
	case <-time.After(100 * time.Millisecond):
	}
}

func newRecoveryTestLifecycle(t *testing.T, dataDir string, maxAccepted int, factory pipelineFactory) *TaskLifecycle {
	t.Helper()
	lifecycle, err := newTaskLifecycle(Config{
		DataDir:          dataDir,
		MaxAcceptedTasks: maxAccepted,
		RetentionTTL:     0,
	}, factory)
	if err != nil {
		t.Fatalf("new task lifecycle: %v", err)
	}
	return lifecycle
}

func controlledRecoveryPipeline() (*Pipeline, error) {
	return controlledLifecyclePipeline(), nil
}

func closeRecoveryTestLifecycle(t *testing.T, lifecycle *TaskLifecycle) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := lifecycle.Close(ctx); err != nil {
		t.Errorf("close task lifecycle: %v", err)
	}
}

func createRecoveryTask(t *testing.T, store *TaskStore, task Task) {
	t.Helper()
	if _, err := store.Create(task); err != nil {
		t.Fatalf("create task %q: %v", task.ID, err)
	}
}

func writeRecoveryUpload(t *testing.T, store *TaskStore, taskID, itemID, name string) {
	t.Helper()
	dir := filepath.Join(store.taskDir(taskID), "uploads")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("create uploads dir: %v", err)
	}
	path := filepath.Join(dir, fmt.Sprintf("zip-%s-%s", itemID, name))
	if err := os.WriteFile(path, []byte("input"), 0o644); err != nil {
		t.Fatalf("write upload %q: %v", path, err)
	}
}

func twoItemSubmission(names ...string) ReportSubmission {
	items := make([]ReportItemSubmission, 0, len(names))
	for _, name := range names {
		items = append(items, testSubmission(name, "input").Items[0])
	}
	return ReportSubmission{Items: items}
}

func waitForLifecycleStop(t *testing.T, lifecycle *TaskLifecycle) {
	t.Helper()
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		if lifecycle.stopping() {
			return
		}
		select {
		case <-ticker.C:
		case <-deadline.C:
			t.Fatal("lifecycle did not begin shutdown")
		}
	}
}
