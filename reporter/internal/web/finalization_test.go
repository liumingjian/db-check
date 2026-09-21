package web

import (
	"archive/zip"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestTaskLifecycleFinalizesMixedResultsWithValidatedDownload(t *testing.T) {
	lifecycle := newFinalizationLifecycle(t, func() (*Pipeline, error) {
		pipeline := controlledLifecyclePipeline()
		pipeline.ExtractZip = func(zipPath string, _ string) error {
			if strings.Contains(filepath.Base(zipPath), "failed.zip") {
				return errors.New("injected item failure")
			}
			return nil
		}
		return pipeline, nil
	})

	task, err := lifecycle.Submit(context.Background(), materializedSubmission(twoItemSubmission("succeeded.zip", "failed.zip")))
	if err != nil {
		t.Fatalf("submit task: %v", err)
	}
	waitForTaskStatus(t, lifecycle, task.ID, TaskDone)

	snapshot, err := lifecycle.Read(context.Background(), task.ID)
	if err != nil {
		t.Fatalf("read task snapshot: %v", err)
	}
	if snapshot.SucceededCount != 1 || snapshot.FailedCount != 1 || snapshot.DownloadURL == "" {
		t.Fatalf("unexpected mixed task snapshot: %#v", snapshot)
	}
	if len(snapshot.Items) != 2 || snapshot.Items[0].Status != string(ItemDone) || snapshot.Items[1].Status != string(ItemFailed) || !strings.Contains(snapshot.Items[1].Error, "injected item failure") {
		t.Fatalf("mixed item outcomes were not preserved: %#v", snapshot.Items)
	}

	path, _, err := lifecycle.Download(context.Background(), task.ID)
	if err != nil {
		t.Fatalf("download mixed task: %v", err)
	}
	archive, err := zip.OpenReader(path)
	if err != nil {
		t.Fatalf("mixed task result is not a ZIP: %v", err)
	}
	defer archive.Close()
	if len(archive.File) != 1 || archive.File[0].Name != "succeeded.zip/report.docx" {
		t.Fatalf("unexpected mixed task ZIP contents: %#v", archive.File)
	}
}

func TestTaskLifecycleFailsWhenEveryItemFails(t *testing.T) {
	lifecycle := newFinalizationLifecycle(t, func() (*Pipeline, error) {
		pipeline := controlledLifecyclePipeline()
		pipeline.ExtractZip = func(string, string) error {
			return errors.New("injected item failure")
		}
		return pipeline, nil
	})

	task, err := lifecycle.Submit(context.Background(), materializedSubmission(twoItemSubmission("first.zip", "second.zip")))
	if err != nil {
		t.Fatalf("submit task: %v", err)
	}
	waitForTaskStatus(t, lifecycle, task.ID, TaskFailed)

	snapshot, err := lifecycle.Read(context.Background(), task.ID)
	if err != nil {
		t.Fatalf("read task snapshot: %v", err)
	}
	if snapshot.SucceededCount != 0 || snapshot.FailedCount != 2 || snapshot.DownloadURL != "" || !strings.Contains(snapshot.Error, "no successful reports") {
		t.Fatalf("unexpected all-failed task snapshot: %#v", snapshot)
	}
	if len(snapshot.Items) != 2 || snapshot.Items[0].Status != string(ItemFailed) || snapshot.Items[1].Status != string(ItemFailed) {
		t.Fatalf("all-failed item outcomes were not preserved: %#v", snapshot.Items)
	}
	if _, _, err := lifecycle.Download(context.Background(), task.ID); !errors.Is(err, ErrTaskNotFinished) {
		t.Fatalf("all-failed task exposed a download: %v", err)
	}
}

func TestTaskLifecycleFailsForMissingCompletedArtifactWithoutRerun(t *testing.T) {
	var runs atomic.Int32
	lifecycle := newFinalizationLifecycle(t, func() (*Pipeline, error) {
		pipeline := controlledLifecyclePipeline()
		pipeline.Runner = noOutputRunner{runs: &runs}
		return pipeline, nil
	})

	task, err := lifecycle.Submit(context.Background(), materializedSubmission(testSubmission("missing-output.zip", "input")))
	if err != nil {
		t.Fatalf("submit task: %v", err)
	}
	waitForTaskStatus(t, lifecycle, task.ID, TaskFailed)

	persisted, err := lifecycle.Get(context.Background(), task.ID)
	if err != nil {
		t.Fatalf("get failed task: %v", err)
	}
	if persisted.Completed != 1 || len(persisted.Items) != 1 || persisted.Items[0].Status != string(ItemDone) || persisted.Items[0].ReportDocx == "" {
		t.Fatalf("missing artifact changed saved item outcome: %#v", persisted)
	}
	if !strings.Contains(persisted.Error, "completed report artifacts unavailable") {
		t.Fatalf("missing artifact did not set explicit task failure: %#v", persisted)
	}
	if _, err := os.Stat(filepath.Join(lifecycle.store.taskDir(task.ID), "reports-"+task.ID+".zip")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing artifact published a result ZIP: %v", err)
	}
	if got := runs.Load(); got != 1 {
		t.Fatalf("missing artifact ran report generation %d times, want 1", got)
	}
	time.Sleep(100 * time.Millisecond)
	if got := runs.Load(); got != 1 {
		t.Fatalf("failed task reran report generation %d times", got)
	}
}

func newFinalizationLifecycle(t *testing.T, factory pipelineFactory) *TaskLifecycle {
	t.Helper()
	lifecycle := newRecoveryTestLifecycle(t, t.TempDir(), 1, factory)
	if err := lifecycle.Start(context.Background()); err != nil {
		t.Fatalf("start task lifecycle: %v", err)
	}
	t.Cleanup(func() {
		closeRecoveryTestLifecycle(t, lifecycle)
	})
	return lifecycle
}

type noOutputRunner struct {
	runs *atomic.Int32
}

func (r noOutputRunner) Run(string, []string, func(LogEvent)) error {
	r.runs.Add(1)
	return nil
}
