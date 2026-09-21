package web

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestTaskLifecyclePublishesCompleteStagedSubmission(t *testing.T) {
	lifecycle, err := NewTaskLifecycle(Config{DataDir: t.TempDir(), MaxAcceptedTasks: 1})
	if err != nil {
		t.Fatalf("NewTaskLifecycle failed: %v", err)
	}
	acceptedAt := time.Date(2026, 9, 21, 12, 0, 0, 0, time.UTC)
	lifecycle.store.now = func() time.Time { return acceptedAt }

	task, err := lifecycle.Submit(context.Background(), materializedSubmission(testSubmission("first.zip", "first")))
	if err != nil {
		t.Fatalf("Submit failed: %v", err)
	}
	if task.Status != TaskQueued || task.AcceptedOrder != 1 || !task.AcceptedAt.Equal(acceptedAt) {
		t.Fatalf("unexpected accepted task: %#v", task)
	}
	if len(task.Items) != 1 || task.Items[0].Name != "first.zip" || task.Items[0].Status != string(TaskQueued) {
		t.Fatalf("unexpected persisted item metadata: %#v", task.Items)
	}

	publishedDir := lifecycle.store.taskDir(task.ID)
	if _, err := os.Stat(filepath.Join(publishedDir, "task.json")); err != nil {
		t.Fatalf("published task metadata missing: %v", err)
	}
	contents, err := os.ReadFile(filepath.Join(publishedDir, "uploads", "zip-1-first.zip"))
	if err != nil {
		t.Fatalf("published upload missing: %v", err)
	}
	if string(contents) != "first" {
		t.Fatalf("unexpected upload contents: %q", contents)
	}
	ids, err := lifecycle.store.ListIDs()
	if err != nil || len(ids) != 1 || ids[0] != task.ID {
		t.Fatalf("published task discovery mismatch: ids=%#v err=%v", ids, err)
	}
	assertEmptyStaging(t, lifecycle.store)
}

func TestTaskLifecycleCleansRejectedAndCanceledStaging(t *testing.T) {
	t.Run("rejected upload releases reservation", func(t *testing.T) {
		lifecycle, err := NewTaskLifecycle(Config{DataDir: t.TempDir(), MaxAcceptedTasks: 1})
		if err != nil {
			t.Fatalf("NewTaskLifecycle failed: %v", err)
		}
		invalid := testSubmission("not-a-zip.txt", "invalid")
		if _, err := lifecycle.Submit(context.Background(), materializedSubmission(invalid)); !errors.Is(err, ErrInvalidSubmission) {
			t.Fatalf("expected invalid submission error, got %v", err)
		}
		assertNoPublishedTasks(t, lifecycle.store)
		assertEmptyStaging(t, lifecycle.store)

		if _, err := lifecycle.Submit(context.Background(), materializedSubmission(testSubmission("valid.zip", "valid"))); err != nil {
			t.Fatalf("reservation was not released after rejected submission: %v", err)
		}
	})

	t.Run("canceled copy releases reservation", func(t *testing.T) {
		lifecycle, err := NewTaskLifecycle(Config{DataDir: t.TempDir(), MaxAcceptedTasks: 1})
		if err != nil {
			t.Fatalf("NewTaskLifecycle failed: %v", err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		canceledSubmission := ReportSubmission{Items: []ReportItemSubmission{{
			Zip: SubmissionFile{
				Name: "canceled.zip",
				Open: func() (io.ReadCloser, error) {
					return io.NopCloser(&cancelOnFirstRead{reader: bytes.NewReader([]byte("content")), cancel: cancel}), nil
				},
			},
		}}}
		if _, err := lifecycle.Submit(ctx, materializedSubmission(canceledSubmission)); !errors.Is(err, context.Canceled) {
			t.Fatalf("expected canceled submission, got %v", err)
		}
		assertNoPublishedTasks(t, lifecycle.store)
		assertEmptyStaging(t, lifecycle.store)

		if _, err := lifecycle.Submit(context.Background(), materializedSubmission(testSubmission("valid.zip", "valid"))); err != nil {
			t.Fatalf("reservation was not released after cancellation: %v", err)
		}
	})
}

func TestTaskLifecycleKeepsPublishedTaskAfterRequestCancellation(t *testing.T) {
	lifecycle, err := NewTaskLifecycle(Config{DataDir: t.TempDir(), MaxAcceptedTasks: 1})
	if err != nil {
		t.Fatalf("NewTaskLifecycle failed: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	task, err := lifecycle.Submit(ctx, materializedSubmission(testSubmission("accepted.zip", "accepted")))
	if err != nil {
		t.Fatalf("Submit failed: %v", err)
	}
	cancel()

	persisted, err := lifecycle.Get(context.Background(), task.ID)
	if err != nil {
		t.Fatalf("published task was removed after cancellation: %v", err)
	}
	if persisted.Status != TaskQueued {
		t.Fatalf("unexpected published task status: %#v", persisted)
	}
}

func TestTaskLifecycleCountsReservationsTowardCapacity(t *testing.T) {
	lifecycle, err := NewTaskLifecycle(Config{DataDir: t.TempDir(), MaxAcceptedTasks: 1})
	if err != nil {
		t.Fatalf("NewTaskLifecycle failed: %v", err)
	}
	started := make(chan struct{})
	release := make(chan struct{})
	result := make(chan error, 1)
	go func() {
		_, err := lifecycle.Submit(context.Background(), SubmissionRequest{
			Materialize: func(context.Context) (ReportSubmission, error) {
				close(started)
				<-release
				return testSubmission("slow.zip", "slow"), nil
			},
		})
		result <- err
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("submission did not reserve capacity")
	}

	materialized := false
	_, err = lifecycle.Submit(context.Background(), SubmissionRequest{
		Materialize: func(context.Context) (ReportSubmission, error) {
			materialized = true
			return testSubmission("second.zip", "second"), nil
		},
	})
	if !errors.Is(err, ErrTaskCapacity) {
		t.Fatalf("expected capacity error, got %v", err)
	}
	if materialized {
		t.Fatal("full capacity materialized a new submission")
	}
	close(release)
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("reserved submission failed: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("reserved submission did not finish")
	}
}

func TestTaskLifecycleDispatchesAcceptedTasksFIFOAfterCoalescedWake(t *testing.T) {
	starts := make(chan string, 3)
	releases := make(chan struct{}, 3)
	cfg := Config{DataDir: t.TempDir(), MaxAcceptedTasks: 3, RetentionTTL: 0}
	lifecycle, err := newTaskLifecycle(cfg, func() (*Pipeline, error) {
		pipeline := controlledLifecyclePipeline()
		pipeline.ExtractZip = func(zipPath, _ string) error {
			starts <- strings.TrimPrefix(filepath.Base(zipPath), "zip-1-")
			<-releases
			return nil
		}
		return pipeline, nil
	})
	if err != nil {
		t.Fatalf("newTaskLifecycle failed: %v", err)
	}
	if err := lifecycle.Start(context.Background()); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := lifecycle.Close(ctx); err != nil {
			t.Errorf("Close failed: %v", err)
		}
	})

	first, err := lifecycle.Submit(context.Background(), materializedSubmission(testSubmission("first.zip", "first")))
	if err != nil {
		t.Fatalf("submit first task: %v", err)
	}
	if got := waitForStart(t, starts); got != "first.zip" {
		t.Fatalf("first task started as %q", got)
	}

	// Keep a stale wake buffered while the worker is busy. The following publish
	// coalesces its signal, so completion must recheck durable queued work.
	lifecycle.signalDispatcher()
	second, err := lifecycle.Submit(context.Background(), materializedSubmission(testSubmission("second.zip", "second")))
	if err != nil {
		t.Fatalf("submit second task: %v", err)
	}
	third, err := lifecycle.Submit(context.Background(), materializedSubmission(testSubmission("third.zip", "third")))
	if err != nil {
		t.Fatalf("submit third task: %v", err)
	}
	if first.AcceptedOrder != 1 || second.AcceptedOrder != 2 || third.AcceptedOrder != 3 {
		t.Fatalf("unexpected FIFO orders: first=%d second=%d third=%d", first.AcceptedOrder, second.AcceptedOrder, third.AcceptedOrder)
	}

	releases <- struct{}{}
	if got := waitForStart(t, starts); got != "second.zip" {
		t.Fatalf("second task started as %q", got)
	}
	releases <- struct{}{}
	if got := waitForStart(t, starts); got != "third.zip" {
		t.Fatalf("third task started as %q", got)
	}
	releases <- struct{}{}
	waitForTaskStatus(t, lifecycle, third.ID, TaskDone)
}

func materializedSubmission(submission ReportSubmission) SubmissionRequest {
	return SubmissionRequest{
		Materialize: func(context.Context) (ReportSubmission, error) {
			return submission, nil
		},
	}
}

func testSubmission(name, content string) ReportSubmission {
	return ReportSubmission{Items: []ReportItemSubmission{{
		Zip: SubmissionFile{
			Name: name,
			Open: func() (io.ReadCloser, error) {
				return io.NopCloser(bytes.NewReader([]byte(content))), nil
			},
		},
	}}}
}

type cancelOnFirstRead struct {
	reader *bytes.Reader
	cancel context.CancelFunc
	done   bool
}

func (r *cancelOnFirstRead) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	if !r.done {
		r.done = true
		r.cancel()
	}
	return n, err
}

func assertNoPublishedTasks(t *testing.T, store *TaskStore) {
	t.Helper()
	ids, err := store.ListIDs()
	if err != nil {
		t.Fatalf("ListIDs failed: %v", err)
	}
	if len(ids) != 0 {
		t.Fatalf("unexpected published tasks: %#v", ids)
	}
}

func assertEmptyStaging(t *testing.T, store *TaskStore) {
	t.Helper()
	entries, err := os.ReadDir(store.stagingDir())
	if err != nil {
		t.Fatalf("read staging directory: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("unexpected staged entries: %#v", entries)
	}
}

func waitForStart(t *testing.T, starts <-chan string) string {
	t.Helper()
	select {
	case name := <-starts:
		return name
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for task start")
		return ""
	}
}
