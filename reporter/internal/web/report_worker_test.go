package web

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"dbcheck/reporter/internal/reports"
)

func TestMoreSubmissionsThanTheOldQueueHeldAllFinish(t *testing.T) {
	f := newReportsFixture(t)
	pipeline := &fakePipeline{gate: make(chan struct{})}
	h := f.start(pipeline)
	user := f.token("user")

	// The old in-memory queue held 32 tasks behind the running one and
	// dropped the rest.
	var ids []string
	for i := range 40 {
		ids = append(ids, f.generate(h, user, upload{fmt.Sprintf("db-%02d.zip", i), "mysql", "1.2.0"}))
	}
	close(pipeline.gate)
	for _, id := range ids {
		if s := f.waitFinished(h, user, id); s.Status != "done" {
			t.Fatalf("task %s = %+v", id, s)
		}
	}
	if n := len(pipeline.ran()); n != 40 {
		t.Fatalf("pipeline ran %d items, want 40", n)
	}
}

func TestARestartResumesAnInterruptedTaskWithoutRerunningFinishedItems(t *testing.T) {
	f := newReportsFixture(t)
	user := f.token("user")
	taskID := f.generate(f.handler, user, upload{"a.zip", "mysql", "1.2.0"}, upload{"b-fail.zip", "mysql", "1.2.0"}, upload{"c.zip", "mysql", "1.2.0"})

	// A first run finished item 1 and was interrupted on item 2.
	ctx := context.Background()
	if id, ok, err := reports.ClaimNext(ctx, f.db); err != nil || !ok || id != taskID {
		t.Fatalf("ClaimNext = %q %v %v", id, ok, err)
	}
	taskDir := filepath.Join(f.cfg.DataDir, "tasks", taskID)
	first := &fakePipeline{}
	first.RunItem(ctx, ItemJob{TaskID: taskID, TaskDir: taskDir, Input: ItemInput{ID: "1", Name: "a.zip"}}, func(LogEvent) {})
	for _, u := range []reports.ItemUpdate{
		{TaskID: taskID, Position: 1, Status: reports.StatusDone},
		{TaskID: taskID, Position: 2, Status: reports.StatusProcessing},
	} {
		if err := reports.SetItemStatus(ctx, f.db, u); err != nil {
			t.Fatal(err)
		}
	}

	pipeline := &fakePipeline{}
	h := f.start(pipeline)
	if s := f.waitFinished(h, user, taskID); s.Status != "done" || s.Completed != 3 {
		t.Fatalf("status after restart = %+v", s)
	}
	if got := strings.Join(pipeline.ran(), ","); got != taskID+"/2,"+taskID+"/3" {
		t.Fatalf("restart ran %s, want items 2 and 3 only", got)
	}
	if got := f.reportEntries(h, user, taskID); strings.Join(got, ",") != "a.zip/report.docx,c.zip/report.docx" {
		t.Fatalf("report entries = %v", got)
	}
	task, err := reports.Get(ctx, f.db, taskID)
	if err != nil {
		t.Fatal(err)
	}
	if it := task.Items[1]; it.Status != reports.StatusFailed || it.Reason != "模拟失败" {
		t.Fatalf("failed item = %+v, want its reason kept", it)
	}
}

func TestATaskWhoseItemsAllFailIsFailed(t *testing.T) {
	f := newReportsFixture(t)
	h := f.start(&fakePipeline{})
	user := f.token("user")
	taskID := f.generate(h, user, upload{"fail.zip", "mysql", "1.2.0"})
	if s := f.waitFinished(h, user, taskID); s.Status != "failed" {
		t.Fatalf("status = %+v", s)
	}
	expectAPIError(t, get(h, "/api/reports/download/"+taskID, user), apiError{http.StatusConflict, "invalid", ""})
}

var errStoreDown = errors.New("store unavailable")

// flakyRecords passes the worker's store calls through to the real records,
// except that the first claimFailures claims and finishFailures finishes fail.
type flakyRecords struct {
	taskRecords
	mu             sync.Mutex
	claimFailures  int
	finishFailures int
}

func (r *flakyRecords) fails(left *int) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if *left == 0 {
		return false
	}
	*left--
	return true
}

func (r *flakyRecords) ClaimNext(ctx context.Context) (string, bool, error) {
	if r.fails(&r.claimFailures) {
		return "", false, errStoreDown
	}
	return r.taskRecords.ClaimNext(ctx)
}

func (r *flakyRecords) Finish(ctx context.Context, u reports.TaskUpdate) error {
	if r.fails(&r.finishFailures) {
		return errStoreDown
	}
	return r.taskRecords.Finish(ctx, u)
}

// startFlaky is start with the worker's store calls going through flaky,
// wrapped around the real records, and quick retries.
func (f *reportsFixture) startFlaky(pipeline ReportPipeline, flaky *flakyRecords) http.Handler {
	f.t.Helper()
	p := f.platform
	p.Pipeline = pipeline
	h, err := newAPIHandler(f.cfg, p, false)
	if err != nil {
		f.t.Fatalf("newAPIHandler: %v", err)
	}
	flaky.taskRecords = h.reports.records
	h.reports.records = flaky
	h.reports.backoff = retryBackoff{first: time.Millisecond, max: 10 * time.Millisecond}
	h.reports.start()
	return h.handler()
}

func TestTheWorkerRetriesClaimingAfterAStoreError(t *testing.T) {
	f := newReportsFixture(t)
	user := f.token("user")
	taskID := f.generate(f.handler, user, upload{"a.zip", "mysql", "1.2.0"})

	// No submission follows to wake the worker: it must retry on its own.
	h := f.startFlaky(&fakePipeline{}, &flakyRecords{claimFailures: 2})
	if s := f.waitFinished(h, user, taskID); s.Status != "done" {
		t.Fatalf("status = %+v", s)
	}
}

func TestATaskWhoseFinalSaveFailsStillFinishes(t *testing.T) {
	f := newReportsFixture(t)
	user := f.token("user")
	h := f.startFlaky(&fakePipeline{}, &flakyRecords{finishFailures: 2})
	taskID := f.generate(h, user, upload{"a.zip", "mysql", "1.2.0"})
	if s := f.waitFinished(h, user, taskID); s.Status != "done" || s.DownloadURL == "" {
		t.Fatalf("status = %+v", s)
	}
}
