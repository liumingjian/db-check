package web

import (
	"context"
	"net/http"
	"os"
	"testing"
	"time"
)

const thirtyDays = 30 * 24 * time.Hour

func (f *reportsFixture) listedOwn(h http.Handler, token, taskID string) listedTask {
	f.t.Helper()
	for _, task := range f.list(h, "/api/reports/mine", token) {
		if task.ID == taskID {
			return task
		}
	}
	f.t.Fatalf("task %s is not listed", taskID)
	return listedTask{}
}

func TestATaskExpiresThirtyDaysAfterSubmissionAndItsDownloadIsRefused(t *testing.T) {
	f := newReportsFixture(t)
	h := f.start(&fakePipeline{})
	user := f.token("user")
	taskID := f.generate(h, user, upload{"mall.zip", "mysql", "1.2.0"})
	f.waitFinished(h, user, taskID)

	f.clock.Advance(thirtyDays - time.Minute)
	user = f.token("user") // sessions last 7 days
	if task := f.listedOwn(h, user, taskID); task.Expired {
		t.Fatalf("task one minute before 30 days = %+v, want not expired", task)
	}
	if entries := f.reportEntries(h, user, taskID); len(entries) != 1 {
		t.Fatalf("download before 30 days = %v", entries)
	}

	f.clock.Advance(time.Minute)
	if task := f.listedOwn(h, user, taskID); !task.Expired || task.Status != "done" {
		t.Fatalf("task at 30 days = %+v, want done and expired", task)
	}
	var got listedTask
	decode(t, get(h, "/api/reports/tasks/"+taskID, user), &got)
	if !got.Expired {
		t.Fatalf("getTask at 30 days = %+v, want expired", got)
	}
	expectAPIError(t, get(h, "/api/reports/download/"+taskID, user), http.StatusConflict, "invalid", "30 天保留期")
}

func TestRetentionDeletesAnExpiredTasksFilesAndKeepsItsRecord(t *testing.T) {
	f := newReportsFixture(t)
	h := f.start(&fakePipeline{})
	user := f.token("user")
	expiring := f.generate(h, user, upload{"mall.zip", "mysql", "1.2.0"}, upload{"fail-core.zip", "oracle", ""})
	f.waitFinished(h, user, expiring)
	f.clock.Advance(time.Hour)
	young := f.generate(h, user, upload{"crm.zip", "gaussdb", "1.1.0"})
	f.waitFinished(h, user, young)
	// A task.json task from before the store expires under its own 24-hour rule only.
	if _, err := f.api.reports.legacy.Create(Task{ID: "legacy", Status: TaskDone}); err != nil {
		t.Fatalf("create legacy task: %v", err)
	}

	f.clock.Advance(thirtyDays - time.Hour)
	if err := f.api.reports.removeExpiredFiles(context.Background()); err != nil {
		t.Fatalf("removeExpiredFiles: %v", err)
	}

	for id, wantKept := range map[string]bool{expiring: false, young: true, "legacy": true} {
		_, err := os.Stat(f.api.reports.taskDir(id))
		if kept := err == nil; kept != wantKept {
			t.Errorf("task %s files kept = %v (stat: %v), want %v", id, kept, err, wantKept)
		}
	}
	user = f.token("user") // sessions last 7 days
	task := f.listedOwn(h, user, expiring)
	if !task.Expired || task.Status != "done" || len(task.Items) != 2 || task.Items[1].Outcome.Reason != "模拟失败" {
		t.Fatalf("expired task's record = %+v, want it kept whole and expired", task)
	}
	expectAPIError(t, get(h, "/api/reports/download/"+expiring, user), http.StatusConflict, "invalid", "30 天保留期")
	if entries := f.reportEntries(h, user, young); len(entries) != 1 {
		t.Fatalf("young task's download = %v", entries)
	}
}
