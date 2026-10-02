package web

import (
	"net/http"
	"slices"
	"testing"
	"time"

	"dbcheck/reporter/internal/users"
)

// listedTask is the contract's ReportTask as the lists and getTask answer it.
type listedTask struct {
	ID        string
	Submitter struct{ ID, DisplayName string }
	Status    string
	CreatedAt string
	Expired   bool
	Items     []struct {
		FileName         string
		DBType           string
		CollectorVersion *string
		CollectorNotice  any
		Outcome          struct{ Status, Reason string }
	}
}

func (f *reportsFixture) list(h http.Handler, path, token string) []listedTask {
	f.t.Helper()
	rec := get(h, path, token)
	if rec.Code != http.StatusOK {
		f.t.Fatalf("GET %s: %d %s", path, rec.Code, rec.Body)
	}
	var tasks []listedTask
	decode(f.t, rec, &tasks)
	return tasks
}

func ids(tasks []listedTask) []string {
	out := make([]string, 0, len(tasks))
	for _, t := range tasks {
		out = append(out, t.ID)
	}
	return out
}

func TestMyReportsListsTheCallersTasksNewestFirstWithEachItemsOutcome(t *testing.T) {
	f := newReportsFixture(t)
	h := f.start(&fakePipeline{})
	user, admin := f.token("user"), f.token("admin")
	older := f.generate(h, user, upload{"mall.zip", "mysql", "1.2.0"}, upload{"fail-core.zip", "oracle", ""})
	f.waitFinished(h, user, older)
	f.generate(h, admin, upload{"admin.zip", "mysql", "1.2.0"})
	f.clock.Advance(time.Minute)
	newer := f.generate(f.handler, user, upload{"crm.zip", "gaussdb", "1.1.0"})

	mine := f.list(h, "/api/reports/mine", user)
	if got := ids(mine); len(got) != 2 || got[0] != newer || got[1] != older {
		t.Fatalf("my reports = %v, want [%s %s]", got, newer, older)
	}
	if s := mine[0].Submitter; s.ID != "u-user" || s.DisplayName != "user" {
		t.Fatalf("submitter = %+v", s)
	}
	if mine[0].Status != "processing" || mine[0].CreatedAt != "2026-10-01T08:01:00.000Z" || mine[0].Expired {
		t.Fatalf("newer task = %+v", mine[0])
	}
	if it := mine[0].Items[0]; it.FileName != "crm.zip" || it.DBType != "gaussdb" || *it.CollectorVersion != "1.1.0" ||
		it.Outcome.Status != "processing" || it.CollectorNotice != nil {
		t.Fatalf("newer task's item = %+v", it)
	}

	if mine[1].Status != "done" {
		t.Fatalf("older task status = %q, want done (one item failed, one did not)", mine[1].Status)
	}
	done, failed := mine[1].Items[0], mine[1].Items[1]
	if done.Outcome.Status != "done" || done.Outcome.Reason != "" {
		t.Fatalf("done item = %+v", done)
	}
	if failed.FileName != "fail-core.zip" || failed.CollectorVersion != nil ||
		failed.Outcome.Status != "failed" || failed.Outcome.Reason != "模拟失败" {
		t.Fatalf("failed item = %+v", failed)
	}

	if got := ids(f.list(h, "/api/reports/mine", f.token("other"))); len(got) != 0 {
		t.Fatalf("another engineer's reports = %v, want none", got)
	}
}

func TestAllReportsIsForAdminsAndNarrowsBySubmitterDisabledUsersIncluded(t *testing.T) {
	f := newReportsFixture(t)
	f.addUser("wangwu", users.RoleEngineer, users.StatusActive)
	user, admin, wangwu := f.token("user"), f.token("admin"), f.token("wangwu")
	usersTask := f.generate(f.handler, user, upload{"mall.zip", "mysql", "1.2.0"})
	f.clock.Advance(time.Minute)
	wangwusTask := f.generate(f.handler, wangwu, upload{"ops.zip", "oracle", "1.2.0"})
	f.clock.Advance(time.Minute)
	adminsTask := f.generate(f.handler, admin, upload{"core.zip", "mysql", "1.2.0"})
	if rec := f.do(http.MethodPost, "/api/users/u-wangwu/disable", admin, map[string]string{"reason": "离职"}); rec.Code != http.StatusOK {
		t.Fatalf("disable wangwu: %d %s", rec.Code, rec.Body)
	}

	if got := ids(f.list(f.handler, "/api/reports", admin)); !slices.Equal(got, []string{adminsTask, wangwusTask, usersTask}) {
		t.Fatalf("all reports = %v", got)
	}
	if got := ids(f.list(f.handler, "/api/reports?submitterId=u-wangwu", admin)); !slices.Equal(got, []string{wangwusTask}) {
		t.Fatalf("wangwu's reports = %v", got)
	}
	if got := f.list(f.handler, "/api/reports?submitterId=no-such-user", admin); got == nil || len(got) != 0 {
		t.Fatalf("an unknown submitter's reports = %#v, want []", got)
	}

	expectAPIError(t, get(f.handler, "/api/reports", user), apiError{http.StatusForbidden, "forbidden", ""})
	expectAPIError(t, get(f.handler, "/api/reports?submitterId=u-user", user), apiError{http.StatusForbidden, "forbidden", ""})
	expectAPIError(t, get(f.handler, "/api/reports", ""), apiError{http.StatusUnauthorized, "unauthorized", ""})
}

func TestGetTaskShowsATaskToItsSubmitterAndAdminsOnly(t *testing.T) {
	f := newReportsFixture(t)
	user, other, admin := f.token("user"), f.token("other"), f.token("admin")
	taskID := f.generate(f.handler, user, upload{"mall.zip", "mysql", "1.2.0"})

	for _, token := range []string{user, admin} {
		rec := get(f.handler, "/api/reports/tasks/"+taskID, token)
		var task listedTask
		decode(t, rec, &task)
		if rec.Code != http.StatusOK || task.ID != taskID || task.Submitter.ID != "u-user" || len(task.Items) != 1 {
			t.Fatalf("getTask: %d %+v", rec.Code, task)
		}
	}
	expectAPIError(t, get(f.handler, "/api/reports/tasks/"+taskID, other), apiError{http.StatusNotFound, "not_found", "报告任务不存在"})
	expectAPIError(t, get(f.handler, "/api/reports/tasks/no-such-task", user), apiError{http.StatusNotFound, "not_found", "报告任务不存在"})
}
