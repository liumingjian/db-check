package web

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"dbcheck/reporter/internal/reports"

	"nhooyr.io/websocket"
)

func TestGenerateStoresTheTaskBeforeAcknowledgingAndARestartRunsIt(t *testing.T) {
	f := newReportsFixture(t)
	user := f.token("user")
	taskID := f.generate(f.handler, user, upload{"mall.zip", "mysql", "1.2.0"}, upload{"core.zip", "oracle", "1.1.0"})

	// Nothing runs it in this server, yet the task is stored and processing.
	if s := f.status(f.handler, user, taskID); s.Status != "processing" || s.Total != 2 || s.Completed != 0 {
		t.Fatalf("status before any worker = %+v", s)
	}

	pipeline := &fakePipeline{}
	restarted := f.start(pipeline)
	if s := f.waitFinished(restarted, user, taskID); s.Status != "done" || s.Completed != 2 || s.DownloadURL != "/api/reports/download/"+taskID {
		t.Fatalf("status after restart = %+v", s)
	}
	if got := f.reportEntries(restarted, user, taskID); strings.Join(got, ",") != "core.zip/report.docx,mall.zip/report.docx" {
		t.Fatalf("report entries = %v", got)
	}
}

func TestGenerateRecordsTheSubmitterAndWhatTheServerReadFromEachZip(t *testing.T) {
	f := newReportsFixture(t)
	taskID := f.generate(f.handler, f.token("user"),
		upload{"ora.zip", "Oracle", "1.1.0"}, upload{"gauss.zip", "gaussdb", ""})

	task, err := reports.Get(context.Background(), f.db, taskID)
	if err != nil {
		t.Fatal(err)
	}
	if task.SubmitterID != "u-user" || !task.CreatedAt.Equal(f.clock.Now()) {
		t.Fatalf("task = %+v", task)
	}
	got := make([]string, 0, len(task.Items))
	for _, it := range task.Items {
		version := "<none>"
		if it.CollectorVersion != nil {
			version = *it.CollectorVersion
		}
		got = append(got, fmt.Sprintf("%d %s %s %s %s", it.Position, it.FileName, it.DBType, version, it.Status))
	}
	want := []string{"1 ora.zip oracle 1.1.0 queued", "2 gauss.zip gaussdb <none> queued"}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("items = %q, want %q", got, want)
	}
}

func TestGenerateRefusesAZipTheServerCannotRead(t *testing.T) {
	f := newReportsFixture(t)
	files := append(zipsOf(t, upload{"good.zip", "mysql", "1.2.0"}), formFile{"zips", "broken.zip", []byte("not a zip")})
	rec := serve(f.handler, generateRequest(t, f.token("user"), files))
	expectAPIError(t, rec, apiError{http.StatusBadRequest, "invalid", "broken.zip：不是有效的 ZIP 文件"})

	entries, err := os.ReadDir(filepath.Join(f.cfg.DataDir, "tasks"))
	if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("a refused submission left %d task directories", len(entries))
	}
}

func TestGenerateRefusesAnEmptySubmission(t *testing.T) {
	f := newReportsFixture(t)
	expectAPIError(t, serve(f.handler, generateRequest(t, f.token("user"), nil)), apiError{http.StatusBadRequest, "invalid", "ZIP"})
}

func TestDownloadRefusesATaskStillGenerating(t *testing.T) {
	f := newReportsFixture(t)
	user := f.token("user")
	taskID := f.generate(f.handler, user, upload{"a.zip", "mysql", "1.2.0"})
	expectAPIError(t, get(f.handler, "/api/reports/download/"+taskID, user), apiError{http.StatusConflict, "invalid", "尚未生成完成"})
}

func TestStatusOfAnExpiredTaskOffersNoDownload(t *testing.T) {
	f := newReportsFixture(t)
	h := f.start(&fakePipeline{})
	user := f.token("user")
	taskID := f.generate(h, user, upload{"a.zip", "mysql", "1.2.0"})
	f.waitFinished(h, user, taskID)
	f.clock.Advance(reports.Retention)
	user = f.token("user") // the first session has expired too

	if s := f.status(h, user, taskID); s.Status != "done" || s.DownloadURL != "" {
		t.Fatalf("status of an expired task = %+v, want done without a download_url", s)
	}
	expectAPIError(t, get(h, "/api/reports/download/"+taskID, user), apiError{http.StatusConflict, "invalid", "保留期"})
}

func TestReportTasksAreHiddenFromOtherEngineersButNotFromAdmins(t *testing.T) {
	f := newReportsFixture(t)
	h := f.start(&fakePipeline{})
	user, other, admin := f.token("user"), f.token("other"), f.token("admin")
	taskID := f.generate(h, user, upload{"mall.zip", "mysql", "1.2.0"})
	f.waitFinished(h, user, taskID)
	srv := httptest.NewServer(h)
	defer srv.Close()

	for _, path := range []string{"/api/reports/status/", "/api/reports/download/"} {
		expectAPIError(t, get(h, path+taskID, other), apiError{http.StatusNotFound, "not_found", "报告任务不存在"})
		if rec := get(h, path+taskID, admin); rec.Code != http.StatusOK {
			t.Fatalf("admin GET %s: %d %s", path, rec.Code, rec.Body)
		}
	}
	if _, resp, err := dialWS(t, srv, taskID, other); err == nil || resp == nil || resp.StatusCode != http.StatusNotFound {
		t.Fatalf("other engineer's WebSocket: resp=%v err=%v, want 404", resp, err)
	}
	conn, _, err := dialWS(t, srv, taskID, admin)
	if err != nil {
		t.Fatalf("admin's WebSocket: %v", err)
	}
	conn.Close(websocket.StatusNormalClosure, "")
}

func TestReportRoutesRefuseTheRetiredSharedToken(t *testing.T) {
	f := newReportsFixture(t)
	user := f.token("user")
	taskID := f.generate(f.handler, user, upload{"mall.zip", "mysql", "1.2.0"})
	srv := httptest.NewServer(f.handler)
	defer srv.Close()

	const shared = "ATI" // the retired DBCHECK_API_TOKEN default
	rec := serve(f.handler, generateRequest(t, shared, zipsOf(t, upload{"mall.zip", "mysql", "1.2.0"})))
	expectAPIError(t, rec, apiError{http.StatusUnauthorized, "unauthorized", ""})
	for _, path := range []string{"/api/reports/status/", "/api/reports/download/"} {
		expectAPIError(t, get(f.handler, path+taskID, shared), apiError{http.StatusUnauthorized, "unauthorized", ""})
	}
	if _, resp, err := dialWS(t, srv, taskID, shared); err == nil || resp == nil || resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("WebSocket with the shared token: resp=%v err=%v, want 401", resp, err)
	}
}

func TestGenerateHandsEveryWDRFileOfAnItemToThePipeline(t *testing.T) {
	f := newReportsFixture(t)
	pipeline := &fakePipeline{}
	h := f.start(pipeline)
	user := f.token("user")
	files := append(zipsOf(t, upload{"gauss.zip", "gaussdb", "1.2.0"}),
		formFile{"wdr_1", "wdr-cluster.html", []byte("<html>")},
		formFile{"wdr_1", "wdr-node.html", []byte("<html>")})
	rec := serve(h, generateRequest(t, user, files))
	if rec.Code != http.StatusOK {
		t.Fatalf("generate: %d %s", rec.Code, rec.Body)
	}
	var body struct {
		TaskID string `json:"task_id"`
	}
	decode(t, rec, &body)
	f.waitFinished(h, user, body.TaskID)

	job := pipeline.jobs[0]
	if len(job.Input.WDRPaths) != 2 || job.Input.WDRPaths[0] == job.Input.WDRPaths[1] || job.Input.AWRPath != "" {
		t.Fatalf("pipeline got %+v", job.Input)
	}
}

func TestGenerateRefusesTwoAWRFilesForOneItem(t *testing.T) {
	f := newReportsFixture(t)
	files := append(zipsOf(t, upload{"ora.zip", "oracle", "1.2.0"}),
		formFile{"awr_1", "awr-a.html", []byte("<html>")},
		formFile{"awr_1", "awr-b.html", []byte("<html>")})
	expectAPIError(t, serve(f.handler, generateRequest(t, f.token("user"), files)), apiError{http.StatusBadRequest, "invalid", "AWR"})
}

func TestGenerateEnforcesTheUploadLimit(t *testing.T) {
	f := newReportsFixture(t)
	f.cfg.MaxUploadBytes = 64
	h := f.start(&fakePipeline{})
	rec := serve(h, generateRequest(t, f.token("user"), zipsOf(t, upload{"big.zip", "mysql", "1.2.0"})))
	expectAPIError(t, rec, apiError{http.StatusRequestEntityTooLarge, "invalid", "上传文件超过大小上限"})
}
