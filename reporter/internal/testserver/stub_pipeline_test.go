package testserver

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"dbcheck/reporter/internal/reports"

	"nhooyr.io/websocket"
)

func collectorZip(t *testing.T) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	for name, content := range map[string]string{
		"manifest.json": `{"db_type":"mysql","artifacts":{"result":"result.json"}}`,
		"result.json":   `{"meta":{"collector_version":"1.2.0"}}`,
	} {
		w, err := zw.Create(name)
		if err != nil {
			t.Fatal(err)
		}
		w.Write([]byte(content))
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// generate submits ZIPs with these names and returns the task id.
func (c client) generate(token string, names ...string) string {
	c.t.Helper()
	var body bytes.Buffer
	mw := multipart.NewWriter(&body)
	for _, name := range names {
		part, err := mw.CreateFormFile("zips", name)
		if err != nil {
			c.t.Fatal(err)
		}
		part.Write(collectorZip(c.t))
	}
	mw.Close()
	req := httptest.NewRequest(http.MethodPost, "/api/reports/generate", &body)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	c.server.ServeHTTP(rec, req)
	c.expect(rec, http.StatusOK)
	var resp struct {
		TaskID string `json:"task_id"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		c.t.Fatal(err)
	}
	return resp.TaskID
}

func (c client) taskStatus(token, taskID string) string {
	c.t.Helper()
	rec := c.do(http.MethodGet, "/api/reports/status/"+taskID, token, nil)
	c.expect(rec, http.StatusOK)
	var s struct{ Status string }
	if err := json.Unmarshal(rec.Body.Bytes(), &s); err != nil {
		c.t.Fatal(err)
	}
	return s.Status
}

func (c client) reset(now string) {
	c.t.Helper()
	c.expect(c.do(http.MethodPost, "/test/reset", "", map[string]string{"now": now}), http.StatusNoContent)
}

func (c client) moveClock(now string) {
	c.t.Helper()
	c.expect(c.do(http.MethodPost, "/test/clock", "", map[string]string{"now": now}), http.StatusNoContent)
}

func TestStubTaskStaysProcessingUntilTheClockReachesIt(t *testing.T) {
	c := newTestServer(t)
	c.reset(seedNow)
	token := c.signIn("user")
	taskID := c.generate(token, "a.zip", "b.zip")

	if got := c.taskStatus(token, taskID); got != "processing" {
		t.Fatalf("status right after generate = %q", got)
	}
	c.moveClock("2026-10-01T08:00:09Z")
	if got := c.taskStatus(token, taskID); got != "processing" {
		t.Fatalf("status before its second item is due = %q", got)
	}
	// Moving the clock past the last item waits for the task to finish.
	c.moveClock("2026-10-01T08:00:10Z")
	if got := c.taskStatus(token, taskID); got != "done" {
		t.Fatalf("status once every item is due = %q", got)
	}
}

func TestStubFailsItemsWhoseNameSaysFail(t *testing.T) {
	c := newTestServer(t)
	c.reset(seedNow)
	token := c.signIn("user")
	taskID := c.generate(token, "ok.zip", "db-fail.zip")
	c.moveClock("2026-10-01T09:00:00Z")

	task, err := reports.Get(context.Background(), c.server.(*Server).db, taskID)
	if err != nil {
		t.Fatal(err)
	}
	if task.Status != reports.StatusDone || task.Items[0].Status != reports.StatusDone ||
		task.Items[1].Status != reports.StatusFailed || task.Items[1].Reason != StubFailReason {
		t.Fatalf("task = %+v", task)
	}
	all := c.generate(token, "fail.zip")
	c.moveClock("2026-10-01T10:00:00Z")
	if got := c.taskStatus(token, all); got != "failed" {
		t.Fatalf("a task whose items all fail = %q", got)
	}
}

func TestWatchingAStubTaskFinishesIt(t *testing.T) {
	c := newTestServer(t)
	c.reset(seedNow)
	token := c.signIn("user")
	taskID := c.generate(token, "a.zip", "b.zip")
	srv := httptest.NewServer(c.server)
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(srv.URL, "http")+"/api/reports/ws/"+taskID,
		&websocket.DialOptions{Subprotocols: []string{token}, HTTPHeader: http.Header{"Origin": []string{"http://localhost:3000"}}})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "")
	for {
		_, msg, err := conn.Read(ctx)
		if err != nil {
			t.Fatalf("read before done: %v", err)
		}
		if strings.Contains(string(msg), `"type":"done"`) {
			break
		}
	}
	if got := c.taskStatus(token, taskID); got != "done" {
		t.Fatalf("status after watching = %q", got)
	}
}

func TestResetRemovesTaskFilesAndUnblocksTheWorker(t *testing.T) {
	c := newTestServer(t)
	c.reset(seedNow)
	token := c.signIn("user")
	waiting := c.generate(token, "a.zip") // left waiting on the clock

	c.reset(seedNow)
	tasksDir := filepath.Join(c.server.(*Server).dataDir, "tasks")
	entries, err := os.ReadDir(tasksDir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if !strings.HasPrefix(e.Name(), "task-seed-") {
			t.Fatalf("reset left task directory %s (the waiting task was %s)", e.Name(), waiting)
		}
	}

	token = c.signIn("user")
	taskID := c.generate(token, "b.zip")
	c.moveClock("2026-10-01T08:00:05Z")
	if got := c.taskStatus(token, taskID); got != "done" {
		t.Fatalf("a task after the reset = %q", got)
	}
}
