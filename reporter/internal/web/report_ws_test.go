package web

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"dbcheck/reporter/internal/reports"

	"nhooyr.io/websocket"
)

func TestWebSocketReplaysProgressAndEndsWithDone(t *testing.T) {
	f := newReportsFixture(t)
	h := f.start(&fakePipeline{})
	user := f.token("user")
	taskID := f.generate(h, user, upload{"a.zip", "mysql", "1.2.0"}, upload{"b.zip", "mysql", "1.2.0"})
	srv := httptest.NewServer(h)
	defer srv.Close()

	conn, _, err := dialWS(t, srv, taskID, user)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "")
	if conn.Subprotocol() != user {
		t.Fatalf("subprotocol = %q, want the session token", conn.Subprotocol())
	}
	if done := readUntilDone(t, conn); !strings.Contains(done, `"download_url":"/api/reports/download/`+taskID+`"`) {
		t.Fatalf("done = %s", done)
	}
}

func TestWebSocketOfAnExpiredTaskOffersNoDownload(t *testing.T) {
	f := newReportsFixture(t)
	h := f.start(&fakePipeline{})
	user := f.token("user")
	taskID := f.generate(h, user, upload{"a.zip", "mysql", "1.2.0"})
	f.waitFinished(h, user, taskID)
	f.clock.Advance(reports.Retention)
	user = f.token("user") // the first session has expired too
	srv := httptest.NewServer(h)
	defer srv.Close()

	conn, _, err := dialWS(t, srv, taskID, user)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "")
	if done := readUntilDone(t, conn); strings.Contains(done, "download_url") {
		t.Fatalf("done of an expired task = %s", done)
	}
}

// readUntilDone reads the task's events until its done message, and returns it.
func readUntilDone(t *testing.T, conn *websocket.Conn) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for {
		_, msg, err := conn.Read(ctx)
		if err != nil {
			t.Fatalf("read before done: %v", err)
		}
		if strings.Contains(string(msg), `"type":"done"`) {
			return string(msg)
		}
	}
}
