package web

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"nhooyr.io/websocket"
)

func TestWebSocketAcceptsTheConfiguredOrigins(t *testing.T) {
	cases := []struct {
		name, allowed, origin string
	}{
		{"wildcard", "*", "http://evil.com"},
		{"host-only entry", "localhost:3000", "http://localhost:3000"},
		{"localhost alias", "http://localhost:3000", "http://127.0.0.1:3000"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := newReportsFixture(t)
			user := f.token("user")
			taskID := f.generate(f.handler, user, upload{"mall.zip", "mysql", "1.2.0"})
			f.cfg.AllowedOrigins = []string{c.allowed}
			h, err := newAPIHandler(f.cfg, f.platform, false)
			if err != nil {
				t.Fatal(err)
			}
			srv := httptest.NewServer(h.handler())
			defer srv.Close()

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			conn, resp, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(srv.URL, "http")+"/api/reports/ws/"+taskID,
				&websocket.DialOptions{Subprotocols: []string{user}, HTTPHeader: http.Header{"Origin": []string{c.origin}}})
			if err != nil {
				t.Fatalf("Dial failed: resp=%v err=%v", resp, err)
			}
			defer conn.Close(websocket.StatusNormalClosure, "")
			_, b, err := conn.Read(ctx)
			if err != nil || !strings.Contains(string(b), `"type":"progress"`) {
				t.Fatalf("first message = %s, %v; want the progress snapshot", b, err)
			}
		})
	}
}
