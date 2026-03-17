package web

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"nhooyr.io/websocket"
)

func TestWebSocketAuthViaSubprotocol(t *testing.T) {
	cfg := Config{
		DataDir:        t.TempDir(),
		AllowedOrigins: []string{"http://example.com"},
		APIToken:       "secret",
	}
	h, err := newAPIHandler(cfg, false)
	if err != nil {
		t.Fatalf("newAPIHandler failed: %v", err)
	}
	if _, err := h.store.Create(Task{ID: "t1", Status: TaskProcessing, Total: 1}); err != nil {
		t.Fatalf("Create task failed: %v", err)
	}

	srv := httptest.NewServer(h.handler())
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/api/reports/ws/t1"
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, resp, err := websocket.Dial(ctx, wsURL, &websocket.DialOptions{
		Subprotocols: []string{"wrong-token"},
		HTTPHeader:   http.Header{"Origin": []string{"http://example.com"}},
	})
	if err == nil {
		t.Fatalf("expected dial error")
	}
	if resp == nil || resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected status %d got resp=%v err=%v", http.StatusUnauthorized, resp, err)
	}
}

func TestWebSocketReplayAndProgressSnapshot(t *testing.T) {
	cfg := Config{
		DataDir:        t.TempDir(),
		AllowedOrigins: []string{"http://example.com"},
		APIToken:       "secret",
		LogReplayLines: 1000,
	}
	h, err := newAPIHandler(cfg, false)
	if err != nil {
		t.Fatalf("newAPIHandler failed: %v", err)
	}
	if _, err := h.store.Create(Task{ID: "t1", Status: TaskProcessing, Total: 1}); err != nil {
		t.Fatalf("Create task failed: %v", err)
	}
	h.hub.emitLog("t1", "info", "hello")

	srv := httptest.NewServer(h.handler())
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/api/reports/ws/t1"
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, resp, err := websocket.Dial(ctx, wsURL, &websocket.DialOptions{
		Subprotocols: []string{cfg.APIToken},
		HTTPHeader:   http.Header{"Origin": []string{"http://example.com"}},
	})
	if err != nil {
		t.Fatalf("Dial failed: resp=%v err=%v", resp, err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "")
	if got := conn.Subprotocol(); got != cfg.APIToken {
		t.Fatalf("unexpected subprotocol: %q", got)
	}

	_, b1, err := conn.Read(ctx)
	if err != nil {
		t.Fatalf("Read #1 failed: %v", err)
	}
	var m1 map[string]any
	if err := json.Unmarshal(b1, &m1); err != nil {
		t.Fatalf("decode #1 failed: %v", err)
	}
	if m1["type"] != "log" {
		t.Fatalf("expected first message type=log got %#v", m1)
	}

	_, b2, err := conn.Read(ctx)
	if err != nil {
		t.Fatalf("Read #2 failed: %v", err)
	}
	var m2 map[string]any
	if err := json.Unmarshal(b2, &m2); err != nil {
		t.Fatalf("decode #2 failed: %v", err)
	}
	if m2["type"] != "progress" {
		t.Fatalf("expected second message type=progress got %#v", m2)
	}
}

func TestWebSocketAllowsWildcardOrigin(t *testing.T) {
	cfg := Config{
		DataDir:        t.TempDir(),
		AllowedOrigins: []string{"*"},
		APIToken:       "secret",
	}
	h, err := newAPIHandler(cfg, false)
	if err != nil {
		t.Fatalf("newAPIHandler failed: %v", err)
	}
	if _, err := h.store.Create(Task{ID: "t1", Status: TaskProcessing, Total: 1}); err != nil {
		t.Fatalf("Create task failed: %v", err)
	}

	srv := httptest.NewServer(h.handler())
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/api/reports/ws/t1"
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, resp, err := websocket.Dial(ctx, wsURL, &websocket.DialOptions{
		Subprotocols: []string{cfg.APIToken},
		HTTPHeader:   http.Header{"Origin": []string{"http://evil.com"}},
	})
	if err != nil {
		t.Fatalf("Dial failed: resp=%v err=%v", resp, err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "")

	if got := conn.Subprotocol(); got != cfg.APIToken {
		t.Fatalf("unexpected subprotocol: %q", got)
	}

	_, b, err := conn.Read(ctx)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if m["type"] != "progress" {
		t.Fatalf("expected first message type=progress got %#v", m)
	}
}

func TestWebSocketAllowsHostOnlyOriginEntry(t *testing.T) {
	cfg := Config{
		DataDir:        t.TempDir(),
		AllowedOrigins: []string{"localhost:3000"},
		APIToken:       "secret",
	}
	h, err := newAPIHandler(cfg, false)
	if err != nil {
		t.Fatalf("newAPIHandler failed: %v", err)
	}
	if _, err := h.store.Create(Task{ID: "t1", Status: TaskProcessing, Total: 1}); err != nil {
		t.Fatalf("Create task failed: %v", err)
	}

	srv := httptest.NewServer(h.handler())
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/api/reports/ws/t1"
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, resp, err := websocket.Dial(ctx, wsURL, &websocket.DialOptions{
		Subprotocols: []string{cfg.APIToken},
		HTTPHeader:   http.Header{"Origin": []string{"http://localhost:3000"}},
	})
	if err != nil {
		t.Fatalf("Dial failed: resp=%v err=%v", resp, err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "")

	if got := conn.Subprotocol(); got != cfg.APIToken {
		t.Fatalf("unexpected subprotocol: %q", got)
	}
}

func TestWebSocketAllowsLocalhostAlias(t *testing.T) {
	cfg := Config{
		DataDir:        t.TempDir(),
		AllowedOrigins: []string{"http://localhost:3000"},
		APIToken:       "secret",
	}
	h, err := newAPIHandler(cfg, false)
	if err != nil {
		t.Fatalf("newAPIHandler failed: %v", err)
	}
	if _, err := h.store.Create(Task{ID: "t1", Status: TaskProcessing, Total: 1}); err != nil {
		t.Fatalf("Create task failed: %v", err)
	}

	srv := httptest.NewServer(h.handler())
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/api/reports/ws/t1"
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, resp, err := websocket.Dial(ctx, wsURL, &websocket.DialOptions{
		Subprotocols: []string{cfg.APIToken},
		HTTPHeader:   http.Header{"Origin": []string{"http://127.0.0.1:3000"}},
	})
	if err != nil {
		t.Fatalf("Dial failed: resp=%v err=%v", resp, err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "")

	if got := conn.Subprotocol(); got != cfg.APIToken {
		t.Fatalf("unexpected subprotocol: %q", got)
	}
}
