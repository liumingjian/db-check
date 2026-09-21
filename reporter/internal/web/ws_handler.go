package web

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"nhooyr.io/websocket"
)

func (h *apiHandler) handleWS(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	taskID, ok := strings.CutPrefix(r.URL.Path, "/api/reports/ws/")
	if !ok || strings.TrimSpace(taskID) == "" {
		http.NotFound(w, r)
		return
	}
	taskID = strings.TrimSpace(taskID)

	if !h.requireWSAuth(w, r) {
		return
	}

	watch, err := h.lifecycle.Watch(r.Context(), taskID)
	if err != nil {
		if errors.Is(err, ErrTaskNotFound) {
			http.NotFound(w, r)
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	defer watch.Close()

	originAllow := newOriginAllowlist(h.cfg.AllowedOrigins)
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		Subprotocols:       []string{h.cfg.APIToken},
		InsecureSkipVerify: originAllow.allowAll,
		OriginPatterns:     originAllow.wsPatterns,
	})
	if err != nil {
		return
	}
	defer conn.Close(websocket.StatusNormalClosure, "")

	ctx := r.Context()

	for _, b := range watch.Logs {
		if err := conn.Write(ctx, websocket.MessageText, b); err != nil {
			return
		}
	}

	snapshot := wsSnapshotMessage{Type: "snapshot", TaskSnapshot: watch.Snapshot}
	if b, err := json.Marshal(snapshot); err == nil {
		if err := conn.Write(ctx, websocket.MessageText, b); err != nil {
			return
		}
	}

	for {
		select {
		case <-watch.Overflow:
			_ = conn.Close(websocket.StatusPolicyViolation, "live updates overflow; reconnect for a task snapshot")
			return
		default:
		}
		select {
		case <-watch.Overflow:
			_ = conn.Close(websocket.StatusPolicyViolation, "live updates overflow; reconnect for a task snapshot")
			return
		case b, ok := <-watch.Updates:
			if !ok {
				return
			}
			if err := conn.Write(ctx, websocket.MessageText, b); err != nil {
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

func (h *apiHandler) requireWSAuth(w http.ResponseWriter, r *http.Request) bool {
	offered := parseWSSubprotocols(r)
	for _, proto := range offered {
		if proto == h.cfg.APIToken {
			return true
		}
	}
	writeError(w, http.StatusUnauthorized, "invalid token")
	return false
}

func parseWSSubprotocols(r *http.Request) []string {
	values := r.Header.Values("Sec-WebSocket-Protocol")
	var out []string
	for _, value := range values {
		parts := strings.Split(value, ",")
		for _, part := range parts {
			trimmed := strings.TrimSpace(part)
			if trimmed != "" {
				out = append(out, trimmed)
			}
		}
	}
	return out
}
