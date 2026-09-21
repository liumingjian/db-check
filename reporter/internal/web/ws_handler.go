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

	// Send a progress snapshot so a reconnecting client can recover state.
	progress := wsProgressMessage{
		Type:        "progress",
		Completed:   watch.Task.Completed,
		Total:       watch.Task.Total,
		CurrentFile: watch.Task.CurrentFile,
	}
	if b, err := json.Marshal(withSeq(&progress, watch.LastSequence)); err == nil {
		if err := conn.Write(ctx, websocket.MessageText, b); err != nil {
			return
		}
	}

	if watch.Task.Status == TaskDone {
		done := wsDoneMessage{Type: "done", DownloadURL: "/api/reports/download/" + watch.Task.ID}
		if b, err := json.Marshal(withSeq(&done, watch.LastSequence)); err == nil {
			_ = conn.Write(ctx, websocket.MessageText, b)
		}
	}
	if watch.Task.Status == TaskFailed && watch.Task.Error != "" {
		errMsg := wsErrorMessage{Type: "error", Message: watch.Task.Error}
		if b, err := json.Marshal(withSeq(&errMsg, watch.LastSequence)); err == nil {
			_ = conn.Write(ctx, websocket.MessageText, b)
		}
	}

	for {
		select {
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
