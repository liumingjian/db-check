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

	task, err := h.store.Load(taskID)
	if err != nil {
		if errors.Is(err, ErrTaskNotFound) {
			http.NotFound(w, r)
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

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

	lastSeq, logs := h.hub.snapshot(taskID)
	for _, b := range logs {
		if err := conn.Write(ctx, websocket.MessageText, b); err != nil {
			return
		}
	}

	// Send a progress snapshot so a reconnecting client can recover state.
	progress := wsProgressMessage{
		Type:        "progress",
		Completed:   task.Completed,
		Total:       task.Total,
		CurrentFile: task.CurrentFile,
	}
	if b, err := json.Marshal(withSeq(&progress, lastSeq)); err == nil {
		if err := conn.Write(ctx, websocket.MessageText, b); err != nil {
			return
		}
	}

	if task.Status == TaskDone {
		done := wsDoneMessage{Type: "done", DownloadURL: "/api/reports/download/" + task.ID}
		if b, err := json.Marshal(withSeq(&done, lastSeq)); err == nil {
			_ = conn.Write(ctx, websocket.MessageText, b)
		}
	}
	if task.Status == TaskFailed && task.Error != "" {
		errMsg := wsErrorMessage{Type: "error", Message: task.Error}
		if b, err := json.Marshal(withSeq(&errMsg, lastSeq)); err == nil {
			_ = conn.Write(ctx, websocket.MessageText, b)
		}
	}

	ch, cancel := h.hub.subscribe(taskID)
	defer cancel()

	for {
		select {
		case b, ok := <-ch:
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
