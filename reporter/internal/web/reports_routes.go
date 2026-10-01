package web

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"dbcheck/reporter/internal/apierr"
	"dbcheck/reporter/internal/reports"
	"dbcheck/reporter/internal/users"

	"nhooyr.io/websocket"
)

// registerReportRoutes mounts report generation. Every route needs an
// active user; a task is visible to its submitter and to admins, and is
// not_found for anyone else.
func (h *apiHandler) registerReportRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /api/reports/generate", h.active(h.handleGenerate))
	mux.HandleFunc("GET /api/reports/status/{id}", h.active(h.handleStatus))
	mux.HandleFunc("GET /api/reports/download/{id}", h.active(h.handleDownload))
	mux.HandleFunc("GET /api/reports/ws/{id}", h.handleWS)
}

var errUploadTooLarge = &apierr.Error{Status: http.StatusRequestEntityTooLarge, Code: apierr.CodeFailed, Message: "上传文件超过大小上限"}

func (h *apiHandler) handleGenerate(w http.ResponseWriter, r *http.Request, u users.User) {
	if h.cfg.MaxUploadBytes > 0 {
		r.Body = http.MaxBytesReader(w, r.Body, h.cfg.MaxUploadBytes)
	}
	if err := r.ParseMultipartForm(32 << 20); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			h.writeAPIError(w, errUploadTooLarge)
			return
		}
		h.writeAPIError(w, apierr.Invalid("请至少上传一个 ZIP 文件"))
		return
	}
	defer r.MultipartForm.RemoveAll()
	task, err := h.reports.submit(r.Context(), u.ID, r.MultipartForm)
	if err != nil {
		h.writeAPIError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"task_id": task.ID,
		"status":  task.Status.Wire(),
		"total":   len(task.Items),
		"ws_url":  "/api/reports/ws/" + task.ID,
	})
}

// visibleTask loads a task the user may see.
func (h *apiHandler) visibleTask(ctx context.Context, id string, u users.User) (reports.Task, error) {
	task, err := reports.Get(ctx, h.platform.DB, id)
	if err != nil {
		return reports.Task{}, err
	}
	if !task.VisibleTo(u) {
		return reports.Task{}, reports.ErrNotFound
	}
	return task, nil
}

func (h *apiHandler) handleStatus(w http.ResponseWriter, r *http.Request, u users.User) {
	task, err := h.visibleTask(r.Context(), r.PathValue("id"), u)
	if err != nil {
		h.writeAPIError(w, err)
		return
	}
	resp := map[string]any{
		"task_id":      task.ID,
		"status":       task.Status.Wire(),
		"total":        len(task.Items),
		"completed":    finishedItems(task.Items),
		"current_file": currentItem(task.Items),
	}
	if task.Status == reports.StatusDone {
		resp["download_url"] = "/api/reports/download/" + task.ID
	}
	if task.Error != "" {
		resp["error"] = task.Error
	}
	writeJSON(w, http.StatusOK, resp)
}

func (h *apiHandler) handleDownload(w http.ResponseWriter, r *http.Request, u users.User) {
	task, err := h.visibleTask(r.Context(), r.PathValue("id"), u)
	if err != nil {
		h.writeAPIError(w, err)
		return
	}
	if task.Status != reports.StatusDone {
		h.writeAPIError(w, apierr.Conflict("报告尚未生成完成"))
		return
	}
	zipPath := resultZipPath(h.reports.taskDir(task.ID), task.ID)
	info, err := os.Stat(zipPath)
	if err != nil {
		h.writeAPIError(w, fmt.Errorf("result zip of task %s: %w", task.ID, err))
		return
	}
	w.Header().Set("Content-Type", "application/zip")
	w.Header().Set("Content-Length", fmt.Sprintf("%d", info.Size()))
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", filepath.Base(zipPath)))
	http.ServeFile(w, r, zipPath)
}

// handleWS streams a task's events. Browsers cannot set headers on a
// WebSocket, so the session token arrives as the subprotocol and is echoed
// back as the accepted one.
func (h *apiHandler) handleWS(w http.ResponseWriter, r *http.Request) {
	var token string
	if offered := parseWSSubprotocols(r); len(offered) > 0 {
		token = offered[0]
	}
	authed := r.Clone(r.Context())
	authed.Header.Set("Authorization", "Bearer "+token)
	h.active(func(w http.ResponseWriter, _ *http.Request, u users.User) {
		h.streamTask(w, r, u, token)
	})(w, authed)
}

func (h *apiHandler) streamTask(w http.ResponseWriter, r *http.Request, u users.User, token string) {
	ctx := r.Context()
	task, err := h.visibleTask(ctx, r.PathValue("id"), u)
	if err != nil {
		h.writeAPIError(w, err)
		return
	}
	originAllow := newOriginAllowlist(h.cfg.AllowedOrigins)
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		Subprotocols:       []string{token},
		InsecureSkipVerify: originAllow.allowAll,
		OriginPatterns:     originAllow.wsPatterns,
	})
	if err != nil {
		return
	}
	defer conn.Close(websocket.StatusNormalClosure, "")
	// Clients only listen; reading in the background answers their close.
	ctx = conn.CloseRead(ctx)

	// Subscribe before reloading the task: a state saved after the reload
	// arrives on ch, so the end of the task cannot fall in between.
	lastSeq, logs, ch, cancel := h.hub.subscribeWithReplay(task.ID)
	defer cancel()
	if task, err = reports.Get(ctx, h.platform.DB, task.ID); err != nil {
		return
	}
	snapshot := append(logs, mustJSON(withSeq(&wsProgressMessage{
		Type: "progress", Completed: finishedItems(task.Items), Total: len(task.Items), CurrentFile: currentItem(task.Items),
	}, lastSeq)))
	switch task.Status {
	case reports.StatusDone:
		snapshot = append(snapshot, mustJSON(withSeq(&wsDoneMessage{Type: "done", DownloadURL: "/api/reports/download/" + task.ID}, lastSeq)))
	case reports.StatusFailed:
		snapshot = append(snapshot, mustJSON(withSeq(&wsErrorMessage{Type: "error", Message: task.Error}, lastSeq)))
	}
	for _, b := range snapshot {
		if err := conn.Write(ctx, websocket.MessageText, b); err != nil {
			return
		}
	}
	if task.Status.Finished() {
		return
	}
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

func mustJSON(v any) []byte {
	b, _ := json.Marshal(v)
	return b
}

func parseWSSubprotocols(r *http.Request) []string {
	var out []string
	for _, value := range r.Header.Values("Sec-WebSocket-Protocol") {
		for _, part := range strings.Split(value, ",") {
			if trimmed := strings.TrimSpace(part); trimmed != "" {
				out = append(out, trimmed)
			}
		}
	}
	return out
}
