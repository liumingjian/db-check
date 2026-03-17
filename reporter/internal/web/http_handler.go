package web

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type apiHandler struct {
	cfg Config

	store *TaskStore
	hub   *taskHub

	queue chan queuedTask
	once  sync.Once
}

type queuedTask struct {
	TaskID string
	Items  []ItemInput
}

func NewHandler(cfg Config) (http.Handler, error) {
	h, err := newAPIHandler(cfg, true)
	if err != nil {
		return nil, err
	}
	return h.handler(), nil
}

func newAPIHandler(cfg Config, startWorker bool) (*apiHandler, error) {
	store, err := NewTaskStore(cfg.DataDir)
	if err != nil {
		return nil, err
	}

	h := &apiHandler{
		cfg:   cfg,
		store: store,
		hub:   newTaskHub(cfg.LogReplayLines),
		queue: make(chan queuedTask, 32),
	}
	if startWorker {
		h.startWorker()
	}

	return h, nil
}

func (h *apiHandler) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/reports/generate", h.handleGenerate)
	mux.HandleFunc("/api/reports/status/", h.handleStatus)
	mux.HandleFunc("/api/reports/download/", h.handleDownload)
	mux.HandleFunc("/api/reports/ws/", h.handleWS)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	})

	return withCORS(h.cfg.AllowedOrigins, mux)
}

func withCORS(allowedOrigins []string, next http.Handler) http.Handler {
	allow := newOriginAllowlist(allowedOrigins)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := strings.TrimSpace(r.Header.Get("Origin"))
		if origin != "" {
			if !allow.allows(origin) {
				writeError(w, http.StatusForbidden, "origin not allowed")
				return
			}
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Vary", "Origin")
			w.Header().Set("Access-Control-Allow-Methods", "GET,POST,OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Authorization,Content-Type")
			w.Header().Set("Access-Control-Max-Age", "600")
		}

		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (h *apiHandler) handleGenerate(w http.ResponseWriter, r *http.Request) {
	if !h.requireAuth(w, r) {
		return
	}
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	if h.cfg.MaxUploadBytes > 0 {
		r.Body = http.MaxBytesReader(w, r.Body, h.cfg.MaxUploadBytes)
	}
	if err := r.ParseMultipartForm(32 << 20); err != nil {
		if errors.Is(err, http.ErrBodyReadAfterClose) || strings.Contains(err.Error(), "http: request body too large") {
			writeError(w, http.StatusRequestEntityTooLarge, "upload too large")
			return
		}
		writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid multipart form: %v", err))
		return
	}

	zips := filesForKey(r.MultipartForm, "zips")
	if len(zips) == 0 {
		zips = filesForKey(r.MultipartForm, "zip")
	}
	if len(zips) == 0 {
		writeError(w, http.StatusBadRequest, "missing zip files (field: zips)")
		return
	}
	awrs := filesForKey(r.MultipartForm, "awrs")
	if hasAnyWDR(r.MultipartForm) {
		// Web service explicitly does not support WDR uploads.
		writeError(w, http.StatusBadRequest, "WDR is not supported")
		return
	}
	if len(awrs) != 0 && len(awrs) != len(zips) {
		writeError(w, http.StatusBadRequest, "invalid awrs: use awr_<index> fields or provide awrs with the same count as zips")
		return
	}

	taskID, err := newTaskID()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to allocate task id")
		return
	}

	task, err := h.store.Create(Task{
		ID:        taskID,
		Status:    TaskProcessing,
		Total:     len(zips),
		Completed: 0,
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	taskDir := h.store.taskDir(task.ID)
	uploadsDir := filepath.Join(taskDir, "uploads")
	if err := os.MkdirAll(uploadsDir, 0o755); err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("create uploads dir failed: %v", err))
		return
	}

	items := make([]ItemInput, 0, len(zips))
	for i, zipHeader := range zips {
		itemID := fmt.Sprintf("%d", i+1)
		zipPath, name, err := saveUpload(uploadsDir, "zip", itemID, zipHeader)
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}

		var awrPath string
		switch {
		case len(awrs) == len(zips) && awrs[i] != nil:
			path, _, err := saveUpload(uploadsDir, "awr", itemID, awrs[i])
			if err != nil {
				writeError(w, http.StatusBadRequest, err.Error())
				return
			}
			awrPath = path
		default:
			if hdr := firstFileForKey(r.MultipartForm, "awr_"+itemID); hdr != nil {
				path, _, err := saveUpload(uploadsDir, "awr", itemID, hdr)
				if err != nil {
					writeError(w, http.StatusBadRequest, err.Error())
					return
				}
				awrPath = path
			}
		}
		items = append(items, ItemInput{
			ID:      itemID,
			Name:    name,
			ZipPath: zipPath,
			AWRPath: awrPath,
		})
	}

	h.enqueue(queuedTask{TaskID: task.ID, Items: items})

	writeJSON(w, http.StatusOK, map[string]any{
		"task_id": task.ID,
		"status":  "processing",
		"total":   task.Total,
		"ws_url":  fmt.Sprintf("/api/reports/ws/%s", task.ID),
	})
}

func (h *apiHandler) handleStatus(w http.ResponseWriter, r *http.Request) {
	if !h.requireAuth(w, r) {
		return
	}
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	taskID, ok := strings.CutPrefix(r.URL.Path, "/api/reports/status/")
	if !ok || strings.TrimSpace(taskID) == "" {
		http.NotFound(w, r)
		return
	}
	taskID = strings.TrimSpace(taskID)
	task, err := h.store.Load(taskID)
	if err != nil {
		if errors.Is(err, ErrTaskNotFound) {
			http.NotFound(w, r)
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	resp := map[string]any{
		"task_id":      task.ID,
		"status":       string(task.Status),
		"total":        task.Total,
		"completed":    task.Completed,
		"current_file": task.CurrentFile,
	}
	if task.Status == TaskDone {
		resp["download_url"] = fmt.Sprintf("/api/reports/download/%s", task.ID)
	}
	if task.Error != "" {
		resp["error"] = task.Error
	}

	writeJSON(w, http.StatusOK, resp)
}

func (h *apiHandler) handleDownload(w http.ResponseWriter, r *http.Request) {
	if !h.requireAuth(w, r) {
		return
	}
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	taskID, ok := strings.CutPrefix(r.URL.Path, "/api/reports/download/")
	if !ok || strings.TrimSpace(taskID) == "" {
		http.NotFound(w, r)
		return
	}
	taskID = strings.TrimSpace(taskID)
	task, err := h.store.Load(taskID)
	if err != nil {
		if errors.Is(err, ErrTaskNotFound) {
			http.NotFound(w, r)
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if task.Status != TaskDone {
		writeError(w, http.StatusConflict, "task not finished")
		return
	}

	zipPath := filepath.Join(h.store.taskDir(task.ID), fmt.Sprintf("reports-%s.zip", task.ID))
	info, err := os.Stat(zipPath)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("result zip not found: %v", err))
		return
	}

	w.Header().Set("Content-Type", "application/zip")
	w.Header().Set("Content-Length", fmt.Sprintf("%d", info.Size()))
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", filepath.Base(zipPath)))
	http.ServeFile(w, r, zipPath)
}

func (h *apiHandler) requireAuth(w http.ResponseWriter, r *http.Request) bool {
	value := strings.TrimSpace(r.Header.Get("Authorization"))
	const prefix = "Bearer "
	if !strings.HasPrefix(value, prefix) {
		writeError(w, http.StatusUnauthorized, "missing bearer token")
		return false
	}
	token := strings.TrimSpace(strings.TrimPrefix(value, prefix))
	if token == "" || token != h.cfg.APIToken {
		writeError(w, http.StatusUnauthorized, "invalid token")
		return false
	}
	return true
}

func (h *apiHandler) enqueue(task queuedTask) {
	select {
	case h.queue <- task:
	default:
		// Queue full: drop on the floor but keep the task record.
		// This should be rare under single-worker design.
	}
}

func (h *apiHandler) startWorker() {
	h.once.Do(func() {
		h.resumeTasks()
		h.startRetentionCleanup()
		go h.workerLoop()
	})
}

func (h *apiHandler) workerLoop() {
	exe, err := os.Executable()
	if err != nil {
		return
	}
	pipeline := NewPipeline(exe, h.cfg.PythonBin)

	for task := range h.queue {
		h.runTask(pipeline, task)
	}
}

func (h *apiHandler) startRetentionCleanup() {
	if h.cfg.RetentionTTL <= 0 {
		return
	}
	interval := time.Hour
	if h.cfg.RetentionTTL < interval {
		interval = h.cfg.RetentionTTL / 2
	}
	if interval < time.Minute {
		interval = time.Minute
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for range ticker.C {
			_, _ = cleanupExpiredTasks(h.store, h.cfg.RetentionTTL, time.Now)
		}
	}()
}

func (h *apiHandler) runTask(p *Pipeline, queued queuedTask) {
	task, err := h.store.Load(queued.TaskID)
	if err != nil {
		return
	}
	task.Status = TaskProcessing
	task.Total = len(queued.Items)
	task.Error = ""

	// Rebuild Items in the input order, preserving previous status/error/report paths.
	prev := make(map[string]TaskItem, len(task.Items))
	for _, item := range task.Items {
		prev[item.ID] = item
	}
	task.Items = make([]TaskItem, 0, len(queued.Items))
	for _, input := range queued.Items {
		item, ok := prev[input.ID]
		if !ok {
			item = TaskItem{ID: input.ID, Status: string(TaskQueued)}
		}
		if item.Name == "" {
			item.Name = input.Name
		}
		task.Items = append(task.Items, item)
	}
	task.Completed = countProcessed(task.Items)
	task.CurrentFile = ""
	_, _ = h.store.Update(task)

	taskDir := h.store.taskDir(task.ID)
	for i, input := range queued.Items {
		// Resume: skip items already completed in a previous run.
		if i < len(task.Items) {
			if task.Items[i].Status == string(ItemDone) || task.Items[i].Status == string(ItemFailed) {
				continue
			}
		}

		task.CurrentFile = input.Name
		_, _ = h.store.Update(task)

		h.hub.emitLog(task.ID, "info", fmt.Sprintf("开始处理 %s", input.Name))
		result := p.runOne(taskDir, input, func(itemID string, ev LogEvent) {
			level := "info"
			if ev.Stream == LogStderr {
				level = "error"
			}
			msg := ev.Line
			if input.Name != "" {
				msg = fmt.Sprintf("[%s] %s", input.Name, msg)
			}
			h.hub.emitLog(task.ID, level, msg)
		})
		if i < len(task.Items) && task.Items[i].ID == result.ID {
			task.Items[i].Status = string(result.Status)
			task.Items[i].Error = result.Error
			task.Items[i].ReportDocx = result.ReportDocx
		}
		task.Completed = countProcessed(task.Items)
		_, _ = h.store.Update(task)

		h.hub.emitProgress(task.ID, task.Completed, task.Total, task.CurrentFile)
	}

	// Build download zip from all completed items (including previous runs).
	results := make([]ItemResult, 0, len(task.Items))
	for _, item := range task.Items {
		switch item.Status {
		case string(ItemDone):
			results = append(results, ItemResult{ID: item.ID, Status: ItemDone, ReportDocx: item.ReportDocx})
		case string(ItemFailed):
			results = append(results, ItemResult{ID: item.ID, Status: ItemFailed, Error: item.Error})
		}
	}

	zipPath := filepath.Join(taskDir, fmt.Sprintf("reports-%s.zip", task.ID))
	if err := buildResultZip(zipPath, results, queued.Items); err != nil {
		task.Status = TaskFailed
		task.Error = err.Error()
		task.CurrentFile = ""
		_, _ = h.store.Update(task)
		h.hub.emitError(task.ID, err.Error())
		return
	}

	task.Status = TaskDone
	task.CurrentFile = ""
	_, _ = h.store.Update(task)
	h.hub.emitDone(task.ID, fmt.Sprintf("/api/reports/download/%s", task.ID))
}

func countProcessed(items []TaskItem) int {
	n := 0
	for _, item := range items {
		if item.Status == string(ItemDone) || item.Status == string(ItemFailed) {
			n++
		}
	}
	return n
}

func newTaskID() (string, error) {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf[:]), nil
}

func filesForKey(form *multipart.Form, key string) []*multipart.FileHeader {
	if form == nil {
		return nil
	}
	if form.File == nil {
		return nil
	}
	return form.File[key]
}

func firstFileForKey(form *multipart.Form, key string) *multipart.FileHeader {
	files := filesForKey(form, key)
	if len(files) == 0 {
		return nil
	}
	return files[0]
}

func hasAnyWDR(form *multipart.Form) bool {
	if form == nil || form.File == nil {
		return false
	}
	if len(form.File["wdrs"]) > 0 || len(form.File["wdr"]) > 0 {
		return true
	}
	for key := range form.File {
		if strings.HasPrefix(key, "wdr_") {
			return true
		}
	}
	return false
}

func saveUpload(dir string, kind string, itemID string, header *multipart.FileHeader) (string, string, error) {
	if header == nil {
		return "", "", errors.New("missing file")
	}
	// Only keep the base name to avoid client-provided paths.
	name := filepath.Base(header.Filename)
	if strings.TrimSpace(name) == "" {
		return "", "", errors.New("invalid filename")
	}

	src, err := header.Open()
	if err != nil {
		return "", "", fmt.Errorf("open upload failed: %w", err)
	}
	defer src.Close()

	ext := strings.ToLower(filepath.Ext(name))
	if kind == "zip" && ext != ".zip" {
		return "", "", fmt.Errorf("invalid zip filename: %q", name)
	}
	if kind == "awr" && ext != ".html" && ext != ".htm" {
		return "", "", fmt.Errorf("invalid awr filename: %q", name)
	}

	dstName := fmt.Sprintf("%s-%s-%s", kind, itemID, name)
	dstPath := filepath.Join(dir, dstName)
	dst, err := os.OpenFile(dstPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return "", "", fmt.Errorf("save upload failed: %w", err)
	}
	defer dst.Close()
	if _, err := io.Copy(dst, src); err != nil {
		return "", "", fmt.Errorf("save upload failed: %w", err)
	}
	return dstPath, name, nil
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]any{"error": message})
}
