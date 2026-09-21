package web

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"path/filepath"
	"strings"
)

type apiHandler struct {
	cfg       Config
	lifecycle *TaskLifecycle
}

var errUploadTooLarge = errors.New("upload too large")

func NewHandler(cfg Config) (http.Handler, error) {
	h, err := newAPIHandler(cfg, true)
	if err != nil {
		return nil, err
	}
	return h.handler(), nil
}

func newAPIHandler(cfg Config, startLifecycle bool) (*apiHandler, error) {
	lifecycle, err := NewTaskLifecycle(cfg)
	if err != nil {
		return nil, err
	}
	if startLifecycle {
		if err := lifecycle.Start(context.Background()); err != nil {
			_ = lifecycle.Close(context.Background())
			return nil, err
		}
	}
	return newAPIHandlerWithLifecycle(cfg, lifecycle), nil
}

func newAPIHandlerWithLifecycle(cfg Config, lifecycle *TaskLifecycle) *apiHandler {
	return &apiHandler{cfg: cfg, lifecycle: lifecycle}
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
			w.Header().Set("Access-Control-Allow-Headers", "Authorization,Content-Type,Idempotency-Key")
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

	defer func() {
		if r.MultipartForm != nil {
			_ = r.MultipartForm.RemoveAll()
		}
	}()

	task, err := h.lifecycle.Submit(r.Context(), SubmissionRequest{
		Key: strings.TrimSpace(r.Header.Get("Idempotency-Key")),
		Materialize: func(ctx context.Context) (ReportSubmission, error) {
			if err := ctx.Err(); err != nil {
				return ReportSubmission{}, err
			}
			if h.cfg.MaxUploadBytes > 0 {
				r.Body = http.MaxBytesReader(w, r.Body, h.cfg.MaxUploadBytes)
			}
			if err := r.ParseMultipartForm(32 << 20); err != nil {
				if errors.Is(err, http.ErrBodyReadAfterClose) || strings.Contains(err.Error(), "http: request body too large") {
					return ReportSubmission{}, errUploadTooLarge
				}
				return ReportSubmission{}, invalidSubmission(fmt.Errorf("invalid multipart form: %w", err))
			}
			submission, err := reportSubmissionFromMultipart(r.MultipartForm)
			if err != nil {
				return ReportSubmission{}, invalidSubmission(err)
			}
			return submission, nil
		},
	})
	if err != nil {
		switch {
		case errors.Is(err, ErrTaskCapacity):
			writeErrorCode(w, http.StatusServiceUnavailable, "capacity_exhausted", err.Error())
		case errors.Is(err, ErrIdempotencyConflict):
			writeErrorCode(w, http.StatusConflict, "idempotency_key_conflict", err.Error())
		case errors.Is(err, errUploadTooLarge):
			writeError(w, http.StatusRequestEntityTooLarge, err.Error())
		case errors.Is(err, ErrInvalidSubmission):
			writeError(w, http.StatusBadRequest, err.Error())
		case errors.Is(err, ErrLifecycleClosed):
			writeError(w, http.StatusServiceUnavailable, "service shutting down")
		default:
			writeError(w, http.StatusInternalServerError, err.Error())
		}
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"task_id": task.ID,
		"status":  string(task.Status),
		"total":   task.Total,
		"ws_url":  fmt.Sprintf("/api/reports/ws/%s", task.ID),
	})
}

func reportSubmissionFromMultipart(form *multipart.Form) (ReportSubmission, error) {
	zips := filesForKey(form, "zips")
	if len(zips) == 0 {
		zips = filesForKey(form, "zip")
	}
	if len(zips) == 0 {
		return ReportSubmission{}, errors.New("missing zip files (field: zips)")
	}
	awrs := filesForKey(form, "awrs")
	wdrs := filesForKey(form, "wdrs")
	if len(awrs) != 0 && len(awrs) != len(zips) {
		return ReportSubmission{}, errors.New("invalid awrs: use awr_<index> fields or provide awrs with the same count as zips")
	}
	if len(wdrs) != 0 && len(wdrs) != len(zips) {
		return ReportSubmission{}, errors.New("invalid wdrs: use wdr_<index> fields or provide wdrs with the same count as zips")
	}

	submission := ReportSubmission{Items: make([]ReportItemSubmission, 0, len(zips))}
	for i, zipHeader := range zips {
		itemID := fmt.Sprintf("%d", i+1)
		item := ReportItemSubmission{Zip: submissionFileFromHeader(zipHeader)}

		switch {
		case len(awrs) == len(zips) && awrs[i] != nil:
			awr := submissionFileFromHeader(awrs[i])
			item.AWR = &awr
		default:
			indexedAWRs := filesForKey(form, "awr_"+itemID)
			if len(indexedAWRs) > 1 {
				return ReportSubmission{}, errors.New("Oracle AWR only supports one HTML file per zip")
			}
			if len(indexedAWRs) == 1 {
				awr := submissionFileFromHeader(indexedAWRs[0])
				item.AWR = &awr
			}
		}

		switch {
		case len(wdrs) == len(zips) && wdrs[i] != nil:
			item.WDRs = append(item.WDRs, submissionFileFromHeader(wdrs[i]))
		default:
			for _, header := range filesForKey(form, "wdr_"+itemID) {
				item.WDRs = append(item.WDRs, submissionFileFromHeader(header))
			}
		}
		submission.Items = append(submission.Items, item)
	}
	return submission, nil
}

func submissionFileFromHeader(header *multipart.FileHeader) SubmissionFile {
	if header == nil {
		return SubmissionFile{}
	}
	return SubmissionFile{
		Name: header.Filename,
		Open: func() (io.ReadCloser, error) {
			return header.Open()
		},
	}
}

func filesForKey(form *multipart.Form, key string) []*multipart.FileHeader {
	if form == nil || form.File == nil {
		return nil
	}
	return form.File[key]
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
	snapshot, err := h.lifecycle.Read(r.Context(), taskID)
	if err != nil {
		if errors.Is(err, ErrTaskNotFound) {
			http.NotFound(w, r)
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, snapshot)
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
	zipPath, size, err := h.lifecycle.Download(r.Context(), taskID)
	if err != nil {
		switch {
		case errors.Is(err, ErrTaskNotFound):
			http.NotFound(w, r)
		case errors.Is(err, ErrTaskNotFinished):
			writeError(w, http.StatusConflict, "task not finished")
		default:
			writeError(w, http.StatusInternalServerError, err.Error())
		}
		return
	}

	w.Header().Set("Content-Type", "application/zip")
	w.Header().Set("Content-Length", fmt.Sprintf("%d", size))
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

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]any{"error": message})
}

func writeErrorCode(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, map[string]any{"code": code, "error": message})
}
