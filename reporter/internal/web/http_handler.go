package web

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"dbcheck/reporter/internal/apierr"
)

type apiHandler struct {
	cfg      Config
	platform Platform

	hub     *taskHub
	reports *reportLifecycle
}

// NewHandler builds every API route over the given platform and starts the
// report worker.
func NewHandler(cfg Config, p Platform) (http.Handler, error) {
	h, err := newAPIHandler(cfg, p, true)
	if err != nil {
		return nil, err
	}
	return h.handler(), nil
}

func newAPIHandler(cfg Config, p Platform, startWorker bool) (*apiHandler, error) {
	p = p.withDefaults()
	hub := newTaskHub(cfg.LogReplayLines)
	lifecycle, err := newReportLifecycle(cfg, p, hub)
	if err != nil {
		return nil, err
	}
	h := &apiHandler{cfg: cfg, platform: p, hub: hub, reports: lifecycle}
	if startWorker {
		lifecycle.start()
	}
	return h, nil
}

func (h *apiHandler) handler() http.Handler {
	mux := http.NewServeMux()
	h.registerReportRoutes(mux)
	h.registerPlatformRoutes(mux)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		h.writeAPIError(w, apierr.NotFound("接口不存在"))
	})

	return withCORS(h.cfg.AllowedOrigins, mux)
}

func withCORS(allowedOrigins []string, next http.Handler) http.Handler {
	allow := newOriginAllowlist(allowedOrigins)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := strings.TrimSpace(r.Header.Get("Origin"))
		if origin != "" {
			if !allow.allows(origin) {
				writeJSON(w, http.StatusForbidden, apierr.Forbidden("来源不在允许列表中"))
				return
			}
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Vary", "Origin")
			w.Header().Set("Access-Control-Allow-Methods", "GET,POST,PUT,PATCH,DELETE,OPTIONS")
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

func newTaskID() (string, error) {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf[:]), nil
}

func filesForKey(form *multipart.Form, key string) []*multipart.FileHeader {
	if form == nil || form.File == nil {
		return nil
	}
	return form.File[key]
}

// saveUpload writes an uploaded file into dir as <kind>-<itemID>-<name> and
// returns its path and original base name. A ZIP must end in .zip, an AWR
// or WDR in .html or .htm.
func saveUpload(dir string, kind string, itemID string, header *multipart.FileHeader) (string, string, error) {
	// Only keep the base name to avoid client-provided paths.
	name := filepath.Base(header.Filename)
	if strings.TrimSpace(name) == "" || name == "." || name == string(filepath.Separator) {
		return "", "", apierr.Invalid("上传文件缺少文件名")
	}
	ext := strings.ToLower(filepath.Ext(name))
	if kind == "zip" && ext != ".zip" {
		return "", "", apierr.Invalid(name + "：不是 ZIP 文件")
	}
	if (kind == "awr" || kind == "wdr") && ext != ".html" && ext != ".htm" {
		return "", "", apierr.Invalid(fmt.Sprintf("%s：%s 文件必须是 HTML", name, strings.ToUpper(kind)))
	}

	src, err := header.Open()
	if err != nil {
		return "", "", fmt.Errorf("open upload failed: %w", err)
	}
	defer src.Close()
	dstPath := filepath.Join(dir, fmt.Sprintf("%s-%s-%s", kind, itemID, name))
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
