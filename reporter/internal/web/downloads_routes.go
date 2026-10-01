package web

import (
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"dbcheck/reporter/internal/apierr"
	"dbcheck/reporter/internal/downloads"
	"dbcheck/reporter/internal/releases"
	"dbcheck/reporter/internal/users"
)

func (h *apiHandler) registerDownloadRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /api/releases/{version}/packages/{platform}", h.active(h.handleDownloadPackage))
	mux.HandleFunc("GET /api/downloads", h.admin(h.handleDownloadRecords))
}

// handleDownloadPackage streams one release package as a .zip attachment,
// after downloads.Start has authorized it and written the download record.
func (h *apiHandler) handleDownloadPackage(w http.ResponseWriter, r *http.Request, u users.User) {
	version, platform := r.PathValue("version"), releases.Platform(r.PathValue("platform"))
	f, pkg, err := downloads.Start(r.Context(), h.platform.DB, h.cfg.DataDir, u, version, platform, h.platform.Now())
	if err != nil {
		h.writeAPIError(w, err)
		return
	}
	defer f.Close()
	w.Header().Set("Content-Type", "application/zip")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", pkg.FileName))
	w.Header().Set("Content-Length", strconv.FormatInt(pkg.Size, 10))
	if _, err := io.Copy(w, f); err != nil {
		h.platform.Log.Printf("[WARN] download of %s by %s cut short: %v", pkg.FileName, u.Username, err)
	}
}

// handleDownloadRecords lists download records newest first, narrowed by
// the query parameters userId, version, from (inclusive), and to
// (exclusive), the times in RFC 3339.
func (h *apiHandler) handleDownloadRecords(w http.ResponseWriter, r *http.Request, _ users.User) {
	query := r.URL.Query()
	filter := downloads.Filter{UserID: query.Get("userId"), Version: query.Get("version")}
	var err error
	if filter.From, err = queryTime(query.Get("from")); err == nil {
		filter.To, err = queryTime(query.Get("to"))
	}
	if err != nil {
		h.writeAPIError(w, err)
		return
	}
	list, err := downloads.List(r.Context(), h.platform.DB, filter)
	if err != nil {
		h.writeAPIError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, list)
}

// queryTime reads an optional RFC 3339 time; empty is the zero time.
func queryTime(value string) (time.Time, error) {
	if value == "" {
		return time.Time{}, nil
	}
	t, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}, apierr.Invalid("时间格式错误：" + value)
	}
	return t, nil
}
