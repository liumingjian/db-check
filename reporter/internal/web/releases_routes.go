package web

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"dbcheck/reporter/internal/apierr"
	"dbcheck/reporter/internal/releases"
	"dbcheck/reporter/internal/store"
	"dbcheck/reporter/internal/users"
)

func (h *apiHandler) registerReleaseRoutes(mux routeMux) {
	mux.HandleFunc("GET /api/releases", h.active(h.handleListReleases))
	mux.HandleFunc("POST /api/releases/{version}/{action}", h.admin(h.handleReleaseAction))
	mux.HandleFunc("POST /api/ci/releases", h.handlePublish)
}

func (h *apiHandler) handleListReleases(w http.ResponseWriter, r *http.Request, u users.User) {
	list, err := releases.List(r.Context(), h.platform.DB, u.Role == users.RoleAdmin)
	if err != nil {
		h.writeAPIError(w, err)
		return
	}
	if list == nil {
		list = []releases.Release{}
	}
	writeJSON(w, http.StatusOK, list)
}

// handleReleaseAction applies promote, deprecate, revoke ({"reason"}), or
// restore.
func (h *apiHandler) handleReleaseAction(w http.ResponseWriter, r *http.Request, _ users.User) {
	action := releases.Action(r.PathValue("action"))
	var body struct{ Reason string }
	if action == releases.ActionRevoke {
		if err := readJSON(r, &body); err != nil {
			h.writeAPIError(w, err)
			return
		}
	}
	ctx := r.Context()
	err := h.platform.DB.Tx(ctx, func(tx store.Querier) error {
		return releases.ChangeStatus(ctx, tx, r.PathValue("version"), action, body.Reason)
	})
	if err != nil {
		h.writeAPIError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// handlePublish is the CI publish API (ADR 0002): one multipart request with
// a `metadata` JSON part and one `.zip` file part per platform, the part
// named by the platform. It answers 201 with the new release, or 200 with
// the stored one for an identical re-publish.
func (h *apiHandler) handlePublish(w http.ResponseWriter, r *http.Request) {
	if err := h.checkPublishCredential(r); err != nil {
		h.writeAPIError(w, err)
		return
	}
	if h.cfg.MaxUploadBytes > 0 {
		r.Body = http.MaxBytesReader(w, r.Body, h.cfg.MaxUploadBytes)
	}
	upload, err := releases.NewUpload(h.cfg.DataDir)
	if err != nil {
		h.writeAPIError(w, err)
		return
	}
	defer upload.Discard()
	meta, err := readPublishRequest(r, upload)
	if err != nil {
		h.writeAPIError(w, err)
		return
	}
	release, created, err := releases.Publish(r.Context(), h.platform.DB, upload, meta)
	if err != nil {
		h.writeAPIError(w, err)
		return
	}
	status := http.StatusOK
	if created {
		status = http.StatusCreated
		h.platform.Log.Printf("[INFO] published collector release %s (%s, %s)", release.Version, release.Status, release.Commit)
	}
	writeJSON(w, status, release)
}

// checkPublishCredential compares the bearer token with DBCHECK_PUBLISH_TOKEN
// in constant time. User sessions are never accepted here.
func (h *apiHandler) checkPublishCredential(r *http.Request) error {
	if h.cfg.PublishToken == "" {
		return apierr.Forbidden("发布接口未启用：服务器未配置 DBCHECK_PUBLISH_TOKEN")
	}
	if subtle.ConstantTimeCompare([]byte(bearerToken(r)), []byte(h.cfg.PublishToken)) != 1 {
		h.platform.Log.Printf("[WARN] publish refused: bad credential, remote=%s", r.RemoteAddr)
		return apierr.Unauthorized("发布凭证无效")
	}
	return nil
}

// readPublishRequest streams the multipart parts: the metadata is decoded,
// every file part is staged in upload.
func readPublishRequest(r *http.Request, upload *releases.Upload) (releases.Metadata, error) {
	mr, err := r.MultipartReader()
	if err != nil {
		return releases.Metadata{}, apierr.Invalid("发布请求必须是 multipart/form-data")
	}
	var meta *releases.Metadata
	for {
		part, err := mr.NextPart()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return releases.Metadata{}, publishReadError(err)
		}
		switch {
		case part.FormName() == "metadata":
			meta = &releases.Metadata{}
			if err := json.NewDecoder(part).Decode(meta); err != nil {
				return releases.Metadata{}, apierr.Invalid("发布元数据不是有效的 JSON")
			}
		case part.FileName() != "":
			if err := upload.Add(releases.Platform(part.FormName()), part.FileName(), part); err != nil {
				return releases.Metadata{}, publishReadError(err)
			}
		default:
			return releases.Metadata{}, apierr.Invalid("发布请求包含未知字段 " + part.FormName())
		}
	}
	if meta == nil {
		return releases.Metadata{}, apierr.Invalid("缺少发布元数据 (metadata)")
	}
	return *meta, nil
}

func publishReadError(err error) error {
	var tooLarge *http.MaxBytesError
	if errors.As(err, &tooLarge) {
		return apierr.Invalid("发布请求过大")
	}
	return err
}
