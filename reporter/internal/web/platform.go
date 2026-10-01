package web

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"time"

	"dbcheck/reporter/internal/apierr"
	"dbcheck/reporter/internal/store"
)

// Platform is what the platform routes need besides Config. Run builds the
// production one; the contract test server builds its own with a pinned
// clock.
type Platform struct {
	DB  *store.DB
	Now func() time.Time
	Log *log.Logger
}

func (p Platform) withDefaults() Platform {
	if p.Now == nil {
		p.Now = time.Now
	}
	if p.Log == nil {
		p.Log = log.Default()
	}
	return p
}

// registerPlatformRoutes mounts every platform domain's routes. Each domain
// keeps its routes in its own <domain>_routes.go and adds one line here.
// Patterns use Go 1.22 method syntax ("GET /api/users/{id}").
func (h *apiHandler) registerPlatformRoutes(mux *http.ServeMux) {
	h.registerAuthRoutes(mux)
	h.registerUsersRoutes(mux)
	h.registerReleaseRoutes(mux)
}

// writeAPIError answers with the error envelope (package apierr). Errors
// that are not *apierr.Error are logged and answered as `failed`, so no
// internal detail reaches the console.
func (h *apiHandler) writeAPIError(w http.ResponseWriter, err error) {
	var apiErr *apierr.Error
	if !errors.As(err, &apiErr) {
		h.platform.Log.Printf("[ERROR] %v", err)
		apiErr = apierr.Failed("服务器内部错误，请稍后重试")
	}
	writeJSON(w, apiErr.Status, apiErr)
}

// readJSON decodes a request body, answering `invalid` when it is malformed.
func readJSON(r *http.Request, into any) error {
	if err := json.NewDecoder(r.Body).Decode(into); err != nil {
		return apierr.Invalid("请求格式错误")
	}
	return nil
}
