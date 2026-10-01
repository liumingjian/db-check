package web

import (
	"net/http"
	"strings"

	"dbcheck/reporter/internal/apierr"
	"dbcheck/reporter/internal/sessions"
	"dbcheck/reporter/internal/users"
)

// authedHandler is a route handler that runs for a signed-in user.
type authedHandler func(w http.ResponseWriter, r *http.Request, u users.User)

// The three access levels. Each loads the user from the store on every
// request, so role and account status changes apply at once. They mirror
// the mock's checks (web/src/lib/api/auth/mock.ts).

// signedIn admits any user with a live session whose account is not
// disabled: current user, own account, resubmit, the forced password change.
func (h *apiHandler) signedIn(next authedHandler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		u, err := h.sessionUser(r)
		if err != nil {
			h.writeAPIError(w, err)
			return
		}
		next(w, r, u)
	}
}

// active admits active users with no forced password change due: every
// console operation.
func (h *apiHandler) active(next authedHandler) http.HandlerFunc {
	return h.signedIn(func(w http.ResponseWriter, r *http.Request, u users.User) {
		if u.Status != users.StatusActive {
			h.writeAPIError(w, apierr.Forbidden("账号尚未启用"))
			return
		}
		if u.MustChangePassword {
			h.writeAPIError(w, apierr.Forbidden("请先修改密码"))
			return
		}
		next(w, r, u)
	})
}

// admin admits active admins.
func (h *apiHandler) admin(next authedHandler) http.HandlerFunc {
	return h.active(func(w http.ResponseWriter, r *http.Request, u users.User) {
		if u.Role != users.RoleAdmin {
			h.writeAPIError(w, apierr.Forbidden("需要管理员权限"))
			return
		}
		next(w, r, u)
	})
}

func (h *apiHandler) sessionUser(r *http.Request) (users.User, error) {
	token := bearerToken(r)
	if token == "" {
		return users.User{}, sessions.ErrNoSession
	}
	ctx := r.Context()
	userID, err := sessions.Resolve(ctx, h.platform.DB, token, h.platform.Now())
	if err != nil {
		return users.User{}, err
	}
	u, err := users.ByID(ctx, h.platform.DB, userID)
	if err != nil {
		return users.User{}, err
	}
	if u.Status == users.StatusDisabled {
		return users.User{}, apierr.Unauthorized(users.DisabledMessage)
	}
	return u, nil
}

func bearerToken(r *http.Request) string {
	value, ok := strings.CutPrefix(strings.TrimSpace(r.Header.Get("Authorization")), "Bearer ")
	if !ok {
		return ""
	}
	return strings.TrimSpace(value)
}

func (h *apiHandler) registerAuthRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /api/auth/sign-in", h.handleSignIn)
	mux.HandleFunc("POST /api/auth/sign-out", h.handleSignOut)
	mux.HandleFunc("GET /api/auth/me", h.signedIn(func(w http.ResponseWriter, _ *http.Request, u users.User) {
		writeJSON(w, http.StatusOK, u)
	}))
}

type sessionResponse struct {
	Token string     `json:"token"`
	User  users.User `json:"user"`
}

func (h *apiHandler) handleSignIn(w http.ResponseWriter, r *http.Request) {
	var body struct{ Username, Password string }
	if err := readJSON(r, &body); err != nil {
		h.writeAPIError(w, err)
		return
	}
	ctx := r.Context()
	u, err := users.Authenticate(ctx, h.platform.DB, body.Username, body.Password)
	if err != nil {
		h.platform.Log.Printf("[WARN] sign-in failed: username=%q remote=%s: %v", body.Username, r.RemoteAddr, err)
		h.writeAPIError(w, err)
		return
	}
	token, err := sessions.Issue(ctx, h.platform.DB, u.ID, h.platform.Now())
	if err != nil {
		h.writeAPIError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, sessionResponse{Token: token, User: u})
}

// handleSignOut ends the presented session. It needs no live session: ending
// an expired or unknown one is a no-op.
func (h *apiHandler) handleSignOut(w http.ResponseWriter, r *http.Request) {
	token := bearerToken(r)
	if token == "" {
		h.writeAPIError(w, sessions.ErrNoSession)
		return
	}
	if err := sessions.Revoke(r.Context(), h.platform.DB, token); err != nil {
		h.writeAPIError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
