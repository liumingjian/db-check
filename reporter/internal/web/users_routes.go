package web

import (
	"context"
	"net/http"
	"time"

	"dbcheck/reporter/internal/sessions"
	"dbcheck/reporter/internal/store"
	"dbcheck/reporter/internal/users"
)

// registerUsersRoutes mounts registration, resubmission, the own profile,
// and the admin's account operations (package users holds the rules).
func (h *apiHandler) registerUsersRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /api/auth/register", h.handleRegister)
	mux.HandleFunc("POST /api/auth/resubmit", h.signedIn(h.handleResubmit))
	mux.HandleFunc("GET /api/users/me", h.signedIn(func(w http.ResponseWriter, r *http.Request, u users.User) {
		h.answerProfile(w, r, func(ctx context.Context, tx store.Querier, _ time.Time) (users.Profile, error) {
			return users.ProfileByID(ctx, tx, u.ID)
		})
	}))
	mux.HandleFunc("GET /api/users", h.admin(func(w http.ResponseWriter, r *http.Request, _ users.User) {
		list, err := users.List(r.Context(), h.platform.DB)
		if err != nil {
			h.writeAPIError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, list)
	}))

	type action func(ctx context.Context, q store.Querier, admin users.User, id string, now time.Time) (users.Profile, error)
	type reasoned func(ctx context.Context, q store.Querier, admin users.User, id, reason string, now time.Time) (users.Profile, error)
	for name, act := range map[string]action{
		"approve": users.Approve, "enable": users.Enable, "promote": users.Promote, "demote": users.Demote,
	} {
		mux.HandleFunc("POST /api/users/{id}/"+name, h.admin(func(w http.ResponseWriter, r *http.Request, admin users.User) {
			h.answerProfile(w, r, func(ctx context.Context, tx store.Querier, now time.Time) (users.Profile, error) {
				return act(ctx, tx, admin, r.PathValue("id"), now)
			})
		}))
	}
	for name, act := range map[string]reasoned{"reject": users.Reject, "disable": users.Disable} {
		mux.HandleFunc("POST /api/users/{id}/"+name, h.admin(func(w http.ResponseWriter, r *http.Request, admin users.User) {
			var body struct{ Reason string }
			if err := readJSON(r, &body); err != nil {
				h.writeAPIError(w, err)
				return
			}
			h.answerProfile(w, r, func(ctx context.Context, tx store.Querier, now time.Time) (users.Profile, error) {
				return act(ctx, tx, admin, r.PathValue("id"), body.Reason, now)
			})
		}))
	}
	mux.HandleFunc("POST /api/users/{id}/reset-password", h.admin(h.handleResetPassword))
	// signedIn, not active: a forced change is the one thing a user with a
	// temporary password may do (package users checks the voluntary case).
	mux.HandleFunc("POST /api/users/me/password", h.signedIn(h.handleChangePassword))
}

// handleResetPassword answers the contract's PasswordReset: the profile and
// the temporary password, shown to the admin once.
func (h *apiHandler) handleResetPassword(w http.ResponseWriter, r *http.Request, admin users.User) {
	var reset struct {
		Profile           users.Profile `json:"profile"`
		TemporaryPassword string        `json:"temporaryPassword"`
	}
	err := h.platform.DB.Tx(r.Context(), func(tx store.Querier) error {
		var err error
		reset.Profile, reset.TemporaryPassword, err = users.ResetPassword(r.Context(), tx, admin, r.PathValue("id"), h.platform.Now())
		return err
	})
	if err != nil {
		h.writeAPIError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, reset)
}

func (h *apiHandler) handleChangePassword(w http.ResponseWriter, r *http.Request, u users.User) {
	var body struct{ NewPassword, CurrentPassword string }
	if err := readJSON(r, &body); err != nil {
		h.writeAPIError(w, err)
		return
	}
	h.answerProfile(w, r, func(ctx context.Context, tx store.Querier, _ time.Time) (users.Profile, error) {
		return users.ChangePassword(ctx, tx, u, body.NewPassword, body.CurrentPassword)
	})
}

// answerProfile runs op in one transaction and answers the profile it returns.
func (h *apiHandler) answerProfile(w http.ResponseWriter, r *http.Request,
	op func(ctx context.Context, tx store.Querier, now time.Time) (users.Profile, error)) {
	var p users.Profile
	err := h.platform.DB.Tx(r.Context(), func(tx store.Querier) error {
		var err error
		p, err = op(r.Context(), tx, h.platform.Now())
		return err
	})
	if err != nil {
		h.writeAPIError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, p)
}

// handleRegister stores a pending engineer and signs them in, so they land
// on the waiting page.
func (h *apiHandler) handleRegister(w http.ResponseWriter, r *http.Request) {
	var body users.Registration
	if err := readJSON(r, &body); err != nil {
		h.writeAPIError(w, err)
		return
	}
	ctx := r.Context()
	var session sessionResponse
	err := h.platform.DB.Tx(ctx, func(tx store.Querier) error {
		now := h.platform.Now()
		u, err := users.Register(ctx, tx, body, now)
		if err != nil {
			return err
		}
		session.User = u
		session.Token, err = sessions.Issue(ctx, tx, u.ID, now)
		return err
	})
	if err != nil {
		h.writeAPIError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, session)
}

func (h *apiHandler) handleResubmit(w http.ResponseWriter, r *http.Request, u users.User) {
	var body users.Resubmission
	if err := readJSON(r, &body); err != nil {
		h.writeAPIError(w, err)
		return
	}
	h.answerProfile(w, r, func(ctx context.Context, tx store.Querier, now time.Time) (users.Profile, error) {
		return users.Resubmit(ctx, tx, u.ID, body, now)
	})
}
