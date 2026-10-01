package web

import (
	"context"
	"net/http"
	"time"

	"dbcheck/reporter/internal/sessions"
	"dbcheck/reporter/internal/store"
	"dbcheck/reporter/internal/users"
)

// accountAction is one of package users' admin actions on an account.
type accountAction func(ctx context.Context, q store.Querier, a users.AccountAction) (users.Profile, error)

// registerUsersRoutes mounts registration, resubmission, the own profile,
// and the admin's account operations (package users holds the rules).
func (h *apiHandler) registerUsersRoutes(mux routeMux) {
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
	for kind, act := range map[users.ActionKind]accountAction{
		users.ActionApprove: users.Approve, users.ActionEnable: users.Enable,
		users.ActionPromote: users.Promote, users.ActionDemote: users.Demote,
	} {
		mux.HandleFunc("POST /api/users/{id}/"+string(kind), h.admin(h.handleAccountAction(act, noReason)))
	}
	for kind, act := range map[users.ActionKind]accountAction{users.ActionReject: users.Reject, users.ActionDisable: users.Disable} {
		mux.HandleFunc("POST /api/users/{id}/"+string(kind), h.admin(h.handleAccountAction(act, bodyReason)))
	}
	mux.HandleFunc("POST /api/users/{id}/reset-password", h.admin(h.handleResetPassword))
	// signedIn, not active: a forced change is the one thing a user with a
	// temporary password may do (package users checks the voluntary case).
	mux.HandleFunc("POST /api/users/me/password", h.signedIn(h.handleChangePassword))
}

// reasonReader reads an account action's reason from its request.
type reasonReader func(r *http.Request) (string, error)

// noReason is for the actions that take no request body.
func noReason(*http.Request) (string, error) { return "", nil }

// bodyReason reads {"reason": "..."} (reject, disable).
func bodyReason(r *http.Request) (string, error) {
	var body struct{ Reason string }
	err := readJSON(r, &body)
	return body.Reason, err
}

// handleAccountAction answers POST /api/users/{id}/<action> with the
// target's new profile.
func (h *apiHandler) handleAccountAction(act accountAction, readReason reasonReader) authedHandler {
	return func(w http.ResponseWriter, r *http.Request, admin users.User) {
		reason, err := readReason(r)
		if err != nil {
			h.writeAPIError(w, err)
			return
		}
		var p users.Profile
		err = h.asActingAdmin(r, admin, func(tx store.Querier, a users.AccountAction) (err error) {
			a.Reason = reason
			p, err = act(r.Context(), tx, a)
			return err
		})
		if err != nil {
			h.writeAPIError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, p)
	}
}

// asActingAdmin runs op in one transaction, with the acting admin reloaded
// inside it (users.ActingAdmin), so an admin demoted or disabled after the
// access check cannot finish the action. The target is the path's {id}.
func (h *apiHandler) asActingAdmin(r *http.Request, admin users.User, op func(tx store.Querier, a users.AccountAction) error) error {
	ctx := r.Context()
	return h.platform.DB.Tx(ctx, func(tx store.Querier) error {
		current, err := users.ActingAdmin(ctx, tx, admin.ID)
		if err != nil {
			return err
		}
		return op(tx, users.AccountAction{Admin: current, TargetID: r.PathValue("id"), At: h.platform.Now()})
	})
}

// handleResetPassword answers the contract's PasswordReset: the profile and
// the temporary password, shown to the admin once.
func (h *apiHandler) handleResetPassword(w http.ResponseWriter, r *http.Request, admin users.User) {
	var reset struct {
		Profile           users.Profile `json:"profile"`
		TemporaryPassword string        `json:"temporaryPassword"`
	}
	err := h.asActingAdmin(r, admin, func(tx store.Querier, a users.AccountAction) (err error) {
		reset.Profile, reset.TemporaryPassword, err = users.ResetPassword(r.Context(), tx, a)
		return err
	})
	if err != nil {
		h.writeAPIError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, reset)
}

func (h *apiHandler) handleChangePassword(w http.ResponseWriter, r *http.Request, u users.User) {
	var change users.PasswordChange
	if err := readJSON(r, &change); err != nil {
		h.writeAPIError(w, err)
		return
	}
	change.User = u
	h.answerProfile(w, r, func(ctx context.Context, tx store.Querier, _ time.Time) (users.Profile, error) {
		return users.ChangePassword(ctx, tx, change)
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
		body.UserID, body.At = u.ID, now
		return users.Resubmit(ctx, tx, body)
	})
}
