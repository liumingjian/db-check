package users

import (
	"context"
	"fmt"
	"strings"
	"time"

	"dbcheck/reporter/internal/apierr"
	"dbcheck/reporter/internal/sessions"
	"dbcheck/reporter/internal/store"
)

// The admin's account actions. Each checks the target's state, changes it,
// and appends the action with the acting admin and time. Callers load the
// acting admin with ActingAdmin in the same transaction they run the action in.

// AccountAction is one admin action on an account: who acts, on whom, when,
// and why, for the actions that take a reason (reject, disable).
type AccountAction struct {
	Admin    User
	TargetID string
	Reason   string
	At       time.Time
}

var errNotAdmin = apierr.Forbidden("需要管理员权限")

// ActingAdmin loads the admin about to act; forbidden unless they are still
// an active admin with no forced password change due. Run it in the
// action's transaction, so an admin demoted or disabled a moment ago cannot
// finish an action already in flight.
func ActingAdmin(ctx context.Context, q store.Querier, id string) (User, error) {
	u, err := ByID(ctx, q, id)
	if err != nil {
		return User{}, err
	}
	if !u.IsAdmin() || u.Status != StatusActive || u.MustChangePassword {
		return User{}, errNotAdmin
	}
	return u, nil
}

// Approve moves a pending user to active.
func Approve(ctx context.Context, q store.Querier, a AccountAction) (Profile, error) {
	return administer(ctx, q, adminOp{AccountAction: a, kind: ActionApprove, change: func(p *Profile) error {
		if p.Status != StatusPending {
			return errNotPending
		}
		p.Status, p.Reason = StatusActive, ""
		return nil
	}})
}

// Reject moves a pending user to rejected; the reason is required.
func Reject(ctx context.Context, q store.Querier, a AccountAction) (Profile, error) {
	reason := strings.TrimSpace(a.Reason)
	if reason == "" {
		return Profile{}, apierr.Invalid("请填写拒绝原因")
	}
	return administer(ctx, q, adminOp{AccountAction: a, kind: ActionReject, change: func(p *Profile) error {
		if p.Status != StatusPending {
			return errNotPending
		}
		p.Status, p.Reason = StatusRejected, reason
		return nil
	}})
}

// Disable moves an active user to disabled and ends all of their sessions;
// the reason is required.
func Disable(ctx context.Context, q store.Querier, a AccountAction) (Profile, error) {
	reason := strings.TrimSpace(a.Reason)
	if reason == "" {
		return Profile{}, apierr.Invalid("请填写禁用原因")
	}
	p, err := administer(ctx, q, adminOp{AccountAction: a, kind: ActionDisable, change: func(p *Profile) error {
		if p.Status != StatusActive {
			return apierr.Invalid("只能禁用已启用的账号")
		}
		if err := keepAdminSeat(ctx, q, a.Admin, p); err != nil {
			return err
		}
		p.Status, p.Reason = StatusDisabled, reason
		return nil
	}})
	if err != nil {
		return Profile{}, err
	}
	return p, sessions.RevokeAll(ctx, q, a.TargetID)
}

// Enable moves a disabled user back to active.
func Enable(ctx context.Context, q store.Querier, a AccountAction) (Profile, error) {
	return administer(ctx, q, adminOp{AccountAction: a, kind: ActionEnable, change: func(p *Profile) error {
		if p.Status != StatusDisabled {
			return apierr.Invalid("只能启用已禁用的账号")
		}
		p.Status, p.Reason = StatusActive, ""
		return nil
	}})
}

// Promote makes an active engineer an admin.
func Promote(ctx context.Context, q store.Querier, a AccountAction) (Profile, error) {
	return administer(ctx, q, adminOp{AccountAction: a, kind: ActionPromote, change: func(p *Profile) error {
		if p.Status != StatusActive || p.Role != RoleEngineer {
			return apierr.Invalid("只能提升已启用的工程师")
		}
		p.Role = RoleAdmin
		return nil
	}})
}

// Demote makes an active admin an engineer.
func Demote(ctx context.Context, q store.Querier, a AccountAction) (Profile, error) {
	return administer(ctx, q, adminOp{AccountAction: a, kind: ActionDemote, change: func(p *Profile) error {
		if p.Status != StatusActive || !p.IsAdmin() {
			return apierr.Invalid("只能降级已启用的管理员")
		}
		if err := keepAdminSeat(ctx, q, a.Admin, p); err != nil {
			return err
		}
		p.Role = RoleEngineer
		return nil
	}})
}

var errNotPending = apierr.Invalid("只能处理待审批的申请")

// adminOp is an account action of one kind; change checks and edits the
// target's role, status, and reason.
type adminOp struct {
	AccountAction
	kind   ActionKind
	change func(p *Profile) error
}

// administer loads the target, applies op's change, stores it, and records
// the action.
func administer(ctx context.Context, q store.Querier, op adminOp) (Profile, error) {
	id := op.TargetID
	p, err := ProfileByID(ctx, q, id)
	if err != nil {
		return Profile{}, err
	}
	if err := op.change(&p); err != nil {
		return Profile{}, err
	}
	_, err = q.ExecContext(ctx, "UPDATE users SET role = ?, status = ?, reason = ? WHERE id = ?",
		p.Role, p.Status, store.NullIfEmpty(p.Reason), id)
	if err != nil {
		return Profile{}, fmt.Errorf("%s %s: %w", op.kind, id, err)
	}
	if err := RecordAction(ctx, q, ActionRecord{UserID: id, Action: op.kind, ByID: op.Admin.ID, At: op.At}); err != nil {
		return Profile{}, err
	}
	return ProfileByID(ctx, q, id)
}

// keepAdminSeat refuses to disable or demote oneself, or the last active
// admin, so the platform always keeps one.
func keepAdminSeat(ctx context.Context, q store.Querier, admin User, target *Profile) error {
	if target.ID == admin.ID {
		return apierr.Invalid("不能对自己执行此操作")
	}
	if !target.IsAdmin() {
		return nil
	}
	others, err := exists(ctx, q, "role = ? AND status = ? AND id <> ?", RoleAdmin, StatusActive, target.ID)
	if err != nil || others {
		return err
	}
	return apierr.Invalid("平台至少要保留一位可用的管理员")
}

// ActionRecord is one entry to append to a user's account action history.
type ActionRecord struct {
	UserID string
	Action ActionKind
	ByID   string
	At     time.Time
}

// RecordAction appends one entry to a user's account action history.
func RecordAction(ctx context.Context, q store.Querier, r ActionRecord) error {
	_, err := q.ExecContext(ctx, "INSERT INTO account_actions (user_id, action, by_id, at) VALUES (?, ?, ?, ?)",
		r.UserID, r.Action, r.ByID, store.FormatTime(r.At))
	if err != nil {
		return fmt.Errorf("record %s on %s: %w", r.Action, r.UserID, err)
	}
	return nil
}
