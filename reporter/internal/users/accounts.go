package users

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"dbcheck/reporter/internal/apierr"
	"dbcheck/reporter/internal/sessions"
	"dbcheck/reporter/internal/store"
)

// ActionKind names one admin action on an account.
type ActionKind string

const (
	ActionApprove ActionKind = "approve"
	ActionReject  ActionKind = "reject"
	ActionDisable ActionKind = "disable"
	ActionEnable  ActionKind = "enable"
	ActionPromote ActionKind = "promote"
	ActionDemote  ActionKind = "demote"
	ActionReset   ActionKind = "reset"
)

// Action is one entry of a user's account action history.
type Action struct {
	Action ActionKind `json:"action"`
	By     string     `json:"by"` // the acting admin's username
	At     string     `json:"at"`
}

// Profile is a user plus what the account screens show, as the console
// contract's UserProfile carries it.
type Profile struct {
	User
	Email     string   `json:"email"`
	Team      string   `json:"team"`
	Note      string   `json:"note"`
	AppliedAt string   `json:"appliedAt"`
	Reason    string   `json:"reason,omitempty"` // while rejected or disabled
	Actions   []Action `json:"actions"`          // oldest first
}

// Registration is what a new engineer submits.
type Registration struct {
	Username    string `json:"username"`
	Password    string `json:"password"`
	DisplayName string `json:"displayName"`
	Email       string `json:"email"`
	Team        string `json:"team"`
	Note        string `json:"note"`
}

// Resubmission is a rejected user's new application; username and email stay.
type Resubmission struct {
	DisplayName string `json:"displayName"`
	Team        string `json:"team"`
	Note        string `json:"note"`
}

// Register stores a pending engineer. Usernames are unique as written,
// emails regardless of case.
func Register(ctx context.Context, q store.Querier, r Registration, now time.Time) (User, error) {
	u := NewUser{
		ID: NewID(), Role: RoleEngineer, Status: StatusPending, AppliedAt: now,
		Username: strings.TrimSpace(r.Username), DisplayName: strings.TrimSpace(r.DisplayName),
		Email: strings.TrimSpace(r.Email), Team: strings.TrimSpace(r.Team), Note: strings.TrimSpace(r.Note),
	}
	if u.Username == "" || u.DisplayName == "" || u.Email == "" || u.Team == "" || r.Password == "" {
		return User{}, apierr.Invalid("请填写用户名、显示名称、邮箱、团队和密码")
	}
	for _, unique := range []struct{ where, value, taken string }{
		{"username = ?", u.Username, "用户名已被占用"},
		{"lower(email) = lower(?)", u.Email, "邮箱已被注册"},
	} {
		taken, err := exists(ctx, q, unique.where, unique.value)
		if err != nil {
			return User{}, err
		}
		if taken {
			return User{}, apierr.Invalid(unique.taken)
		}
	}
	hash, err := HashPassword(r.Password)
	if err != nil {
		return User{}, err
	}
	u.PasswordHash = hash
	if err := Insert(ctx, q, u); err != nil {
		return User{}, err
	}
	return ByID(ctx, q, u.ID)
}

// Resubmit moves a rejected user back to pending with a new application.
func Resubmit(ctx context.Context, q store.Querier, id string, r Resubmission, now time.Time) (Profile, error) {
	displayName, team := strings.TrimSpace(r.DisplayName), strings.TrimSpace(r.Team)
	if displayName == "" || team == "" {
		return Profile{}, apierr.Invalid("请填写显示名称和团队")
	}
	p, err := ProfileByID(ctx, q, id)
	if err != nil {
		return Profile{}, err
	}
	if p.Status != StatusRejected {
		return Profile{}, apierr.Invalid("只有被拒绝的申请可以重新提交")
	}
	_, err = q.ExecContext(ctx, `UPDATE users SET display_name = ?, team = ?, note = ?, status = ?, reason = NULL, applied_at = ?
		WHERE id = ?`, displayName, team, strings.TrimSpace(r.Note), StatusPending, store.FormatTime(now), id)
	if err != nil {
		return Profile{}, fmt.Errorf("resubmit %s: %w", id, err)
	}
	return ProfileByID(ctx, q, id)
}

const profileColumns = userColumns + ", coalesce(email, ''), team, note, coalesce(reason, ''), applied_at"

func scanProfile(row interface{ Scan(...any) error }) (Profile, error) {
	var p Profile
	err := row.Scan(&p.ID, &p.Username, &p.DisplayName, &p.Role, &p.Status, &p.MustChangePassword,
		&p.Email, &p.Team, &p.Note, &p.Reason, &p.AppliedAt)
	p.Actions = []Action{}
	return p, err
}

// ProfileByID loads one profile with its action history; ErrNotFound when
// there is none.
func ProfileByID(ctx context.Context, q store.Querier, id string) (Profile, error) {
	p, err := scanProfile(q.QueryRowContext(ctx, "SELECT "+profileColumns+" FROM users WHERE id = ?", id))
	if errors.Is(err, sql.ErrNoRows) {
		return Profile{}, ErrNotFound
	}
	if err != nil {
		return Profile{}, err
	}
	actions, err := loadActions(ctx, q, "WHERE a.user_id = ?", id)
	if err != nil {
		return Profile{}, err
	}
	p.Actions = append(p.Actions, actions[id]...)
	return p, nil
}

// List returns every profile, newest application first.
func List(ctx context.Context, q store.Querier) ([]Profile, error) {
	rows, err := q.QueryContext(ctx, "SELECT "+profileColumns+" FROM users ORDER BY applied_at DESC, id")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var all []Profile
	for rows.Next() {
		p, err := scanProfile(rows)
		if err != nil {
			return nil, err
		}
		all = append(all, p)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	actions, err := loadActions(ctx, q, "")
	if err != nil {
		return nil, err
	}
	for i := range all {
		all[i].Actions = append(all[i].Actions, actions[all[i].ID]...)
	}
	return all, nil
}

// loadActions returns action histories by user ID, oldest first.
func loadActions(ctx context.Context, q store.Querier, where string, args ...any) (map[string][]Action, error) {
	rows, err := q.QueryContext(ctx, `SELECT a.user_id, a.action, b.username, a.at
		FROM account_actions a JOIN users b ON b.id = a.by_id `+where+` ORDER BY a.seq`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	byUser := map[string][]Action{}
	for rows.Next() {
		var userID string
		var a Action
		if err := rows.Scan(&userID, &a.Action, &a.By, &a.At); err != nil {
			return nil, err
		}
		byUser[userID] = append(byUser[userID], a)
	}
	return byUser, rows.Err()
}

// RecordAction appends one entry to a user's account action history.
func RecordAction(ctx context.Context, q store.Querier, userID string, action ActionKind, byID string, at time.Time) error {
	_, err := q.ExecContext(ctx, "INSERT INTO account_actions (user_id, action, by_id, at) VALUES (?, ?, ?, ?)",
		userID, action, byID, store.FormatTime(at))
	if err != nil {
		return fmt.Errorf("record %s on %s: %w", action, userID, err)
	}
	return nil
}

// The admin's account actions. Each checks the target's state, changes it,
// and appends the action with the acting admin and time. Callers check that
// admin is an active admin; run each in one transaction.

// Approve moves a pending user to active.
func Approve(ctx context.Context, q store.Querier, admin User, id string, now time.Time) (Profile, error) {
	return administer(ctx, q, admin, id, ActionApprove, now, func(p *Profile) error {
		if p.Status != StatusPending {
			return errNotPending
		}
		p.Status, p.Reason = StatusActive, ""
		return nil
	})
}

// Reject moves a pending user to rejected; the reason is required.
func Reject(ctx context.Context, q store.Querier, admin User, id, reason string, now time.Time) (Profile, error) {
	reason = strings.TrimSpace(reason)
	if reason == "" {
		return Profile{}, apierr.Invalid("请填写拒绝原因")
	}
	return administer(ctx, q, admin, id, ActionReject, now, func(p *Profile) error {
		if p.Status != StatusPending {
			return errNotPending
		}
		p.Status, p.Reason = StatusRejected, reason
		return nil
	})
}

// Disable moves an active user to disabled and ends all of their sessions;
// the reason is required.
func Disable(ctx context.Context, q store.Querier, admin User, id, reason string, now time.Time) (Profile, error) {
	reason = strings.TrimSpace(reason)
	if reason == "" {
		return Profile{}, apierr.Invalid("请填写禁用原因")
	}
	p, err := administer(ctx, q, admin, id, ActionDisable, now, func(p *Profile) error {
		if p.Status != StatusActive {
			return apierr.Invalid("只能禁用已启用的账号")
		}
		if err := keepAdminSeat(ctx, q, admin, p); err != nil {
			return err
		}
		p.Status, p.Reason = StatusDisabled, reason
		return nil
	})
	if err != nil {
		return Profile{}, err
	}
	return p, sessions.RevokeAll(ctx, q, id)
}

// Enable moves a disabled user back to active.
func Enable(ctx context.Context, q store.Querier, admin User, id string, now time.Time) (Profile, error) {
	return administer(ctx, q, admin, id, ActionEnable, now, func(p *Profile) error {
		if p.Status != StatusDisabled {
			return apierr.Invalid("只能启用已禁用的账号")
		}
		p.Status, p.Reason = StatusActive, ""
		return nil
	})
}

// Promote makes an active engineer an admin.
func Promote(ctx context.Context, q store.Querier, admin User, id string, now time.Time) (Profile, error) {
	return administer(ctx, q, admin, id, ActionPromote, now, func(p *Profile) error {
		if p.Status != StatusActive || p.Role != RoleEngineer {
			return apierr.Invalid("只能提升已启用的工程师")
		}
		p.Role = RoleAdmin
		return nil
	})
}

// Demote makes an active admin an engineer.
func Demote(ctx context.Context, q store.Querier, admin User, id string, now time.Time) (Profile, error) {
	return administer(ctx, q, admin, id, ActionDemote, now, func(p *Profile) error {
		if p.Status != StatusActive || p.Role != RoleAdmin {
			return apierr.Invalid("只能降级已启用的管理员")
		}
		if err := keepAdminSeat(ctx, q, admin, p); err != nil {
			return err
		}
		p.Role = RoleEngineer
		return nil
	})
}

var errNotPending = apierr.Invalid("只能处理待审批的申请")

// administer loads the target, lets change check and edit its role, status,
// and reason, stores them, and records the action.
func administer(ctx context.Context, q store.Querier, admin User, id string, action ActionKind, now time.Time,
	change func(p *Profile) error) (Profile, error) {
	p, err := ProfileByID(ctx, q, id)
	if err != nil {
		return Profile{}, err
	}
	if err := change(&p); err != nil {
		return Profile{}, err
	}
	_, err = q.ExecContext(ctx, "UPDATE users SET role = ?, status = ?, reason = ? WHERE id = ?",
		p.Role, p.Status, nullable(p.Reason), id)
	if err != nil {
		return Profile{}, fmt.Errorf("%s %s: %w", action, id, err)
	}
	if err := RecordAction(ctx, q, id, action, admin.ID, now); err != nil {
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
	if target.Role != RoleAdmin {
		return nil
	}
	others, err := exists(ctx, q, "role = ? AND status = ? AND id <> ?", RoleAdmin, StatusActive, target.ID)
	if err != nil || others {
		return err
	}
	return apierr.Invalid("平台至少要保留一位可用的管理员")
}

func exists(ctx context.Context, q store.Querier, where string, args ...any) (bool, error) {
	var found bool
	err := q.QueryRowContext(ctx, "SELECT EXISTS (SELECT 1 FROM users WHERE "+where+")", args...).Scan(&found)
	return found, err
}
