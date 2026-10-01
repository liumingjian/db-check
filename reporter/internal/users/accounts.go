package users

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"dbcheck/reporter/internal/apierr"
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

// Resubmission is a rejected user's new application; username and email
// stay. The caller sets UserID and At; the rest arrives in the request.
type Resubmission struct {
	UserID      string    `json:"-"`
	DisplayName string    `json:"displayName"`
	Team        string    `json:"team"`
	Note        string    `json:"note"`
	At          time.Time `json:"-"`
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
func Resubmit(ctx context.Context, q store.Querier, r Resubmission) (Profile, error) {
	id := r.UserID
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
		WHERE id = ?`, displayName, team, strings.TrimSpace(r.Note), StatusPending, store.FormatTime(r.At), id)
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

func exists(ctx context.Context, q store.Querier, where string, args ...any) (bool, error) {
	var found bool
	err := q.QueryRowContext(ctx, "SELECT EXISTS (SELECT 1 FROM users WHERE "+where+")", args...).Scan(&found)
	return found, err
}
