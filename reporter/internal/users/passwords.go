package users

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"dbcheck/reporter/internal/apierr"
	"dbcheck/reporter/internal/sessions"
	"dbcheck/reporter/internal/store"

	"golang.org/x/crypto/bcrypt"
)

// ResetPassword gives the target a temporary password that must be changed
// at the next sign-in, ends all of their sessions, and returns the password
// so the admin can hand it over once. Like every account action, the caller
// loads the acting admin with ActingAdmin; run it in one transaction.
func ResetPassword(ctx context.Context, q store.Querier, a AccountAction) (Profile, string, error) {
	password := TemporaryPassword()
	if err := setPassword(ctx, q, passwordUpdate{userID: a.TargetID, password: password, mustChange: true}); err != nil {
		return Profile{}, "", err
	}
	p, err := administer(ctx, q, adminOp{AccountAction: a, kind: ActionReset, change: func(*Profile) error { return nil }})
	if err != nil {
		return Profile{}, "", err
	}
	return p, password, sessions.RevokeAll(ctx, q, a.TargetID)
}

// PasswordChange is a user changing their own password. The caller sets
// User; the passwords arrive in the request.
type PasswordChange struct {
	User            User   `json:"-"`
	NewPassword     string `json:"newPassword"`
	CurrentPassword string `json:"currentPassword"`
}

// ChangePassword sets the user's own new password. A forced change needs no
// current password; otherwise the user must be active and give the right
// current password. The new password may be neither blank nor the current one.
func ChangePassword(ctx context.Context, q store.Querier, c PasswordChange) (Profile, error) {
	u := c.User
	forced := u.MustChangePassword
	if !forced && u.Status != StatusActive {
		return Profile{}, apierr.Forbidden("账号尚未启用")
	}
	var hash []byte
	err := q.QueryRowContext(ctx, "SELECT password_hash FROM users WHERE id = ?", u.ID).Scan(&hash)
	if errors.Is(err, sql.ErrNoRows) {
		return Profile{}, ErrNotFound
	}
	if err != nil {
		return Profile{}, err
	}
	if !forced {
		if c.CurrentPassword == "" {
			return Profile{}, apierr.Invalid("请输入当前密码")
		}
		if bcrypt.CompareHashAndPassword(hash, []byte(c.CurrentPassword)) != nil {
			return Profile{}, apierr.Invalid("当前密码不正确")
		}
	}
	if strings.TrimSpace(c.NewPassword) == "" {
		return Profile{}, apierr.Invalid("请输入新密码")
	}
	if bcrypt.CompareHashAndPassword(hash, []byte(c.NewPassword)) == nil {
		return Profile{}, apierr.Invalid("新密码不能与当前密码相同")
	}
	if err := setPassword(ctx, q, passwordUpdate{userID: u.ID, password: c.NewPassword}); err != nil {
		return Profile{}, err
	}
	return ProfileByID(ctx, q, u.ID)
}

// passwordUpdate is a password to store, and whether it must be changed at the
// next sign-in.
type passwordUpdate struct {
	userID, password string
	mustChange       bool
}

func setPassword(ctx context.Context, q store.Querier, p passwordUpdate) error {
	hash, err := HashPassword(p.password)
	if err != nil {
		return err
	}
	_, err = q.ExecContext(ctx, "UPDATE users SET password_hash = ?, must_change_password = ? WHERE id = ?",
		hash, p.mustChange, p.userID)
	if err != nil {
		return fmt.Errorf("set password of %s: %w", p.userID, err)
	}
	return nil
}
