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

	"golang.org/x/crypto/bcrypt"
)

// ResetPassword gives a user a temporary password that must be changed at
// the next sign-in, ends all of their sessions, and returns the password so
// the admin can hand it over once. The caller checks that admin is an active
// admin; run it in one transaction.
func ResetPassword(ctx context.Context, q store.Querier, admin User, id string, now time.Time) (Profile, string, error) {
	password := TemporaryPassword()
	if err := setPassword(ctx, q, id, password, true); err != nil {
		return Profile{}, "", err
	}
	p, err := administer(ctx, q, admin, id, ActionReset, now, func(*Profile) error { return nil })
	if err != nil {
		return Profile{}, "", err
	}
	return p, password, sessions.RevokeAll(ctx, q, id)
}

// ChangePassword sets u's own new password. A forced change needs no current
// password; otherwise u must be active and give the right current password.
// The new password may be neither blank nor the current one.
func ChangePassword(ctx context.Context, q store.Querier, u User, newPassword, currentPassword string) (Profile, error) {
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
		if currentPassword == "" {
			return Profile{}, apierr.Invalid("请输入当前密码")
		}
		if bcrypt.CompareHashAndPassword(hash, []byte(currentPassword)) != nil {
			return Profile{}, apierr.Invalid("当前密码不正确")
		}
	}
	if strings.TrimSpace(newPassword) == "" {
		return Profile{}, apierr.Invalid("请输入新密码")
	}
	if bcrypt.CompareHashAndPassword(hash, []byte(newPassword)) == nil {
		return Profile{}, apierr.Invalid("新密码不能与当前密码相同")
	}
	if err := setPassword(ctx, q, u.ID, newPassword, false); err != nil {
		return Profile{}, err
	}
	return ProfileByID(ctx, q, u.ID)
}

// setPassword stores a new password and whether it must be changed at the
// next sign-in.
func setPassword(ctx context.Context, q store.Querier, id, password string, mustChange bool) error {
	hash, err := HashPassword(password)
	if err != nil {
		return err
	}
	_, err = q.ExecContext(ctx, "UPDATE users SET password_hash = ?, must_change_password = ? WHERE id = ?",
		hash, mustChange, id)
	if err != nil {
		return fmt.Errorf("set password of %s: %w", id, err)
	}
	return nil
}
