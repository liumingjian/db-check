// Package users owns user accounts (CONTEXT.md: User, Engineer, Admin,
// Account status) and their passwords. Functions take a store.Querier so
// callers can combine them in one transaction.
package users

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"sync"
	"time"

	"dbcheck/reporter/internal/apierr"
	"dbcheck/reporter/internal/store"

	"golang.org/x/crypto/bcrypt"
)

// Role is the wire value of a user's role.
type Role string

const (
	RoleAdmin    Role = "admin"
	RoleEngineer Role = "user" // the engineer role is "user" on the wire
)

type Status string

const (
	StatusPending  Status = "pending"
	StatusActive   Status = "active"
	StatusRejected Status = "rejected"
	StatusDisabled Status = "disabled"
)

// PasswordCost is the bcrypt cost for every password the platform sets.
const PasswordCost = 12

// User is a user as the console contract's User type carries it.
type User struct {
	ID                 string `json:"id"`
	Username           string `json:"username"`
	DisplayName        string `json:"displayName"`
	Role               Role   `json:"role"`
	Status             Status `json:"status"`
	MustChangePassword bool   `json:"mustChangePassword,omitempty"`
}

// IsAdmin reports whether the user holds the admin role, whatever their
// account status.
func (u User) IsAdmin() bool { return u.Role == RoleAdmin }

// NewUser is everything stored for a new account.
type NewUser struct {
	ID                 string
	Username           string
	DisplayName        string
	Role               Role
	Status             Status
	PasswordHash       string
	MustChangePassword bool
	Email              string // empty means none
	Team               string
	Note               string
	Reason             string // empty means none
	AppliedAt          time.Time
}

var (
	ErrNotFound    = apierr.NotFound("用户不存在")
	ErrAdminExists = errors.New("an admin already exists")
	errBadLogin    = apierr.Unauthorized("用户名或密码错误")
	errDisabled    = apierr.Forbidden(DisabledMessage)
)

// DisabledMessage tells a disabled user why they are refused.
const DisabledMessage = "该账号已被禁用，请联系管理员"

// Insert stores a new account.
func Insert(ctx context.Context, q store.Querier, u NewUser) error {
	_, err := q.ExecContext(ctx, `INSERT INTO users
		(id, username, display_name, role, status, password_hash, must_change_password, email, team, note, reason, applied_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		u.ID, u.Username, u.DisplayName, u.Role, u.Status, u.PasswordHash, u.MustChangePassword,
		store.NullIfEmpty(u.Email), u.Team, u.Note, store.NullIfEmpty(u.Reason), store.FormatTime(u.AppliedAt))
	if err != nil {
		return fmt.Errorf("insert user %s: %w", u.Username, err)
	}
	return nil
}

const userColumns = "id, username, display_name, role, status, must_change_password"

// ByID loads a user; ErrNotFound when there is none.
func ByID(ctx context.Context, q store.Querier, id string) (User, error) {
	var u User
	err := q.QueryRowContext(ctx, "SELECT "+userColumns+" FROM users WHERE id = ?", id).
		Scan(&u.ID, &u.Username, &u.DisplayName, &u.Role, &u.Status, &u.MustChangePassword)
	if errors.Is(err, sql.ErrNoRows) {
		return User{}, ErrNotFound
	}
	return u, err
}

// Authenticate checks a username and password. Unknown users and wrong
// passwords fail alike (unauthorized), and an unknown user still costs one
// bcrypt comparison so timing does not reveal which usernames exist. Only a
// correct password learns that the account is disabled (forbidden).
func Authenticate(ctx context.Context, q store.Querier, username, password string) (User, error) {
	var id, hash string
	err := q.QueryRowContext(ctx, "SELECT id, password_hash FROM users WHERE username = ?", username).Scan(&id, &hash)
	if errors.Is(err, sql.ErrNoRows) {
		bcrypt.CompareHashAndPassword(dummyHash(), []byte(password))
		return User{}, errBadLogin
	}
	if err != nil {
		return User{}, err
	}
	if bcrypt.CompareHashAndPassword([]byte(hash), []byte(password)) != nil {
		return User{}, errBadLogin
	}
	u, err := ByID(ctx, q, id)
	if err != nil {
		return User{}, err
	}
	if u.Status == StatusDisabled {
		return User{}, errDisabled
	}
	return u, nil
}

var dummyHash = sync.OnceValue(func() []byte {
	hash, err := bcrypt.GenerateFromPassword([]byte("no such user"), PasswordCost)
	if err != nil {
		panic(err)
	}
	return hash
})

// HashPassword hashes a password at PasswordCost.
func HashPassword(password string) (string, error) {
	hash, err := bcrypt.GenerateFromPassword([]byte(password), PasswordCost)
	return string(hash), err
}

// NewID returns a fresh user ID.
func NewID() string {
	return "u-" + hex.EncodeToString(randomBytes(8))
}

// tempAlphabet reads unambiguously aloud and on paper (no 0/O, 1/l/I).
const tempAlphabet = "abcdefghjkmnpqrstuvwxyzABCDEFGHJKMNPQRSTUVWXYZ23456789"

// TemporaryPassword returns a random 12-character password for an account
// that must change it at first sign-in.
func TemporaryPassword() string {
	out := make([]byte, 12)
	for i := range out {
		n, err := rand.Int(rand.Reader, big.NewInt(int64(len(tempAlphabet))))
		if err != nil {
			panic(err) // crypto/rand never fails on supported platforms
		}
		out[i] = tempAlphabet[n.Int64()]
	}
	return string(out)
}

// CreateFirstAdmin creates an active admin with a temporary password that
// must be changed at first sign-in, and returns that password. It refuses
// with ErrAdminExists once any admin exists, so it cannot be a back door.
func CreateFirstAdmin(ctx context.Context, db *store.DB, username string, now time.Time) (string, error) {
	password := TemporaryPassword()
	hash, err := HashPassword(password)
	if err != nil {
		return "", err
	}
	err = db.Tx(ctx, func(tx store.Querier) error {
		var admins int
		if err := tx.QueryRowContext(ctx, "SELECT count(*) FROM users WHERE role = ?", RoleAdmin).Scan(&admins); err != nil {
			return err
		}
		if admins > 0 {
			return ErrAdminExists
		}
		return Insert(ctx, tx, NewUser{
			ID: NewID(), Username: username, DisplayName: username,
			Role: RoleAdmin, Status: StatusActive, PasswordHash: hash, MustChangePassword: true,
			AppliedAt: now,
		})
	})
	if err != nil {
		return "", err
	}
	return password, nil
}

func randomBytes(n int) []byte {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic(err) // crypto/rand never fails on supported platforms
	}
	return b
}
