package testserver

import (
	"context"
	"time"

	"dbcheck/reporter/internal/store"
	"dbcheck/reporter/internal/users"

	"golang.org/x/crypto/bcrypt"
)

type seedUser struct {
	ID                 string       `json:"id"`
	Username           string       `json:"username"`
	Password           string       `json:"password"`
	DisplayName        string       `json:"displayName"`
	Role               users.Role   `json:"role"`
	Status             users.Status `json:"status"`
	MustChangePassword bool         `json:"mustChangePassword"`
	Email              string       `json:"email"`
	Team               string       `json:"team"`
	Note               string       `json:"note"`
	Reason             string       `json:"reason"`
	AppliedAt          string       `json:"appliedAt"`
	// The account action history ("actions") is loaded with the account
	// lifecycle (#37).
}

func seedUsers(ctx context.Context, tx store.Querier, f fixture, now time.Time) error {
	var seed []seedUser
	if err := f.section("users", &seed); err != nil {
		return err
	}
	for _, u := range seed {
		appliedAt, err := seedTime(u.AppliedAt, now)
		if err != nil {
			return err
		}
		// The lowest cost keeps resets and seed sign-ins fast; real
		// passwords use users.PasswordCost.
		hash, err := bcrypt.GenerateFromPassword([]byte(u.Password), bcrypt.MinCost)
		if err != nil {
			return err
		}
		err = users.Insert(ctx, tx, users.NewUser{
			ID: u.ID, Username: u.Username, DisplayName: u.DisplayName, Role: u.Role, Status: u.Status,
			PasswordHash: string(hash), MustChangePassword: u.MustChangePassword,
			Email: u.Email, Team: u.Team, Note: u.Note, Reason: u.Reason, AppliedAt: appliedAt,
		})
		if err != nil {
			return err
		}
	}
	return nil
}
