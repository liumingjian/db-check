package testserver

import (
	"context"
	"fmt"
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
	Actions            []seedAction `json:"actions"`
}

type seedAction struct {
	Action users.ActionKind `json:"action"`
	By     string           `json:"by"` // the acting admin's username
	At     string           `json:"at"`
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
	return seedActions(ctx, tx, seed, now)
}

// seedActions loads every account action history once all users exist, as
// actions name their acting admin by username.
func seedActions(ctx context.Context, tx store.Querier, seed []seedUser, now time.Time) error {
	ids := map[string]string{}
	for _, u := range seed {
		ids[u.Username] = u.ID
	}
	for _, u := range seed {
		for _, a := range u.Actions {
			at, err := seedTime(a.At, now)
			if err != nil {
				return err
			}
			byID, ok := ids[a.By]
			if !ok {
				return fmt.Errorf("seed fixture: %s's %s action names unknown admin %q", u.Username, a.Action, a.By)
			}
			if err := users.RecordAction(ctx, tx, u.ID, a.Action, byID, at); err != nil {
				return err
			}
		}
	}
	return nil
}
