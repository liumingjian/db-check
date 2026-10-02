package users_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"dbcheck/reporter/internal/apierr"
	"dbcheck/reporter/internal/store"
	"dbcheck/reporter/internal/users"
)

var now = time.Date(2026, 10, 1, 8, 0, 0, 0, time.UTC)

func openStore(t *testing.T) *store.DB {
	t.Helper()
	db, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// addAdmin stores an active admin named name and returns them as loaded.
func addAdmin(t *testing.T, db *store.DB, name string) users.User {
	t.Helper()
	ctx := context.Background()
	u := users.NewUser{ID: "u-" + name, Username: name, DisplayName: name, Role: users.RoleAdmin,
		Status: users.StatusActive, PasswordHash: "unused", AppliedAt: now}
	if err := users.Insert(ctx, db, u); err != nil {
		t.Fatalf("users.Insert: %v", err)
	}
	loaded, err := users.ByID(ctx, db, u.ID)
	if err != nil {
		t.Fatal(err)
	}
	return loaded
}

var (
	errLastAdmin = apierr.Invalid("平台至少要保留一位可用的管理员")
	errNotAdmin  = apierr.Forbidden("需要管理员权限")
)

func expectRefusal(t *testing.T, err error, want *apierr.Error) {
	t.Helper()
	var got *apierr.Error
	if !errors.As(err, &got) || *got != *want {
		t.Fatalf("err = %v, want %v", err, want)
	}
}

// staleAdmin returns "former" as loaded while still an admin, after "last"
// has demoted them: "last" is now the only active admin.
func staleAdmin(t *testing.T, db *store.DB) users.User {
	t.Helper()
	former := addAdmin(t, db, "former")
	last := addAdmin(t, db, "last")
	demotion := users.AccountAction{Admin: last, TargetID: former.ID, At: now}
	if _, err := users.Demote(context.Background(), db, demotion); err != nil {
		t.Fatalf("demote former: %v", err)
	}
	return former
}

func TestTheLastActiveAdminCannotBeDemotedOrDisabled(t *testing.T) {
	db := openStore(t)
	former := staleAdmin(t, db)
	ctx := context.Background()

	_, err := users.Demote(ctx, db, users.AccountAction{Admin: former, TargetID: "u-last", At: now})
	expectRefusal(t, err, errLastAdmin)
	_, err = users.Disable(ctx, db, users.AccountAction{Admin: former, TargetID: "u-last", Reason: "离职", At: now})
	expectRefusal(t, err, errLastAdmin)
}

func TestActingAdminRefusesAnAdminNoLongerActive(t *testing.T) {
	db := openStore(t)
	former := staleAdmin(t, db)
	ctx := context.Background()

	_, err := users.ActingAdmin(ctx, db, former.ID)
	expectRefusal(t, err, errNotAdmin)
	if _, err := users.Disable(ctx, db, users.AccountAction{Admin: addAdmin(t, db, "third"),
		TargetID: "u-last", Reason: "离职", At: now}); err != nil {
		t.Fatalf("disable last: %v", err)
	}
	_, err = users.ActingAdmin(ctx, db, "u-last")
	expectRefusal(t, err, errNotAdmin)
	if admin, err := users.ActingAdmin(ctx, db, "u-third"); err != nil || admin.ID != "u-third" {
		t.Fatalf("ActingAdmin(third) = %+v, %v", admin, err)
	}
}
