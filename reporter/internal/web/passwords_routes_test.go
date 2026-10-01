package web

import (
	"net/http"
	"testing"

	"dbcheck/reporter/internal/users"
)

func changePassword(newPassword string, current ...string) map[string]string {
	body := map[string]string{"newPassword": newPassword}
	if len(current) > 0 {
		body["currentPassword"] = current[0]
	}
	return body
}

func TestResetPasswordShowsATemporaryPasswordOnceAndEndsEverySession(t *testing.T) {
	f := newPlatformFixture(t)
	f.addUser("admin", users.RoleAdmin, users.StatusActive)
	f.addUser("user", users.RoleEngineer, users.StatusActive)
	admin, first, second := f.token("admin"), f.token("user"), f.token("user")

	rec := f.do(http.MethodPost, "/api/users/u-user/reset-password", admin, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("reset: %d %s", rec.Code, rec.Body)
	}
	var reset struct {
		TemporaryPassword string
		Profile           struct {
			Username           string
			MustChangePassword bool
			Actions            []struct{ Action, By string }
		}
	}
	decode(t, rec, &reset)
	actions := reset.Profile.Actions
	if reset.TemporaryPassword == "" || !reset.Profile.MustChangePassword || len(actions) == 0 ||
		actions[len(actions)-1].Action != "reset" || actions[len(actions)-1].By != "admin" {
		t.Fatalf("reset = %+v", reset)
	}

	for _, token := range []string{first, second} {
		expectAPIError(t, f.do(http.MethodGet, "/api/auth/me", token, nil), http.StatusUnauthorized, "unauthorized", "")
	}
	expectAPIError(t, f.signIn("user", "user"), http.StatusUnauthorized, "unauthorized", "")
	rec = f.signIn("user", reset.TemporaryPassword)
	var session struct{ User struct{ MustChangePassword bool } }
	decode(t, rec, &session)
	if rec.Code != http.StatusOK || !session.User.MustChangePassword {
		t.Fatalf("sign-in with the temporary password: %d %s", rec.Code, rec.Body)
	}
}

func TestResetPasswordIsForAdminsOnly(t *testing.T) {
	f := newPlatformFixture(t)
	f.addUser("admin", users.RoleAdmin, users.StatusActive)
	f.addUser("user", users.RoleEngineer, users.StatusActive)

	expectAPIError(t, f.do(http.MethodPost, "/api/users/u-admin/reset-password", f.token("user"), nil),
		http.StatusForbidden, "forbidden", "")
	expectAPIError(t, f.do(http.MethodPost, "/api/users/u-nobody/reset-password", f.token("admin"), nil),
		http.StatusNotFound, "not_found", "")
	f.token("admin") // the admin's own password is untouched
}

// gatedEndpoints holds one representative endpoint per platform domain that
// a pending forced password change must refuse. Add a line when a domain
// gains routes.
var gatedEndpoints = []struct{ domain, method, path string }{
	{"users", http.MethodGet, "/api/users"},
	{"releases", http.MethodGet, "/api/releases"},
	{"reports", http.MethodGet, "/api/reports/status/any-task"},
	{"downloads", http.MethodGet, "/api/downloads"},
}

// openEndpoints are what a user with a forced password change can still
// reach: the current user and the own profile.
var openEndpoints = []struct{ domain, method, path string }{
	{"auth", http.MethodGet, "/api/auth/me"},
	{"users", http.MethodGet, "/api/users/me"},
}

func TestForcedPasswordChangeHoldsTheUserToTheirOwnAccount(t *testing.T) {
	f := newPlatformFixture(t)
	f.insert(users.NewUser{ID: "u-temp", Username: "temp", DisplayName: "temp",
		Role: users.RoleAdmin, Status: users.StatusActive, MustChangePassword: true})
	token := f.token("temp")

	for _, e := range gatedEndpoints {
		t.Run("gated/"+e.domain, func(t *testing.T) {
			expectAPIError(t, f.do(e.method, e.path, token, nil), http.StatusForbidden, "forbidden", "请先修改密码")
		})
	}
	for _, e := range openEndpoints {
		t.Run("open/"+e.domain, func(t *testing.T) {
			if rec := f.do(e.method, e.path, token, nil); rec.Code != http.StatusOK {
				t.Fatalf("%s %s: %d %s", e.method, e.path, rec.Code, rec.Body)
			}
		})
	}

	// The forced change needs no current password and lifts the gate.
	rec := f.do(http.MethodPost, "/api/users/me/password", token, changePassword("my-new-pass"))
	var profile struct{ MustChangePassword bool }
	decode(t, rec, &profile)
	if rec.Code != http.StatusOK || profile.MustChangePassword {
		t.Fatalf("forced change: %d %s", rec.Code, rec.Body)
	}
	// The gate lifts: no endpoint answers 403 any more (one may still
	// answer 404 for the made-up record it names).
	for _, e := range gatedEndpoints {
		if rec := f.do(e.method, e.path, token, nil); rec.Code == http.StatusForbidden {
			t.Fatalf("after the change, %s %s: %d %s", e.method, e.path, rec.Code, rec.Body)
		}
	}
	expectAPIError(t, f.signIn("temp", "temp"), http.StatusUnauthorized, "unauthorized", "")
	if rec := f.signIn("temp", "my-new-pass"); rec.Code != http.StatusOK {
		t.Fatalf("sign-in with the new password: %d %s", rec.Code, rec.Body)
	}
}

func TestForcedPasswordChangeRefusesABlankOrUnchangedPassword(t *testing.T) {
	f := newPlatformFixture(t)
	f.insert(users.NewUser{ID: "u-temp", Username: "temp", DisplayName: "temp",
		Role: users.RoleEngineer, Status: users.StatusActive, MustChangePassword: true})
	token := f.token("temp")

	expectAPIError(t, f.do(http.MethodPost, "/api/users/me/password", token, changePassword("  ")),
		http.StatusBadRequest, "invalid", "请输入新密码")
	expectAPIError(t, f.do(http.MethodPost, "/api/users/me/password", token, changePassword("temp")),
		http.StatusBadRequest, "invalid", "新密码不能与当前密码相同")
	expectAPIError(t, f.do(http.MethodGet, "/api/users", token, nil), http.StatusForbidden, "forbidden", "请先修改密码")
}

func TestVoluntaryPasswordChangeNeedsTheRightCurrentPassword(t *testing.T) {
	cases := []struct {
		name    string
		body    map[string]string
		message string
	}{
		{"no current password", changePassword("my-new-pass"), "请输入当前密码"},
		{"an empty current password", changePassword("my-new-pass", ""), "请输入当前密码"},
		{"a wrong current password", changePassword("my-new-pass", "wrong"), "当前密码不正确"},
		{"a blank new password", changePassword("  ", "user"), "请输入新密码"},
		{"the current password again", changePassword("user", "user"), "新密码不能与当前密码相同"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := newPlatformFixture(t)
			f.addUser("user", users.RoleEngineer, users.StatusActive)
			token := f.token("user")
			rec := f.do(http.MethodPost, "/api/users/me/password", token, c.body)
			expectAPIError(t, rec, http.StatusBadRequest, "invalid", c.message)
			f.token("user") // unchanged
		})
	}
}

func TestVoluntaryPasswordChangeKeepsTheSession(t *testing.T) {
	f := newPlatformFixture(t)
	f.addUser("user", users.RoleEngineer, users.StatusActive)
	token := f.token("user")

	if rec := f.do(http.MethodPost, "/api/users/me/password", token, changePassword("my-new-pass", "user")); rec.Code != http.StatusOK {
		t.Fatalf("change: %d %s", rec.Code, rec.Body)
	}
	if rec := f.do(http.MethodGet, "/api/users/me", token, nil); rec.Code != http.StatusOK {
		t.Fatalf("session after the change: %d %s", rec.Code, rec.Body)
	}
	expectAPIError(t, f.signIn("user", "user"), http.StatusUnauthorized, "unauthorized", "")
	if rec := f.signIn("user", "my-new-pass"); rec.Code != http.StatusOK {
		t.Fatalf("sign-in with the new password: %d %s", rec.Code, rec.Body)
	}
}

func TestVoluntaryPasswordChangeIsForActiveUsers(t *testing.T) {
	f := newPlatformFixture(t)
	f.addUser("lisi", users.RoleEngineer, users.StatusPending)

	expectAPIError(t, f.do(http.MethodPost, "/api/users/me/password", f.token("lisi"), changePassword("my-new-pass", "lisi")),
		http.StatusForbidden, "forbidden", "账号尚未启用")
}
