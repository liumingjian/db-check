package web

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"dbcheck/reporter/internal/users"
)

func TestSignInOpensASessionForTheCurrentUser(t *testing.T) {
	f := newPlatformFixture(t)
	f.addUser("admin", users.RoleAdmin, users.StatusActive)

	rec := f.signIn("admin", "admin")
	if rec.Code != http.StatusOK {
		t.Fatalf("sign-in: %d %s", rec.Code, rec.Body)
	}
	var session struct {
		Token string
		User  struct{ Username, Role, Status string }
	}
	decode(t, rec, &session)
	if session.Token == "" || session.User.Username != "admin" || session.User.Role != "admin" {
		t.Fatalf("session = %+v", session)
	}

	rec = f.do(http.MethodGet, "/api/auth/me", session.Token, nil)
	var me struct{ Username, Role, Status string }
	decode(t, rec, &me)
	if rec.Code != http.StatusOK || me.Username != "admin" || me.Status != "active" {
		t.Fatalf("me: %d %+v", rec.Code, me)
	}
}

func TestSignInRefusesWrongCredentialsAndLogsThem(t *testing.T) {
	f := newPlatformFixture(t)
	f.addUser("user", users.RoleEngineer, users.StatusActive)

	expectAPIError(t, f.signIn("user", "nope"), apiError{http.StatusUnauthorized, "unauthorized", "用户名或密码错误"})
	expectAPIError(t, f.signIn("nobody", "nope"), apiError{http.StatusUnauthorized, "unauthorized", "用户名或密码错误"})

	for _, name := range []string{`"user"`, `"nobody"`} {
		if !strings.Contains(f.log.String(), name) {
			t.Fatalf("log %q does not record the failed sign-in for %s", f.log, name)
		}
	}
}

func TestSignInRefusesADisabledUser(t *testing.T) {
	f := newPlatformFixture(t)
	f.addUser("wangwu", users.RoleEngineer, users.StatusDisabled)

	expectAPIError(t, f.signIn("wangwu", "wangwu"), apiError{http.StatusForbidden, "forbidden", "禁用"})
	// A wrong password says nothing about the account status.
	expectAPIError(t, f.signIn("wangwu", "nope"), apiError{http.StatusUnauthorized, "unauthorized", "用户名或密码错误"})
}

func TestSignInLetsApplicantsReachOnlyTheirSession(t *testing.T) {
	f := newPlatformFixture(t)
	f.addUser("lisi", users.RoleEngineer, users.StatusPending)

	rec := f.do(http.MethodGet, "/api/auth/me", f.token("lisi"), nil)
	var me struct{ Status string }
	decode(t, rec, &me)
	if rec.Code != http.StatusOK || me.Status != "pending" {
		t.Fatalf("me: %d %s", rec.Code, rec.Body)
	}
}

func TestSessionExpiresSevenDaysAfterSignIn(t *testing.T) {
	f := newPlatformFixture(t)
	f.addUser("user", users.RoleEngineer, users.StatusActive)
	token := f.token("user")

	f.clock.Advance(7*24*time.Hour - time.Millisecond)
	if rec := f.do(http.MethodGet, "/api/auth/me", token, nil); rec.Code != http.StatusOK {
		t.Fatalf("me just before expiry: %d %s", rec.Code, rec.Body)
	}

	f.clock.Advance(time.Millisecond)
	expectAPIError(t, f.do(http.MethodGet, "/api/auth/me", token, nil), apiError{http.StatusUnauthorized, "unauthorized", "登录"})
}

func TestSignOutEndsTheSession(t *testing.T) {
	f := newPlatformFixture(t)
	f.addUser("user", users.RoleEngineer, users.StatusActive)
	token := f.token("user")
	other := f.token("user")

	if rec := f.do(http.MethodPost, "/api/auth/sign-out", token, nil); rec.Code != http.StatusNoContent {
		t.Fatalf("sign-out: %d %s", rec.Code, rec.Body)
	}
	expectAPIError(t, f.do(http.MethodGet, "/api/auth/me", token, nil), apiError{http.StatusUnauthorized, "unauthorized", "登录"})
	if rec := f.do(http.MethodGet, "/api/auth/me", other, nil); rec.Code != http.StatusOK {
		t.Fatalf("another session of the same user: %d %s", rec.Code, rec.Body)
	}
}

func TestCurrentUserNeedsASession(t *testing.T) {
	f := newPlatformFixture(t)
	expectAPIError(t, f.do(http.MethodGet, "/api/auth/me", "", nil), apiError{http.StatusUnauthorized, "unauthorized", "登录"})
	expectAPIError(t, f.do(http.MethodGet, "/api/auth/me", "no-such-session", nil), apiError{http.StatusUnauthorized, "unauthorized", "登录"})
}

// accessFixture is one handler per access level, and a session for each of
// its users: active admin and engineer, a pending and a rejected applicant,
// and "temp", an admin with a forced password change due.
type accessFixture struct {
	levels map[string]http.Handler
	tokens map[string]string
}

func newAccessFixture(t *testing.T) accessFixture {
	f := newPlatformFixture(t)
	f.addUser("admin", users.RoleAdmin, users.StatusActive)
	f.addUser("user", users.RoleEngineer, users.StatusActive)
	f.addUser("lisi", users.RoleEngineer, users.StatusPending)
	f.addUser("zhaoliu", users.RoleEngineer, users.StatusRejected)
	f.insert(users.NewUser{ID: "u-temp", Username: "temp", DisplayName: "temp",
		Role: users.RoleAdmin, Status: users.StatusActive, MustChangePassword: true})
	reached := func(w http.ResponseWriter, _ *http.Request, u users.User) {
		writeJSON(w, http.StatusOK, u)
	}
	tokens := map[string]string{}
	for _, name := range []string{"admin", "user", "lisi", "zhaoliu", "temp"} {
		tokens[name] = f.token(name)
	}
	return accessFixture{
		levels: map[string]http.Handler{
			"signedIn": f.api.signedIn(reached),
			"active":   f.api.active(reached),
			"admin":    f.api.admin(reached),
		},
		tokens: tokens,
	}
}

// accessProbe is a request through one access level as one of the fixture's
// users ("" for no session), and the status it must answer.
type accessProbe struct {
	level, username string
	status          int
}

func (f accessFixture) probe(t *testing.T, c accessProbe) {
	req := httptest.NewRequest(http.MethodGet, "/probe", nil)
	if c.username != "" {
		req.Header.Set("Authorization", "Bearer "+f.tokens[c.username])
	}
	rec := httptest.NewRecorder()
	f.levels[c.level].ServeHTTP(rec, req)
	if rec.Code != c.status {
		t.Fatalf("status = %d, want %d (body %s)", rec.Code, c.status, rec.Body)
	}
	if c.status == http.StatusOK {
		var u struct{ Username string }
		decode(t, rec, &u)
		if u.Username != c.username {
			t.Fatalf("handler got user %q, want %q", u.Username, c.username)
		}
	}
}

func TestAccessLevelsGuardRoutesByAccountStatusAndRole(t *testing.T) {
	f := newAccessFixture(t)
	for _, c := range []accessProbe{
		{"signedIn", "lisi", http.StatusOK},
		{"signedIn", "zhaoliu", http.StatusOK},
		{"active", "user", http.StatusOK},
		{"active", "lisi", http.StatusForbidden},
		{"active", "zhaoliu", http.StatusForbidden},
		{"signedIn", "temp", http.StatusOK},
		{"active", "temp", http.StatusForbidden},
		{"admin", "temp", http.StatusForbidden},
		{"admin", "admin", http.StatusOK},
		{"admin", "user", http.StatusForbidden},
		{"admin", "lisi", http.StatusForbidden},
		{"admin", "", http.StatusUnauthorized},
	} {
		t.Run(c.level+"/"+c.username, func(t *testing.T) { f.probe(t, c) })
	}
}
