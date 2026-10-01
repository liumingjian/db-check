package web

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"dbcheck/reporter/internal/store"
	"dbcheck/reporter/internal/users"

	"golang.org/x/crypto/bcrypt"
)

// testClock is a settable clock for the platform under test.
type testClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *testClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *testClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// platformFixture is a handler over a fresh store, with a pinned clock and a
// captured log.
type platformFixture struct {
	t       *testing.T
	api     *apiHandler
	handler http.Handler
	db      *store.DB
	clock   *testClock
	log     *bytes.Buffer
}

// testPlatform opens a store in dataDir, closed when the test ends.
func testPlatform(t *testing.T, dataDir string) Platform {
	t.Helper()
	db, err := store.Open(dataDir)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return Platform{DB: db}
}

func newPlatformFixture(t *testing.T) *platformFixture {
	t.Helper()
	dataDir := t.TempDir()
	p := testPlatform(t, dataDir)
	f := &platformFixture{
		t:     t,
		db:    p.DB,
		clock: &testClock{now: time.Date(2026, 10, 1, 8, 0, 0, 0, time.UTC)},
		log:   &bytes.Buffer{},
	}
	p.Now = f.clock.Now
	p.Log = log.New(f.log, "", 0)
	cfg := Config{DataDir: dataDir, AllowedOrigins: []string{"http://example.com"}}
	h, err := newAPIHandler(cfg, p, false)
	if err != nil {
		t.Fatalf("newAPIHandler: %v", err)
	}
	f.api = h
	f.handler = h.handler()
	return f
}

// addUser stores a user whose password is their username.
func (f *platformFixture) addUser(username string, role users.Role, status users.Status) {
	f.t.Helper()
	f.insert(users.NewUser{ID: "u-" + username, Username: username, DisplayName: username, Role: role, Status: status})
}

// insert stores u with its username as password and the clock's time as
// application time.
func (f *platformFixture) insert(u users.NewUser) {
	f.t.Helper()
	hash, err := bcrypt.GenerateFromPassword([]byte(u.Username), bcrypt.MinCost)
	if err != nil {
		f.t.Fatal(err)
	}
	u.PasswordHash, u.AppliedAt = string(hash), f.clock.Now()
	if err := users.Insert(context.Background(), f.db, u); err != nil {
		f.t.Fatalf("users.Insert: %v", err)
	}
}

func (f *platformFixture) do(method, path, token string, body any) *httptest.ResponseRecorder {
	f.t.Helper()
	var raw []byte
	if body != nil {
		var err error
		if raw, err = json.Marshal(body); err != nil {
			f.t.Fatal(err)
		}
	}
	req := httptest.NewRequest(method, path, bytes.NewReader(raw))
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rec := httptest.NewRecorder()
	f.handler.ServeHTTP(rec, req)
	return rec
}

func (f *platformFixture) signIn(username, password string) *httptest.ResponseRecorder {
	return f.do(http.MethodPost, "/api/auth/sign-in", "", map[string]string{"username": username, "password": password})
}

func (f *platformFixture) token(username string) string {
	f.t.Helper()
	rec := f.signIn(username, username)
	if rec.Code != http.StatusOK {
		f.t.Fatalf("sign-in %s: %d %s", username, rec.Code, rec.Body)
	}
	var session struct{ Token string }
	decode(f.t, rec, &session)
	return session.Token
}

func decode(t *testing.T, rec *httptest.ResponseRecorder, into any) {
	t.Helper()
	if err := json.Unmarshal(rec.Body.Bytes(), into); err != nil {
		t.Fatalf("decode %q: %v", rec.Body, err)
	}
}

// expectAPIError checks the error envelope: status, contract code, and a
// message containing want.
func expectAPIError(t *testing.T, rec *httptest.ResponseRecorder, status int, code, want string) {
	t.Helper()
	if rec.Code != status {
		t.Fatalf("status = %d, want %d (body %s)", rec.Code, status, rec.Body)
	}
	var body struct{ Code, Message string }
	decode(t, rec, &body)
	if body.Code != code || !strings.Contains(body.Message, want) {
		t.Fatalf("error = %+v, want code %q and a message containing %q", body, code, want)
	}
}

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

	expectAPIError(t, f.signIn("user", "nope"), http.StatusUnauthorized, "unauthorized", "用户名或密码错误")
	expectAPIError(t, f.signIn("nobody", "nope"), http.StatusUnauthorized, "unauthorized", "用户名或密码错误")

	for _, name := range []string{`"user"`, `"nobody"`} {
		if !strings.Contains(f.log.String(), name) {
			t.Fatalf("log %q does not record the failed sign-in for %s", f.log, name)
		}
	}
}

func TestSignInRefusesADisabledUser(t *testing.T) {
	f := newPlatformFixture(t)
	f.addUser("wangwu", users.RoleEngineer, users.StatusDisabled)

	expectAPIError(t, f.signIn("wangwu", "wangwu"), http.StatusForbidden, "forbidden", "禁用")
	// A wrong password says nothing about the account status.
	expectAPIError(t, f.signIn("wangwu", "nope"), http.StatusUnauthorized, "unauthorized", "用户名或密码错误")
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
	expectAPIError(t, f.do(http.MethodGet, "/api/auth/me", token, nil), http.StatusUnauthorized, "unauthorized", "登录")
}

func TestSignOutEndsTheSession(t *testing.T) {
	f := newPlatformFixture(t)
	f.addUser("user", users.RoleEngineer, users.StatusActive)
	token := f.token("user")
	other := f.token("user")

	if rec := f.do(http.MethodPost, "/api/auth/sign-out", token, nil); rec.Code != http.StatusNoContent {
		t.Fatalf("sign-out: %d %s", rec.Code, rec.Body)
	}
	expectAPIError(t, f.do(http.MethodGet, "/api/auth/me", token, nil), http.StatusUnauthorized, "unauthorized", "登录")
	if rec := f.do(http.MethodGet, "/api/auth/me", other, nil); rec.Code != http.StatusOK {
		t.Fatalf("another session of the same user: %d %s", rec.Code, rec.Body)
	}
}

func TestCurrentUserNeedsASession(t *testing.T) {
	f := newPlatformFixture(t)
	expectAPIError(t, f.do(http.MethodGet, "/api/auth/me", "", nil), http.StatusUnauthorized, "unauthorized", "登录")
	expectAPIError(t, f.do(http.MethodGet, "/api/auth/me", "no-such-session", nil), http.StatusUnauthorized, "unauthorized", "登录")
}

func TestAccessLevelsGuardRoutesByAccountStatusAndRole(t *testing.T) {
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
	levels := map[string]http.Handler{
		"signedIn": f.api.signedIn(reached),
		"active":   f.api.active(reached),
		"admin":    f.api.admin(reached),
	}
	tokens := map[string]string{
		"admin": f.token("admin"), "user": f.token("user"),
		"lisi": f.token("lisi"), "zhaoliu": f.token("zhaoliu"), "temp": f.token("temp"),
	}

	cases := []struct {
		level, username string
		status          int
	}{
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
	}
	for _, c := range cases {
		t.Run(c.level+"/"+c.username, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/probe", nil)
			if c.username != "" {
				req.Header.Set("Authorization", "Bearer "+tokens[c.username])
			}
			rec := httptest.NewRecorder()
			levels[c.level].ServeHTTP(rec, req)
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
		})
	}
}
