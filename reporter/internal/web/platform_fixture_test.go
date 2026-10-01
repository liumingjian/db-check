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

// apiError is an expected error envelope: HTTP status, contract code, and
// a part of the message.
type apiError struct {
	status  int
	code    string
	message string
}

// expectAPIError checks that rec answers the error envelope want.
func expectAPIError(t *testing.T, rec *httptest.ResponseRecorder, want apiError) {
	t.Helper()
	if rec.Code != want.status {
		t.Fatalf("status = %d, want %d (body %s)", rec.Code, want.status, rec.Body)
	}
	var body struct{ Code, Message string }
	decode(t, rec, &body)
	if body.Code != want.code || !strings.Contains(body.Message, want.message) {
		t.Fatalf("error = %+v, want code %q and a message containing %q", body, want.code, want.message)
	}
}
