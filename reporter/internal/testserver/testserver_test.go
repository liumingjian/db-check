package testserver

import (
	"archive/zip"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"dbcheck/reporter/internal/releases"
)

func TestSeedTimeMirrorsTheFixtureGrammar(t *testing.T) {
	now := time.Date(2026, 10, 1, 8, 0, 0, 0, time.UTC)
	cases := []struct {
		offset string
		want   time.Time
	}{
		{"now", now},
		{"now-45m", now.Add(-45 * time.Minute)},
		{"now-2h", now.Add(-2 * time.Hour)},
		{"now-1d2h3m", now.Add(-(26*time.Hour + 3*time.Minute))},
		{"now-60d23h", now.Add(-(60*24 + 23) * time.Hour)},
	}
	for _, c := range cases {
		t.Run(c.offset, func(t *testing.T) {
			got, err := seedTime(c.offset, now)
			if err != nil || !got.Equal(c.want) {
				t.Fatalf("seedTime(%q) = %v, %v; want %v", c.offset, got, err, c.want)
			}
		})
	}
	for _, bad := range []string{"", "now-", "now-3m2h", "now+1d", "2026-10-01T08:00:00Z", "now-1x"} {
		if _, err := seedTime(bad, now); err == nil {
			t.Errorf("seedTime(%q) accepted a malformed offset", bad)
		}
	}
}

type client struct {
	t      *testing.T
	server http.Handler
}

func (c client) do(method, path, token string, body any) *httptest.ResponseRecorder {
	c.t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		c.t.Fatal(err)
	}
	req := httptest.NewRequest(method, path, bytes.NewReader(raw))
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rec := httptest.NewRecorder()
	c.server.ServeHTTP(rec, req)
	return rec
}

func (c client) expect(rec *httptest.ResponseRecorder, status int) {
	c.t.Helper()
	if rec.Code != status {
		c.t.Fatalf("status = %d, want %d (body %s)", rec.Code, status, rec.Body)
	}
}

func (c client) signIn(username string) string {
	c.t.Helper()
	rec := c.do(http.MethodPost, "/api/auth/sign-in", "", map[string]string{"username": username, "password": username})
	c.expect(rec, http.StatusOK)
	var session struct{ Token string }
	if err := json.Unmarshal(rec.Body.Bytes(), &session); err != nil {
		c.t.Fatal(err)
	}
	return session.Token
}

func newTestServer(t *testing.T) client {
	t.Helper()
	s, err := New(t.TempDir(), filepath.Join("..", "..", "..", "tests", "fixtures", "console-seed.json"))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return client{t, s}
}

const seedNow = "2026-10-01T08:00:00Z"

func TestResetRestoresTheSeed(t *testing.T) {
	c := newTestServer(t)
	c.expect(c.do(http.MethodPost, "/test/reset", "", map[string]string{"now": seedNow}), http.StatusNoContent)

	token := c.signIn("admin")
	rec := c.do(http.MethodGet, "/api/auth/me", token, nil)
	c.expect(rec, http.StatusOK)
	var me struct{ ID, Role string }
	if err := json.Unmarshal(rec.Body.Bytes(), &me); err != nil {
		t.Fatal(err)
	}
	if me.ID != "u-admin-001" || me.Role != "admin" {
		t.Fatalf("seed admin = %+v", me)
	}

	c.expect(c.do(http.MethodPost, "/test/reset", "", map[string]string{"now": seedNow}), http.StatusNoContent)
	c.expect(c.do(http.MethodGet, "/api/auth/me", token, nil), http.StatusUnauthorized)
	c.signIn("admin")
}

func TestResetLoadsTheAccountActionHistory(t *testing.T) {
	c := newTestServer(t)
	c.expect(c.do(http.MethodPost, "/test/reset", "", map[string]string{"now": seedNow}), http.StatusNoContent)

	rec := c.do(http.MethodGet, "/api/users", c.signIn("admin"), nil)
	c.expect(rec, http.StatusOK)
	var list []struct {
		Username string
		Actions  []struct{ Action, By, At string }
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &list); err != nil {
		t.Fatal(err)
	}
	for _, u := range list {
		if u.Username != "wangwu" {
			continue
		}
		if len(u.Actions) != 2 || u.Actions[0].Action != "approve" || u.Actions[1].Action != "disable" ||
			u.Actions[1].By != "admin" || u.Actions[1].At != "2026-09-10T10:00:00.000Z" {
			t.Fatalf("wangwu's actions = %+v", u.Actions)
		}
		return
	}
	t.Fatal("wangwu is not listed")
}

func TestClockStaysPinnedUntilMoved(t *testing.T) {
	c := newTestServer(t)
	c.expect(c.do(http.MethodPost, "/test/reset", "", map[string]string{"now": seedNow}), http.StatusNoContent)
	token := c.signIn("user")

	c.expect(c.do(http.MethodPost, "/test/clock", "", map[string]string{"now": "2026-10-08T07:59:59Z"}), http.StatusNoContent)
	c.expect(c.do(http.MethodGet, "/api/auth/me", token, nil), http.StatusOK)

	c.expect(c.do(http.MethodPost, "/test/clock", "", map[string]string{"now": "2026-10-08T08:00:00Z"}), http.StatusNoContent)
	c.expect(c.do(http.MethodGet, "/api/auth/me", token, nil), http.StatusUnauthorized)
}

func TestResetRefusesAMissingTime(t *testing.T) {
	c := newTestServer(t)
	c.expect(c.do(http.MethodPost, "/test/reset", "", map[string]string{}), http.StatusBadRequest)
}

func TestResetSeedsReleasesAndTheirPackageFiles(t *testing.T) {
	c := newTestServer(t)
	c.expect(c.do(http.MethodPost, "/test/reset", "", map[string]string{"now": seedNow}), http.StatusNoContent)

	rec := c.do(http.MethodGet, "/api/releases", c.signIn("admin"), nil)
	c.expect(rec, http.StatusOK)
	var list []releases.Release
	if err := json.Unmarshal(rec.Body.Bytes(), &list); err != nil {
		t.Fatal(err)
	}
	if len(list) != 4 || list[0].Version != "1.3.0-rc1" || list[3].RevokeReason == "" {
		t.Fatalf("seed releases = %+v", list)
	}
	if list[0].PublishedAt != "2026-09-28T12:11:00.000Z" {
		t.Fatalf("1.3.0-rc1 publishedAt = %s", list[0].PublishedAt)
	}

	// Package files survive a second reset, and each is a real zip with
	// the size and SHA256 the fixture lists.
	c.expect(c.do(http.MethodPost, "/test/reset", "", map[string]string{"now": seedNow}), http.StatusNoContent)
	dataDir := c.server.(*Server).dataDir
	for _, r := range list {
		for _, p := range r.Packages {
			raw, err := os.ReadFile(releases.PackagePath(dataDir, r.Version, p.Platform))
			if err != nil {
				t.Fatal(err)
			}
			sum := sha256.Sum256(raw)
			if got := hex.EncodeToString(sum[:]); int64(len(raw)) != p.Size || got != p.SHA256 {
				t.Errorf("%s: size %d sha256 %s; the fixture says %d %s", p.FileName, len(raw), got, p.Size, p.SHA256)
			}
			if _, err := zip.NewReader(bytes.NewReader(raw), int64(len(raw))); err != nil {
				t.Errorf("%s is not a zip: %v", p.FileName, err)
			}
		}
	}
}

func TestResetRemovesPackageFilesOutsideTheSeed(t *testing.T) {
	c := newTestServer(t)
	c.expect(c.do(http.MethodPost, "/test/reset", "", map[string]string{"now": seedNow}), http.StatusNoContent)
	stray := filepath.Join(releases.Dir(c.server.(*Server).dataDir), "9.9.9")
	if err := os.MkdirAll(stray, 0o755); err != nil {
		t.Fatal(err)
	}
	c.expect(c.do(http.MethodPost, "/test/reset", "", map[string]string{"now": seedNow}), http.StatusNoContent)
	if _, err := os.Stat(stray); !os.IsNotExist(err) {
		t.Fatalf("reset kept %s: %v", stray, err)
	}
}
