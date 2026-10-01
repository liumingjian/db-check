package web

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"
	"time"

	"dbcheck/reporter/internal/users"
)

// downloadFixture has four releases, one of each status an engineer may or
// may not download: 1.1.0 deprecated, 1.2.0 latest, 1.3.0-rc1 pre-release,
// and 1.0.0 revoked. It has an active engineer "user" and an admin "admin".
type downloadFixture struct {
	*platformFixture
	published map[string]testRelease
}

func newDownloadFixture(t *testing.T) downloadFixture {
	t.Helper()
	f := downloadFixture{newPlatformFixture(t), map[string]testRelease{}}
	f.enablePublish()
	f.addUser("admin", users.RoleAdmin, users.StatusActive)
	f.addUser("user", users.RoleEngineer, users.StatusActive)
	for _, v := range []string{"1.0.0", "1.1.0", "1.2.0", "1.3.0-rc1"} {
		r := newTestRelease(t, v)
		f.mustPublish(r)
		f.published[v] = r
	}
	rec := f.do(http.MethodPost, "/api/releases/1.0.0/revoke", f.token("admin"), map[string]string{"reason": "采集结果有误"})
	if rec.Code != http.StatusNoContent {
		t.Fatalf("revoke 1.0.0: %d %s", rec.Code, rec.Body)
	}
	return f
}

func (f downloadFixture) download(token, version, platform string) *httptest.ResponseRecorder {
	return f.do(http.MethodGet, "/api/releases/"+version+"/packages/"+platform, token, nil)
}

type listedRecord struct{ ID, UserID, Version, Platform, At string }

func (f downloadFixture) records(query string) []listedRecord {
	f.t.Helper()
	rec := f.do(http.MethodGet, "/api/downloads"+query, f.token("admin"), nil)
	if rec.Code != http.StatusOK {
		f.t.Fatalf("download records: %d %s", rec.Code, rec.Body)
	}
	var list []listedRecord
	decode(f.t, rec, &list)
	return list
}

func TestDownloadStreamsThePackageAndRecordsIt(t *testing.T) {
	f := newDownloadFixture(t)
	rec := f.download(f.token("user"), "1.2.0", "windows-arm64")
	if rec.Code != http.StatusOK {
		t.Fatalf("download: %d %s", rec.Code, rec.Body)
	}
	want := f.published["1.2.0"].Packages[3]
	if !bytes.Equal(rec.Body.Bytes(), want.content) {
		t.Fatalf("downloaded %d bytes, not the published package", rec.Body.Len())
	}
	if got := rec.Header().Get("Content-Disposition"); got != `attachment; filename="db-collector-1.2.0-windows-arm64.zip"` {
		t.Fatalf("Content-Disposition = %q", got)
	}
	if got := rec.Header().Get("Content-Type"); got != "application/zip" {
		t.Fatalf("Content-Type = %q", got)
	}

	list := f.records("")
	if len(list) != 1 || list[0].UserID != "u-user" || list[0].Version != "1.2.0" ||
		list[0].Platform != "windows-arm64" || list[0].At != "2026-10-01T08:00:00.000Z" || list[0].ID == "" {
		t.Fatalf("records = %+v", list)
	}
}

func TestDownloadRefusesHiddenOrUnknownPackagesAndRecordsNothing(t *testing.T) {
	cases := []struct{ name, user, version, platform string }{
		{"engineer, pre-release", "user", "1.3.0-rc1", "linux-amd64"},
		{"engineer, revoked", "user", "1.0.0", "linux-amd64"},
		{"unknown version", "admin", "9.9.9", "linux-amd64"},
		{"unknown platform", "admin", "1.2.0", "darwin-arm64"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := newDownloadFixture(t)
			rec := f.download(f.token(c.user), c.version, c.platform)
			expectAPIError(t, rec, http.StatusNotFound, "not_found", c.version)
			if list := f.records(""); len(list) != 0 {
				t.Fatalf("a refused download wrote records: %+v", list)
			}
		})
	}
}

func TestAdminDownloadsEveryReleaseStatus(t *testing.T) {
	f := newDownloadFixture(t)
	for _, v := range []string{"1.3.0-rc1", "1.2.0", "1.1.0", "1.0.0"} {
		rec := f.download(f.token("admin"), v, "linux-arm64")
		if rec.Code != http.StatusOK || !bytes.Equal(rec.Body.Bytes(), f.published[v].Packages[1].content) {
			t.Fatalf("admin download of %s: %d", v, rec.Code)
		}
	}
	if got := len(f.records("")); got != 4 {
		t.Fatalf("records = %d, want 4", got)
	}
}

func TestDownloadNeedsAnActiveSession(t *testing.T) {
	f := newDownloadFixture(t)
	expectAPIError(t, f.download("", "1.2.0", "linux-amd64"), http.StatusUnauthorized, "unauthorized", "")
	f.addUser("lisi", users.RoleEngineer, users.StatusPending)
	expectAPIError(t, f.download(f.token("lisi"), "1.2.0", "linux-amd64"), http.StatusForbidden, "forbidden", "未启用")
}

func TestDownloadRecordsAreForAdminsOnly(t *testing.T) {
	f := newDownloadFixture(t)
	rec := f.do(http.MethodGet, "/api/downloads", f.token("user"), nil)
	expectAPIError(t, rec, http.StatusForbidden, "forbidden", "管理员")
}

func TestDownloadRecordsFilterNewestFirst(t *testing.T) {
	f := newDownloadFixture(t)
	f.addUser("zhangsan", users.RoleEngineer, users.StatusActive)
	steps := []struct{ at, user, version, platform string }{
		{"2026-10-01T09:00:00Z", "user", "1.1.0", "linux-amd64"},
		{"2026-10-02T09:00:00Z", "zhangsan", "1.2.0", "linux-amd64"},
		{"2026-10-03T09:00:00Z", "user", "1.2.0", "windows-amd64"},
		{"2026-10-04T09:00:00Z", "user", "1.2.0", "linux-arm64"},
	}
	for _, s := range steps {
		at, err := time.Parse(time.RFC3339, s.at)
		if err != nil {
			t.Fatal(err)
		}
		f.clock.Advance(at.Sub(f.clock.Now()))
		if rec := f.download(f.token(s.user), s.version, s.platform); rec.Code != http.StatusOK {
			t.Fatalf("download at %s: %d %s", s.at, rec.Code, rec.Body)
		}
	}
	cases := []struct {
		query string
		want  []string // platforms, newest first
	}{
		{"", []string{"linux-arm64", "windows-amd64", "linux-amd64", "linux-amd64"}},
		{"?userId=u-user", []string{"linux-arm64", "windows-amd64", "linux-amd64"}},
		{"?version=1.2.0", []string{"linux-arm64", "windows-amd64", "linux-amd64"}},
		{"?from=2026-10-02T09:00:00Z&to=2026-10-04T09:00:00Z", []string{"windows-amd64", "linux-amd64"}},
		{"?from=2026-10-03T09:00:00.000Z", []string{"linux-arm64", "windows-amd64"}},
		{"?userId=u-user&version=1.2.0&to=2026-10-04T00:00:00Z", []string{"windows-amd64"}},
		{"?userId=u-nobody", nil},
	}
	for _, c := range cases {
		var got []string
		for _, r := range f.records(c.query) {
			got = append(got, r.Platform)
		}
		if !slices.Equal(got, c.want) {
			t.Errorf("records%s = %v, want %v", c.query, got, c.want)
		}
	}
}

func TestDownloadRecordsRefuseAMalformedTime(t *testing.T) {
	f := newDownloadFixture(t)
	rec := f.do(http.MethodGet, "/api/downloads?from=yesterday", f.token("admin"), nil)
	expectAPIError(t, rec, http.StatusBadRequest, "invalid", "yesterday")
}
