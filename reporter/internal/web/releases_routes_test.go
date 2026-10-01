package web

import (
	"archive/zip"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"dbcheck/reporter/internal/users"
)

const testPublishToken = "ci-publish-secret"

var testPlatforms = []string{"linux-amd64", "linux-arm64", "windows-amd64", "windows-arm64"}

// testPackage is one release package as the publish request carries it.
type testPackage struct {
	Platform string `json:"platform"`
	Size     int64  `json:"size"`
	SHA256   string `json:"sha256"`
	content  []byte
	name     string // the uploaded file name
}

// testRelease is a publish request: metadata plus the package files.
type testRelease struct {
	Version     string        `json:"version"`
	Tag         string        `json:"tag"`
	Commit      string        `json:"commit"`
	PublishedAt string        `json:"publishedAt"`
	Notes       string        `json:"notes"`
	DBTypes     []string      `json:"dbTypes"`
	Packages    []testPackage `json:"packages"`
}

func zipBytes(t *testing.T, body string) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	w, err := zw.Create("db-collector/README.txt")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte(body)); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func packageFor(t *testing.T, version, platform string, content []byte) testPackage {
	t.Helper()
	sum := sha256.Sum256(content)
	return testPackage{
		Platform: platform,
		Size:     int64(len(content)),
		SHA256:   hex.EncodeToString(sum[:]),
		content:  content,
		name:     fmt.Sprintf("db-collector-%s-%s.zip", version, platform),
	}
}

// newTestRelease is a valid publish request for version, its packages
// distinct per platform.
func newTestRelease(t *testing.T, version string) testRelease {
	t.Helper()
	r := testRelease{
		Version: version, Tag: "v" + version, Commit: "abc1234",
		PublishedAt: "2026-10-01T07:00:00.000Z", Notes: "- 新功能",
		DBTypes: []string{"mysql", "oracle", "gaussdb"},
	}
	for _, p := range testPlatforms {
		r.Packages = append(r.Packages, packageFor(t, version, p, zipBytes(t, version+" "+p)))
	}
	return r
}

func (r testRelease) withPackage(p testPackage) testRelease {
	pkgs := append([]testPackage(nil), r.Packages...)
	for i := range pkgs {
		if pkgs[i].Platform == p.Platform {
			pkgs[i] = p
		}
	}
	r.Packages = pkgs
	return r
}

// enablePublish configures the publish credential.
func (f *platformFixture) enablePublish() {
	f.api.cfg.PublishToken = testPublishToken
}

// publishRequest builds the multipart publish request: a `metadata` JSON
// part, then one file part per package, named by platform.
func publishRequest(t *testing.T, r testRelease, token string) *http.Request {
	t.Helper()
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	meta, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	if err := mw.WriteField("metadata", string(meta)); err != nil {
		t.Fatal(err)
	}
	for _, p := range r.Packages {
		if p.content == nil {
			continue
		}
		w, err := mw.CreateFormFile(p.Platform, p.name)
		if err != nil {
			t.Fatal(err)
		}
		w.Write(p.content)
	}
	mw.Close()
	req := httptest.NewRequest(http.MethodPost, "/api/ci/releases", &buf)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	return req
}

func (f *platformFixture) publish(r testRelease) *httptest.ResponseRecorder {
	f.t.Helper()
	return f.serve(publishRequest(f.t, r, testPublishToken))
}

func (f *platformFixture) serve(req *http.Request) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	f.handler.ServeHTTP(rec, req)
	return rec
}

func (f *platformFixture) mustPublish(r testRelease) {
	f.t.Helper()
	if rec := f.publish(r); rec.Code != http.StatusCreated {
		f.t.Fatalf("publish %s: %d %s", r.Version, rec.Code, rec.Body)
	}
}

type listedRelease struct {
	Version      string
	Tag          string
	Status       string
	RevokeReason *string
	DBTypes      []string
	Packages     []struct {
		Platform, OS, Arch, FileName, SHA256 string
		Size                                 int64
	}
}

func (f *platformFixture) releases(token string) []listedRelease {
	f.t.Helper()
	rec := f.do(http.MethodGet, "/api/releases", token, nil)
	if rec.Code != http.StatusOK {
		f.t.Fatalf("list releases: %d %s", rec.Code, rec.Body)
	}
	var list []listedRelease
	decode(f.t, rec, &list)
	return list
}

func (f *platformFixture) statuses(token string) map[string]string {
	f.t.Helper()
	out := map[string]string{}
	for _, r := range f.releases(token) {
		out[r.Version] = r.Status
	}
	return out
}

func TestPublishValidation(t *testing.T) {
	notZip := []byte("this is not a zip archive")
	cases := []struct {
		name    string
		release func(t *testing.T) testRelease
		token   string
		status  int
		code    string
		message string
	}{
		{
			name: "checksum mismatch",
			release: func(t *testing.T) testRelease {
				r := newTestRelease(t, "1.0.0")
				p := r.Packages[1]
				p.SHA256 = hex.EncodeToString(make([]byte, 32))
				return r.withPackage(p)
			},
			status: http.StatusBadRequest, code: "invalid", message: "SHA256",
		},
		{
			name: "size mismatch",
			release: func(t *testing.T) testRelease {
				r := newTestRelease(t, "1.0.0")
				p := r.Packages[2]
				p.Size++
				return r.withPackage(p)
			},
			status: http.StatusBadRequest, code: "invalid", message: "大小",
		},
		{
			name: "missing platform in metadata and files",
			release: func(t *testing.T) testRelease {
				r := newTestRelease(t, "1.0.0")
				r.Packages = r.Packages[:3]
				return r
			},
			status: http.StatusBadRequest, code: "invalid", message: "windows-arm64",
		},
		{
			name: "missing package file",
			release: func(t *testing.T) testRelease {
				r := newTestRelease(t, "1.0.0")
				p := r.Packages[3]
				p.content = nil
				return r.withPackage(p)
			},
			status: http.StatusBadRequest, code: "invalid", message: "windows-arm64",
		},
		{
			name: "extra platform",
			release: func(t *testing.T) testRelease {
				r := newTestRelease(t, "1.0.0")
				r.Packages = append(r.Packages, packageFor(t, "1.0.0", "darwin-arm64", zipBytes(t, "mac")))
				return r
			},
			status: http.StatusBadRequest, code: "invalid", message: "darwin-arm64",
		},
		{
			name: "package that is not a zip",
			release: func(t *testing.T) testRelease {
				r := newTestRelease(t, "1.0.0")
				return r.withPackage(packageFor(t, "1.0.0", "linux-arm64", notZip))
			},
			status: http.StatusBadRequest, code: "invalid", message: "zip",
		},
		{
			name: "package file without the .zip name",
			release: func(t *testing.T) testRelease {
				r := newTestRelease(t, "1.0.0")
				p := r.Packages[0]
				p.name = "db-collector-1.0.0-linux-amd64.tar.gz"
				return r.withPackage(p)
			},
			status: http.StatusBadRequest, code: "invalid", message: ".zip",
		},
		{
			name: "tag that does not match the version",
			release: func(t *testing.T) testRelease {
				r := newTestRelease(t, "1.0.0")
				r.Tag = "v1.0.1"
				return r
			},
			status: http.StatusBadRequest, code: "invalid", message: "标签",
		},
		{
			name: "version that is neither vX.Y.Z nor vX.Y.Z-rcN",
			release: func(t *testing.T) testRelease {
				r := newTestRelease(t, "1.0.0-beta")
				return r
			},
			status: http.StatusBadRequest, code: "invalid", message: "版本号",
		},
		{
			name: "unsupported database type",
			release: func(t *testing.T) testRelease {
				r := newTestRelease(t, "1.0.0")
				r.DBTypes = []string{"mysql", "db2"}
				return r
			},
			status: http.StatusBadRequest, code: "invalid", message: "db2",
		},
		{
			name:    "missing credential",
			release: func(t *testing.T) testRelease { return newTestRelease(t, "1.0.0") },
			token:   "-",
			status:  http.StatusUnauthorized, code: "unauthorized", message: "发布凭证",
		},
		{
			name:    "wrong credential",
			release: func(t *testing.T) testRelease { return newTestRelease(t, "1.0.0") },
			token:   "not-the-secret",
			status:  http.StatusUnauthorized, code: "unauthorized", message: "发布凭证",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := newPlatformFixture(t)
			f.enablePublish()
			token := testPublishToken
			if c.token == "-" {
				token = ""
			} else if c.token != "" {
				token = c.token
			}
			rec := f.serve(publishRequest(t, c.release(t), token))
			expectAPIError(t, rec, c.status, c.code, c.message)

			f.addUser("admin", users.RoleAdmin, users.StatusActive)
			if list := f.releases(f.token("admin")); len(list) != 0 {
				t.Fatalf("a refused publish left releases behind: %+v", list)
			}
			assertNoPackageFiles(t, f.api.cfg.DataDir)
		})
	}
}

// assertNoPackageFiles checks that nothing, not even a staged upload, is
// left under the releases area of the data directory.
func assertNoPackageFiles(t *testing.T, dataDir string) {
	t.Helper()
	filepath.Walk(filepath.Join(dataDir, "releases"), func(path string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			t.Errorf("left behind %s", path)
		}
		return nil
	})
}

func TestPublishRefusesAUserSession(t *testing.T) {
	f := newPlatformFixture(t)
	f.enablePublish()
	f.addUser("admin", users.RoleAdmin, users.StatusActive)
	rec := f.serve(publishRequest(t, newTestRelease(t, "1.0.0"), f.token("admin")))
	expectAPIError(t, rec, http.StatusUnauthorized, "unauthorized", "发布凭证")
}

func TestConfigReadsThePublishTokenFromTheEnvironment(t *testing.T) {
	env := map[string]string{"DBCHECK_DATA_DIR": t.TempDir(), "ALLOWED_ORIGINS": "http://example.com"}
	for value, want := range map[string]string{"": "", "  s3cret \n": "s3cret"} {
		env["DBCHECK_PUBLISH_TOKEN"] = value
		cfg, err := ParseConfig(nil, func(k string) string { return env[k] })
		if err != nil || cfg.PublishToken != want {
			t.Fatalf("PublishToken from %q = %q, %v; want %q", value, cfg.PublishToken, err, want)
		}
	}
}

func TestPublishIsDisabledWithoutAConfiguredToken(t *testing.T) {
	f := newPlatformFixture(t)
	for _, token := range []string{"", testPublishToken} {
		rec := f.serve(publishRequest(t, newTestRelease(t, "1.0.0"), token))
		expectAPIError(t, rec, http.StatusForbidden, "forbidden", "未启用")
	}
}

func TestPublishStoresTheReleaseAndItsPackages(t *testing.T) {
	f := newPlatformFixture(t)
	f.enablePublish()
	r := newTestRelease(t, "1.2.0")
	f.mustPublish(r)

	f.addUser("user", users.RoleEngineer, users.StatusActive)
	list := f.releases(f.token("user"))
	if len(list) != 1 || list[0].Version != "1.2.0" || list[0].Tag != "v1.2.0" || list[0].Status != "latest" {
		t.Fatalf("releases = %+v", list)
	}
	if len(list[0].DBTypes) != 3 {
		t.Fatalf("dbTypes = %v", list[0].DBTypes)
	}
	for i, p := range list[0].Packages {
		want := r.Packages[i]
		if p.Platform != want.Platform || p.SHA256 != want.SHA256 || p.Size != want.Size || p.FileName != want.name {
			t.Fatalf("package %d = %+v, want %+v", i, p, want)
		}
		stored, err := os.ReadFile(filepath.Join(f.api.cfg.DataDir, "releases", "1.2.0", want.name))
		if err != nil || !bytes.Equal(stored, want.content) {
			t.Fatalf("stored %s: %v (equal %v)", want.name, err, bytes.Equal(stored, want.content))
		}
	}
	if p := list[0].Packages[3]; p.OS != "Windows" || p.Arch != "ARM64" {
		t.Fatalf("windows-arm64 labels = %s / %s", p.OS, p.Arch)
	}
}

func TestRepublishingIdenticalContentSucceeds(t *testing.T) {
	f := newPlatformFixture(t)
	f.enablePublish()
	r := newTestRelease(t, "1.2.0")
	f.mustPublish(r)

	rec := f.publish(r)
	if rec.Code != http.StatusOK {
		t.Fatalf("identical re-publish: %d %s", rec.Code, rec.Body)
	}
	f.addUser("admin", users.RoleAdmin, users.StatusActive)
	if got := f.statuses(f.token("admin")); len(got) != 1 || got["1.2.0"] != "latest" {
		t.Fatalf("statuses = %v", got)
	}
	assertOnlyReleaseDirs(t, f.api.cfg.DataDir, "1.2.0")
}

func TestRepublishingDifferentContentConflicts(t *testing.T) {
	f := newPlatformFixture(t)
	f.enablePublish()
	r := newTestRelease(t, "1.2.0")
	f.mustPublish(r)

	changed := r.withPackage(packageFor(t, "1.2.0", "linux-amd64", zipBytes(t, "rebuilt")))
	expectAPIError(t, f.publish(changed), http.StatusConflict, "invalid", "1.2.0")

	otherCommit := r
	otherCommit.Commit = "fff0000"
	expectAPIError(t, f.publish(otherCommit), http.StatusConflict, "invalid", "1.2.0")

	stored, err := os.ReadFile(filepath.Join(f.api.cfg.DataDir, "releases", "1.2.0", r.Packages[0].name))
	if err != nil || !bytes.Equal(stored, r.Packages[0].content) {
		t.Fatalf("a conflicting publish changed the stored package: %v", err)
	}
	assertOnlyReleaseDirs(t, f.api.cfg.DataDir, "1.2.0")
}

// assertOnlyReleaseDirs checks the releases area holds exactly the given
// versions' directories: no staged uploads are left.
func assertOnlyReleaseDirs(t *testing.T, dataDir string, versions ...string) {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(dataDir, "releases"))
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	if fmt.Sprint(names) != fmt.Sprint(versions) {
		t.Fatalf("releases dir = %v, want %v", names, versions)
	}
}

func TestPublishingAFinalReleaseDemotesThePreviousLatest(t *testing.T) {
	f := newPlatformFixture(t)
	f.enablePublish()
	f.addUser("admin", users.RoleAdmin, users.StatusActive)
	token := f.token("admin")

	f.mustPublish(newTestRelease(t, "1.1.0"))
	f.mustPublish(newTestRelease(t, "1.2.0"))
	f.mustPublish(newTestRelease(t, "1.3.0-rc1"))

	want := map[string]string{"1.1.0": "deprecated", "1.2.0": "latest", "1.3.0-rc1": "pre-release"}
	if got := f.statuses(token); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("statuses = %v, want %v", got, want)
	}
}

func TestPromotingDemotesThePreviousLatest(t *testing.T) {
	f := newPlatformFixture(t)
	f.enablePublish()
	f.addUser("admin", users.RoleAdmin, users.StatusActive)
	token := f.token("admin")
	f.mustPublish(newTestRelease(t, "1.2.0"))
	f.mustPublish(newTestRelease(t, "1.3.0-rc1"))

	rec := f.do(http.MethodPost, "/api/releases/1.3.0-rc1/promote", token, nil)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("promote: %d %s", rec.Code, rec.Body)
	}
	want := map[string]string{"1.2.0": "deprecated", "1.3.0-rc1": "latest"}
	if got := f.statuses(token); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("statuses = %v, want %v", got, want)
	}
}

func TestReleaseListFollowsTheRole(t *testing.T) {
	f := newPlatformFixture(t)
	f.enablePublish()
	f.addUser("admin", users.RoleAdmin, users.StatusActive)
	f.addUser("user", users.RoleEngineer, users.StatusActive)
	admin := f.token("admin")
	for _, v := range []string{"1.0.0", "1.1.0", "1.2.0", "1.3.0-rc1"} {
		r := newTestRelease(t, v)
		r.PublishedAt = fmt.Sprintf("2026-09-0%cT08:00:00.000Z", v[2]+1)
		f.mustPublish(r)
	}
	rec := f.do(http.MethodPost, "/api/releases/1.0.0/revoke", admin, map[string]string{"reason": " 锁表 "})
	if rec.Code != http.StatusNoContent {
		t.Fatalf("revoke: %d %s", rec.Code, rec.Body)
	}

	var versions []string
	for _, r := range f.releases(admin) {
		versions = append(versions, r.Version+":"+r.Status)
		if r.Version == "1.0.0" && (r.RevokeReason == nil || *r.RevokeReason != "锁表") {
			t.Fatalf("revoke reason = %v", r.RevokeReason)
		}
	}
	if want := "[1.3.0-rc1:pre-release 1.2.0:latest 1.1.0:deprecated 1.0.0:revoked]"; fmt.Sprint(versions) != want {
		t.Fatalf("admin list = %v, want %s", versions, want)
	}
	versions = nil
	for _, r := range f.releases(f.token("user")) {
		versions = append(versions, r.Version)
	}
	if fmt.Sprint(versions) != "[1.2.0 1.1.0]" {
		t.Fatalf("engineer list = %v", versions)
	}
}

func TestReleaseStatusActionsFollowTheTable(t *testing.T) {
	f := newPlatformFixture(t)
	f.enablePublish()
	f.addUser("admin", users.RoleAdmin, users.StatusActive)
	f.addUser("user", users.RoleEngineer, users.StatusActive)
	admin := f.token("admin")
	f.mustPublish(newTestRelease(t, "1.2.0"))

	expectAPIError(t, f.do(http.MethodPost, "/api/releases/1.2.0/deprecate", f.token("user"), nil), http.StatusForbidden, "forbidden", "管理员")
	expectAPIError(t, f.do(http.MethodPost, "/api/releases/9.9.9/promote", admin, nil), http.StatusNotFound, "not_found", "9.9.9")
	expectAPIError(t, f.do(http.MethodPost, "/api/releases/1.2.0/restore", admin, nil), http.StatusBadRequest, "invalid", "1.2.0")
	expectAPIError(t, f.do(http.MethodPost, "/api/releases/1.2.0/revoke", admin, map[string]string{"reason": "  "}), http.StatusBadRequest, "invalid", "原因")
	expectAPIError(t, f.do(http.MethodPost, "/api/releases/1.2.0/archive", admin, nil), http.StatusNotFound, "not_found", "")

	steps := []struct{ action, want string }{
		{"deprecate", "deprecated"}, // no latest release is valid
		{"revoke", "revoked"},
		{"restore", "deprecated"},
		{"promote", "latest"},
	}
	for _, s := range steps {
		rec := f.do(http.MethodPost, "/api/releases/1.2.0/"+s.action, admin, map[string]string{"reason": "原因"})
		if rec.Code != http.StatusNoContent {
			t.Fatalf("%s: %d %s", s.action, rec.Code, rec.Body)
		}
		if got := f.statuses(admin)["1.2.0"]; got != s.want {
			t.Fatalf("after %s: %s, want %s", s.action, got, s.want)
		}
	}
	if r := f.releases(admin)[0]; r.RevokeReason != nil {
		t.Fatalf("restore kept the revoke reason %q", *r.RevokeReason)
	}
}
