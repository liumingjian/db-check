package web

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"dbcheck/reporter/internal/users"
)

// refusedPublish is a publish request the server must refuse, leaving no
// release and no package file behind. edit changes a valid 1.0.0 release.
type refusedPublish struct {
	name  string
	edit  func(t *testing.T, r testRelease) testRelease
	token string // the publish credential sent; empty means the right one
	want  apiError
}

// noCredential as a refusedPublish token sends no Authorization header.
const noCredential = "-"

func (c refusedPublish) check(t *testing.T) {
	f := newPlatformFixture(t)
	f.enablePublish()
	release := newTestRelease(t, "1.0.0")
	if c.edit != nil {
		release = c.edit(t, release)
	}
	token := c.token
	switch token {
	case "":
		token = testPublishToken
	case noCredential:
		token = ""
	}
	expectAPIError(t, f.serve(publishRequest(t, release, token)), c.want)

	f.addUser("admin", users.RoleAdmin, users.StatusActive)
	if list := f.releases(f.token("admin")); len(list) != 0 {
		t.Fatalf("a refused publish left releases behind: %+v", list)
	}
	assertNoPackageFiles(t, f.api.cfg.DataDir)
}

func checkRefusedPublishes(t *testing.T, cases []refusedPublish) {
	for _, c := range cases {
		t.Run(c.name, c.check)
	}
}

// editPackage returns an edit that changes package i of the release.
func editPackage(i int, change func(p *testPackage)) func(*testing.T, testRelease) testRelease {
	return func(_ *testing.T, r testRelease) testRelease {
		p := r.Packages[i]
		change(&p)
		return r.withPackage(p)
	}
}

func invalid(message string) apiError { return apiError{http.StatusBadRequest, "invalid", message} }

func TestPublishRefusesPackagesThatDoNotMatchTheMetadata(t *testing.T) {
	checkRefusedPublishes(t, []refusedPublish{
		{name: "checksum mismatch", want: invalid("SHA256"),
			edit: editPackage(1, func(p *testPackage) { p.SHA256 = hex.EncodeToString(make([]byte, 32)) })},
		{name: "size mismatch", want: invalid("大小"),
			edit: editPackage(2, func(p *testPackage) { p.Size++ })},
		{name: "missing package file", want: invalid("windows-arm64"),
			edit: editPackage(3, func(p *testPackage) { p.content = nil })},
		{name: "package file without the .zip name", want: invalid(".zip"),
			edit: editPackage(0, func(p *testPackage) { p.name = "db-collector-1.0.0-linux-amd64.tar.gz" })},
		{name: "package that is not a zip", want: invalid("zip"),
			edit: func(t *testing.T, r testRelease) testRelease {
				return r.withPackage(packageFor(t, "1.0.0", "linux-arm64", []byte("this is not a zip archive")))
			}},
	})
}

func TestPublishRefusesAMissingOrExtraPlatform(t *testing.T) {
	checkRefusedPublishes(t, []refusedPublish{
		{name: "missing platform in metadata and files", want: invalid("windows-arm64"),
			edit: func(_ *testing.T, r testRelease) testRelease {
				r.Packages = r.Packages[:3]
				return r
			}},
		{name: "extra platform", want: invalid("darwin-arm64"),
			edit: func(t *testing.T, r testRelease) testRelease {
				r.Packages = append(r.Packages, packageFor(t, "1.0.0", "darwin-arm64", zipBytes(t, "mac")))
				return r
			}},
	})
}

func TestPublishRefusesBadReleaseMetadata(t *testing.T) {
	checkRefusedPublishes(t, []refusedPublish{
		{name: "tag that does not match the version", want: invalid("标签"),
			edit: func(_ *testing.T, r testRelease) testRelease {
				r.Tag = "v1.0.1"
				return r
			}},
		{name: "version that is neither vX.Y.Z nor vX.Y.Z-rcN", want: invalid("版本号"),
			edit: func(t *testing.T, _ testRelease) testRelease { return newTestRelease(t, "1.0.0-beta") }},
		{name: "unsupported database type", want: invalid("db2"),
			edit: func(_ *testing.T, r testRelease) testRelease {
				r.DBTypes = []string{"mysql", "db2"}
				return r
			}},
	})
}

func TestPublishRefusesAMissingOrWrongCredential(t *testing.T) {
	refused := apiError{http.StatusUnauthorized, "unauthorized", "发布凭证"}
	checkRefusedPublishes(t, []refusedPublish{
		{name: "missing credential", token: noCredential, want: refused},
		{name: "wrong credential", token: "not-the-secret", want: refused},
	})
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
	expectAPIError(t, rec, apiError{http.StatusUnauthorized, "unauthorized", "发布凭证"})
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
		expectAPIError(t, rec, apiError{http.StatusForbidden, "forbidden", "未启用"})
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
	expectAPIError(t, f.publish(changed), apiError{http.StatusConflict, "invalid", "1.2.0"})

	otherCommit := r
	otherCommit.Commit = "fff0000"
	expectAPIError(t, f.publish(otherCommit), apiError{http.StatusConflict, "invalid", "1.2.0"})

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
