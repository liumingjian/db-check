package publish

import (
	"archive/zip"
	"bytes"
	"context"
	"fmt"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"dbcheck/reporter/internal/releases"
	"dbcheck/reporter/internal/store"
	"dbcheck/reporter/internal/web"
)

// testRepo is a throwaway git repository holding just the collector's
// version file, so the tag check never touches the real repository's tags.
type testRepo struct {
	t   *testing.T
	dir string
}

func newTestRepo(t *testing.T, builtinVersion string) testRepo {
	t.Helper()
	r := testRepo{t: t, dir: t.TempDir()}
	r.writeVersion(builtinVersion)
	r.git("init", "-q")
	r.commit("collector " + builtinVersion)
	return r
}

func (r testRepo) writeVersion(v string) {
	r.t.Helper()
	path := filepath.Join(r.dir, versionFile)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		r.t.Fatal(err)
	}
	src := "package cli\n\nconst (\n\tDefaultTopN = 20\n\tVersion     = \"" + v + "\"\n)\n"
	if err := os.WriteFile(path, []byte(src), 0o644); err != nil {
		r.t.Fatal(err)
	}
}

func (r testRepo) commit(msg string) {
	r.t.Helper()
	r.git("add", "-A")
	r.git("commit", "-q", "-m", msg)
}

func (r testRepo) git(args ...string) string {
	r.t.Helper()
	base := []string{"-c", "user.name=test", "-c", "user.email=test@example.com", "-c", "tag.gpgSign=false", "-c", "commit.gpgSign=false"}
	cmd := exec.Command("git", append(base, args...)...)
	cmd.Dir = r.dir
	out, err := cmd.CombinedOutput()
	if err != nil {
		r.t.Fatalf("git %v: %v\n%s", args, err, out)
	}
	return strings.TrimSpace(string(out))
}

// buildSpy records whether packaging ran.
type buildSpy struct{ called bool }

func (b *buildSpy) build(context.Context, string, string) error {
	b.called = true
	return nil
}

func (r testRepo) options(b *buildSpy) Options {
	return Options{
		RepoDir: r.dir,
		DistDir: filepath.Join(r.t.TempDir(), "dist"),
		BaseURL: "http://127.0.0.1:1",
		Token:   "ci-secret",
		Build:   b.build,
	}
}

func requireRefusedBeforeBuild(t *testing.T, err error, b *buildSpy, wantInMessage string) {
	t.Helper()
	if err == nil {
		t.Fatal("Run succeeded, want a refusal")
	}
	if !strings.Contains(err.Error(), wantInMessage) {
		t.Fatalf("error %q does not mention %q", err, wantInMessage)
	}
	if b.called {
		t.Fatal("packages were built before the refusal")
	}
}

func TestRefusesUntaggedHeadBeforeBuilding(t *testing.T) {
	repo := newTestRepo(t, "1.2.0")
	b := &buildSpy{}

	_, err := Run(context.Background(), repo.options(b))

	requireRefusedBeforeBuild(t, err, b, "HEAD is not on a release tag")
}

const testToken = "ci-publish-secret"

// newPlatform starts the real db-web handler with the publish API enabled.
func newPlatform(t *testing.T) (baseURL string, db *store.DB) {
	t.Helper()
	dataDir := t.TempDir()
	db, err := store.Open(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	h, err := web.NewHandler(web.Config{DataDir: dataDir, PublishToken: testToken, MaxUploadBytes: 1 << 20}, web.Platform{DB: db})
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return srv.URL, db
}

// buildZips stands in for the packaging script: one small zip per platform.
func buildZips(_ context.Context, version, distDir string) error {
	if err := os.MkdirAll(distDir, 0o755); err != nil {
		return err
	}
	for _, p := range releases.Platforms {
		var buf bytes.Buffer
		zw := zip.NewWriter(&buf)
		w, err := zw.Create("db-collector/README.txt")
		if err != nil {
			return err
		}
		fmt.Fprintf(w, "db-collector %s %s", version, p)
		if err := zw.Close(); err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(distDir, releases.FileName(version, p)), buf.Bytes(), 0o644); err != nil {
			return err
		}
	}
	return nil
}

func TestPublishesTheTaggedReleaseAndRepublishesIdempotently(t *testing.T) {
	baseURL, db := newPlatform(t)
	repo := newTestRepo(t, "1.2.0")
	repo.git("tag", "-a", "v1.2.0", "-m", "- 新增 GaussDB 采集\n- 修复超时")
	commit := repo.git("rev-parse", "HEAD")
	o := Options{RepoDir: repo.dir, DistDir: filepath.Join(t.TempDir(), "dist"), BaseURL: baseURL, Token: testToken, Build: buildZips}

	first, err := Run(context.Background(), o)
	if err != nil {
		t.Fatalf("first run: %v", err)
	}
	second, err := Run(context.Background(), o)
	if err != nil {
		t.Fatalf("second run: %v", err)
	}

	if !first.Created || second.Created {
		t.Fatalf("created = %v then %v, want true then false", first.Created, second.Created)
	}
	stored, err := releases.Get(context.Background(), db, "1.2.0")
	if err != nil {
		t.Fatal(err)
	}
	if stored.Tag != "v1.2.0" || stored.Commit != commit || stored.Status != releases.StatusLatest {
		t.Fatalf("stored tag %s commit %s status %s, want v1.2.0 %s latest", stored.Tag, stored.Commit, stored.Status, commit)
	}
	if stored.Notes != "- 新增 GaussDB 采集\n- 修复超时" {
		t.Fatalf("notes = %q, want the tag annotation", stored.Notes)
	}
	if strings.Join(stored.DBTypes, ",") != "mysql,oracle,gaussdb" {
		t.Fatalf("dbTypes = %v", stored.DBTypes)
	}
	if len(stored.Packages) != 4 {
		t.Fatalf("stored %d packages, want 4", len(stored.Packages))
	}
}

func TestTakesReleaseNotesFromTheChangelogWhenTheTagHasNoAnnotation(t *testing.T) {
	baseURL, _ := newPlatform(t)
	repo := newTestRepo(t, "1.2.0")
	changelog := "# Changelog\n\n## [1.3.0] - 2026-11-01\n\n- 以后的版本\n\n## [1.2.0] - 2026-10-01\n\n- 新增 GaussDB 采集\n- 修复超时\n\n## [1.1.0]\n\n- 旧版本\n"
	if err := os.WriteFile(filepath.Join(repo.dir, "CHANGELOG.md"), []byte(changelog), 0o644); err != nil {
		t.Fatal(err)
	}
	repo.commit("changelog")
	repo.git("tag", "v1.2.0")
	o := Options{RepoDir: repo.dir, DistDir: filepath.Join(t.TempDir(), "dist"), BaseURL: baseURL, Token: testToken, Build: buildZips}

	res, err := Run(context.Background(), o)

	if err != nil {
		t.Fatal(err)
	}
	if res.Release.Notes != "- 新增 GaussDB 采集\n- 修复超时" {
		t.Fatalf("notes = %q, want the CHANGELOG.md section for 1.2.0", res.Release.Notes)
	}
}

func TestRefusesReleaseWithoutNotesBeforeBuilding(t *testing.T) {
	repo := newTestRepo(t, "1.2.0")
	repo.git("tag", "v1.2.0")
	b := &buildSpy{}

	_, err := Run(context.Background(), repo.options(b))

	requireRefusedBeforeBuild(t, err, b, "no release notes for v1.2.0")
}

func TestRefusesUncommittedChangesBeforeBuilding(t *testing.T) {
	repo := newTestRepo(t, "1.2.0")
	repo.git("tag", "-a", "v1.2.0", "-m", "notes")
	if err := os.WriteFile(filepath.Join(repo.dir, versionFile), []byte("package cli\n\nconst Version = \"1.2.0\" // edited\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	b := &buildSpy{}

	_, err := Run(context.Background(), repo.options(b))

	requireRefusedBeforeBuild(t, err, b, "uncommitted changes")
}

func TestRefusesMissingPublishTokenBeforeBuilding(t *testing.T) {
	repo := newTestRepo(t, "1.2.0")
	repo.git("tag", "-a", "v1.2.0", "-m", "notes")
	b := &buildSpy{}
	o := repo.options(b)
	o.Token = ""

	_, err := Run(context.Background(), o)

	requireRefusedBeforeBuild(t, err, b, "DBCHECK_PUBLISH_TOKEN")
}

func TestRefusesHeadWithoutAReleaseTagBeforeBuilding(t *testing.T) {
	cases := []struct {
		name string
		tag  func(r testRepo)
	}{
		{"old release-X.Y.Z naming", func(r testRepo) { r.git("tag", "-a", "release-1.2.0", "-m", "notes") }},
		{"tag missing its v", func(r testRepo) { r.git("tag", "-a", "1.2.0", "-m", "notes") }},
		{"release tag on an earlier commit", func(r testRepo) {
			r.git("tag", "-a", "v1.2.0", "-m", "notes")
			r.git("commit", "-q", "--allow-empty", "-m", "after the tag")
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			repo := newTestRepo(t, "1.2.0")
			c.tag(repo)
			b := &buildSpy{}

			_, err := Run(context.Background(), repo.options(b))

			requireRefusedBeforeBuild(t, err, b, "HEAD is not on a release tag")
		})
	}
}

func TestRefusesTagThatDisagreesWithTheBuiltinVersionBeforeBuilding(t *testing.T) {
	for _, tag := range []string{"v1.3.0", "v1.2.0-rc1"} {
		t.Run(tag, func(t *testing.T) {
			repo := newTestRepo(t, "1.2.0")
			repo.git("tag", "-a", tag, "-m", "notes")
			b := &buildSpy{}

			_, err := Run(context.Background(), repo.options(b))

			requireRefusedBeforeBuild(t, err, b, "does not match the collector's built-in version 1.2.0")
		})
	}
}
