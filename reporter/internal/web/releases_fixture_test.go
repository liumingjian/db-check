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
	"testing"
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
