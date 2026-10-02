// Package publish is the publish script's client of the publish API
// (ADR 0002, including its pre-CI note): from a release tag on HEAD it
// builds the four release packages and posts them, with their metadata, to
// POST /api/ci/releases. Until the CI phase a person runs it through
// scripts/publish_release.sh; CI will call the same script.
package publish

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"dbcheck/reporter/internal/releases"
)

// Options configure one publish run.
type Options struct {
	// RepoDir is the root of the git checkout to publish from.
	RepoDir string
	// DistDir receives the release packages.
	DistDir string
	// BaseURL is the platform's address, e.g. http://127.0.0.1:8080.
	BaseURL string
	// Token is DBCHECK_PUBLISH_TOKEN.
	Token string
	// Build writes the four release packages for version into distDir,
	// named releases.FileName. Nil runs scripts/build_release_packages.sh.
	Build func(ctx context.Context, version, distDir string) error
}

// Result is the published release, and whether this run created it (false
// for an identical re-publish).
type Result struct {
	Release releases.Release
	Created bool
}

// dbTypes are the database types a release supports. The script maintains
// this list until the collector can report them (the CI phase).
var dbTypes = []string{"mysql", "oracle", "gaussdb"}

// Run checks the release on HEAD, builds its packages, and publishes them.
// Every refusal comes before the build.
func Run(ctx context.Context, o Options) (Result, error) {
	if o.Token == "" {
		return Result{}, errors.New("DBCHECK_PUBLISH_TOKEN is not set")
	}
	rel, err := readRelease(ctx, o.RepoDir)
	if err != nil {
		return Result{}, err
	}
	build := o.Build
	if build == nil {
		build = packagingScript(o.RepoDir)
	}
	if err := build(ctx, rel.version, o.DistDir); err != nil {
		return Result{}, fmt.Errorf("build the release packages: %w", err)
	}
	meta := releases.Metadata{
		Version: rel.version, Tag: rel.tag, Commit: rel.commit, Notes: rel.notes, DBTypes: dbTypes,
		PublishedAt: time.Now().UTC().Format(time.RFC3339),
	}
	for _, p := range releases.Platforms {
		pkg, err := describePackage(filepath.Join(o.DistDir, releases.FileName(rel.version, p)), p)
		if err != nil {
			return Result{}, err
		}
		meta.Packages = append(meta.Packages, pkg)
	}
	return post(ctx, o, meta)
}

// packagingScript builds the packages with scripts/build_release_packages.sh,
// its progress going to stderr.
func packagingScript(repo string) func(ctx context.Context, version, distDir string) error {
	return func(ctx context.Context, version, distDir string) error {
		cmd := exec.CommandContext(ctx, filepath.Join(repo, "scripts", "build_release_packages.sh"))
		cmd.Dir = repo
		cmd.Env = append(os.Environ(), "VERSION="+version, "DIST_DIR="+distDir)
		cmd.Stdout, cmd.Stderr = os.Stderr, os.Stderr
		return cmd.Run()
	}
}

func describePackage(path string, p releases.Platform) (releases.PackageMetadata, error) {
	f, err := os.Open(path)
	if err != nil {
		return releases.PackageMetadata{}, fmt.Errorf("release package for %s: %w", p, err)
	}
	defer f.Close()
	h := sha256.New()
	size, err := io.Copy(h, f)
	if err != nil {
		return releases.PackageMetadata{}, fmt.Errorf("hash %s: %w", path, err)
	}
	return releases.PackageMetadata{Platform: p, Size: size, SHA256: hex.EncodeToString(h.Sum(nil))}, nil
}

// post sends the publish request, streaming the package files.
func post(ctx context.Context, o Options, meta releases.Metadata) (Result, error) {
	body, w := io.Pipe()
	mw := multipart.NewWriter(w)
	go func() { w.CloseWithError(writeRequest(mw, o.DistDir, meta)) }()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(o.BaseURL, "/")+"/api/ci/releases", body)
	if err != nil {
		return Result{}, err
	}
	req.Header.Set("Authorization", "Bearer "+o.Token)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return Result{}, fmt.Errorf("publish to %s: %w", o.BaseURL, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		var apiErr struct{ Code, Message string }
		json.NewDecoder(resp.Body).Decode(&apiErr)
		return Result{}, fmt.Errorf("publish refused: HTTP %d %s: %s", resp.StatusCode, apiErr.Code, apiErr.Message)
	}
	res := Result{Created: resp.StatusCode == http.StatusCreated}
	if err := json.NewDecoder(resp.Body).Decode(&res.Release); err != nil {
		return Result{}, fmt.Errorf("read the published release: %w", err)
	}
	return res, nil
}

// writeRequest writes the multipart body: the metadata part, then one file
// part per package, named by platform.
func writeRequest(mw *multipart.Writer, distDir string, meta releases.Metadata) error {
	part, err := mw.CreateFormField("metadata")
	if err != nil {
		return err
	}
	if err := json.NewEncoder(part).Encode(meta); err != nil {
		return err
	}
	for _, p := range meta.Packages {
		name := releases.FileName(meta.Version, p.Platform)
		part, err := mw.CreateFormFile(string(p.Platform), name)
		if err != nil {
			return err
		}
		f, err := os.Open(filepath.Join(distDir, name))
		if err != nil {
			return err
		}
		_, err = io.Copy(part, f)
		f.Close()
		if err != nil {
			return err
		}
	}
	return mw.Close()
}
