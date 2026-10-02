package publish

import (
	"context"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
)

// versionFile holds the collector's built-in version, relative to the
// repository root.
const versionFile = "collector/internal/cli/config.go"

// release is what the checkout says about the release on HEAD.
type release struct {
	version, tag, commit, notes string
}

// readRelease refuses unless HEAD is exactly on a release tag naming the
// collector's built-in version, with no uncommitted changes and with
// release notes, so every package traces to the tag and its commit.
func readRelease(ctx context.Context, repo string) (release, error) {
	version, err := builtinVersion(repo)
	if err != nil {
		return release{}, err
	}
	tag, err := releaseTag(ctx, repo, version)
	if err != nil {
		return release{}, err
	}
	commit, err := git(ctx, repo, "rev-parse", "HEAD")
	if err != nil {
		return release{}, err
	}
	changed, err := git(ctx, repo, "status", "--porcelain", "--untracked-files=no")
	if err != nil {
		return release{}, err
	}
	if changed != "" {
		return release{}, fmt.Errorf("the checkout has uncommitted changes; commit or discard them so the packages match %s:\n%s", tag, changed)
	}
	notes, err := releaseNotes(ctx, repo, tag)
	if err != nil {
		return release{}, err
	}
	return release{version: version, tag: tag, commit: commit, notes: notes}, nil
}

// builtinVersion reads the collector's built-in version, the Version
// constant that `db-collector --version` prints, from its source file. The
// source is read rather than run so the tag check happens before anything
// is built.
func builtinVersion(repo string) (string, error) {
	f, err := parser.ParseFile(token.NewFileSet(), filepath.Join(repo, versionFile), nil, 0)
	if err != nil {
		return "", fmt.Errorf("read the collector's built-in version: %w", err)
	}
	for _, decl := range f.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok || gen.Tok != token.CONST {
			continue
		}
		for _, spec := range gen.Specs {
			vs := spec.(*ast.ValueSpec)
			for i, name := range vs.Names {
				if name.Name != "Version" || i >= len(vs.Values) {
					continue
				}
				if lit, ok := vs.Values[i].(*ast.BasicLit); ok && lit.Kind == token.STRING {
					return strconv.Unquote(lit.Value)
				}
			}
		}
	}
	return "", fmt.Errorf("no string constant Version in %s", versionFile)
}

var releaseTagPattern = regexp.MustCompile(`^v\d+\.\d+\.\d+(-rc\d+)?$`)

// releaseTag returns the vX.Y.Z or vX.Y.Z-rcN tag on HEAD that names the
// built-in version. HEAD may carry several release tags (an rc and its
// final release on one commit); the built-in version picks one.
func releaseTag(ctx context.Context, repo, version string) (string, error) {
	out, err := git(ctx, repo, "tag", "--points-at", "HEAD")
	if err != nil {
		return "", err
	}
	var found []string
	for _, tag := range strings.Fields(out) {
		if !releaseTagPattern.MatchString(tag) {
			continue
		}
		if tag == "v"+version {
			return tag, nil
		}
		found = append(found, tag)
	}
	if len(found) == 0 {
		return "", fmt.Errorf("HEAD is not on a release tag (vX.Y.Z or vX.Y.Z-rcN); tag the release commit, e.g. git tag -a v%s", version)
	}
	return "", fmt.Errorf("release tag %s does not match the collector's built-in version %s (%s); fix one of them so the packages trace to the tag",
		strings.Join(found, ", "), version, versionFile)
}

// releaseNotes is the tag annotation, without any signature. A tag without
// one (a lightweight tag) falls back to the version's section of
// CHANGELOG.md, read only if the file exists.
func releaseNotes(ctx context.Context, repo, tag string) (string, error) {
	// For a lightweight tag %(contents) is the commit message, hence the
	// object type check.
	out, err := git(ctx, repo, "for-each-ref", "--format=%(objecttype)%00%(contents)%00%(contents:signature)", "refs/tags/"+tag)
	if err != nil {
		return "", err
	}
	parts := strings.SplitN(out, "\x00", 3)
	if len(parts) == 3 && parts[0] == "tag" {
		if notes := strings.TrimSpace(strings.TrimSuffix(parts[1], parts[2])); notes != "" {
			return notes, nil
		}
	}
	version := strings.TrimPrefix(tag, "v")
	changelog, err := os.ReadFile(filepath.Join(repo, "CHANGELOG.md"))
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return "", err
	}
	if notes := changelogSection(string(changelog), version); notes != "" {
		return notes, nil
	}
	return "", fmt.Errorf("no release notes for %s: annotate the tag (git tag -a %s -m '<notes>') or add a '## [%s]' section to CHANGELOG.md", tag, tag, version)
}

// changelogSection returns the body under the "## [version]" (or
// "## version", "## vversion") heading, up to the next "## " heading.
func changelogSection(changelog, version string) string {
	heading := regexp.MustCompile(`^##\s+\[?v?` + regexp.QuoteMeta(version) + `\]?(\s|$)`)
	var body []string
	in := false
	for _, line := range strings.Split(changelog, "\n") {
		if strings.HasPrefix(line, "## ") {
			if in {
				break
			}
			in = heading.MatchString(line)
			continue
		}
		if in {
			body = append(body, line)
		}
	}
	return strings.TrimSpace(strings.Join(body, "\n"))
}

func git(ctx context.Context, repo string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Dir = repo
	out, err := cmd.Output()
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return "", fmt.Errorf("git %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(string(exitErr.Stderr)))
	}
	return strings.TrimSpace(string(out)), err
}
