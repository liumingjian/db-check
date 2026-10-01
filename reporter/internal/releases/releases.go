// Package releases owns collector releases (CONTEXT.md: Collector release,
// Release package, Release status; ADR 0002): the release status table, who
// sees which release, and publishing a release with its four package files.
// Functions take a store.Querier so callers can combine them in one
// transaction.
//
// Package files live under the data directory, at
// <data dir>/releases/<version>/db-collector-<version>-<platform>.zip
// (PackagePath), and never expire.
package releases

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	"dbcheck/reporter/internal/apierr"
	"dbcheck/reporter/internal/store"
)

type Status string

const (
	StatusPreRelease Status = "pre-release"
	StatusLatest     Status = "latest"
	StatusDeprecated Status = "deprecated"
	StatusRevoked    Status = "revoked"
)

// Platform is one of the four equal release package platforms.
type Platform string

// Platforms are every release's platforms, in display order.
var Platforms = []Platform{"linux-amd64", "linux-arm64", "windows-amd64", "windows-arm64"}

// Package is a release package as the console contract's ReleasePackage
// carries it.
type Package struct {
	Platform Platform `json:"platform"`
	OS       string   `json:"os"`
	Arch     string   `json:"arch"`
	FileName string   `json:"fileName"`
	Size     int64    `json:"size"`
	SHA256   string   `json:"sha256"`
}

// Release is a collector release as the console contract's
// CollectorRelease carries it.
type Release struct {
	Version      string    `json:"version"`
	Tag          string    `json:"tag"`
	Commit       string    `json:"commit"`
	PublishedAt  string    `json:"publishedAt"`
	Status       Status    `json:"status"`
	RevokeReason string    `json:"revokeReason,omitempty"`
	Notes        string    `json:"notes"`
	DBTypes      []string  `json:"dbTypes"`
	Packages     []Package `json:"packages"`
}

// FileName is a release package's file name.
func FileName(version string, p Platform) string {
	return fmt.Sprintf("db-collector-%s-%s.zip", version, p)
}

// Dir is the releases area of the data directory.
func Dir(dataDir string) string {
	return filepath.Join(dataDir, "releases")
}

// PackagePath is where a published package file is stored.
func PackagePath(dataDir, version string, p Platform) string {
	return filepath.Join(Dir(dataDir), version, FileName(version, p))
}

func newPackage(version string, p Platform, size int64, sha string) Package {
	osName, arch, _ := strings.Cut(string(p), "-")
	labels := map[string]string{"linux": "Linux", "windows": "Windows", "amd64": "x86_64", "arm64": "ARM64"}
	return Package{Platform: p, OS: labels[osName], Arch: labels[arch], FileName: FileName(version, p), Size: size, SHA256: sha}
}

// Visible is the release visibility rule: engineers see latest and
// deprecated releases; admins see every status.
func Visible(s Status, admin bool) bool {
	return admin || s == StatusLatest || s == StatusDeprecated
}

const releaseColumns = "version, tag, commit_sha, published_at, status, COALESCE(revoke_reason, ''), notes, db_types"

// List returns the releases visible to an admin or an engineer, newest
// first, each with its packages in platform order.
func List(ctx context.Context, q store.Querier, admin bool) ([]Release, error) {
	query := "SELECT " + releaseColumns + " FROM releases"
	if !admin {
		query += " WHERE status IN ('latest', 'deprecated')"
	}
	rows, err := q.QueryContext(ctx, query+" ORDER BY published_at DESC, version DESC")
	if err != nil {
		return nil, err
	}
	var list []Release
	for rows.Next() {
		r, err := scanRelease(rows)
		if err != nil {
			rows.Close()
			return nil, err
		}
		list = append(list, r)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for i := range list {
		if list[i].Packages, err = packages(ctx, q, list[i].Version); err != nil {
			return nil, err
		}
	}
	return list, nil
}

// Get loads one release with its packages, whatever its status; NotFound
// when there is none.
func Get(ctx context.Context, q store.Querier, version string) (Release, error) {
	r, found, err := find(ctx, q, version)
	if err == nil && !found {
		err = notFound(version)
	}
	return r, err
}

func find(ctx context.Context, q store.Querier, version string) (r Release, found bool, err error) {
	r, err = scanRelease(q.QueryRowContext(ctx, "SELECT "+releaseColumns+" FROM releases WHERE version = ?", version))
	if errors.Is(err, sql.ErrNoRows) {
		return Release{}, false, nil
	}
	if err != nil {
		return Release{}, false, err
	}
	r.Packages, err = packages(ctx, q, version)
	return r, err == nil, err
}

func notFound(version string) error {
	return apierr.NotFound(fmt.Sprintf("版本 %s 不存在", version))
}

func scanRelease(row interface{ Scan(...any) error }) (Release, error) {
	var r Release
	var dbTypes string
	if err := row.Scan(&r.Version, &r.Tag, &r.Commit, &r.PublishedAt, &r.Status, &r.RevokeReason, &r.Notes, &dbTypes); err != nil {
		return Release{}, err
	}
	if err := json.Unmarshal([]byte(dbTypes), &r.DBTypes); err != nil {
		return Release{}, fmt.Errorf("release %s db_types: %w", r.Version, err)
	}
	return r, nil
}

func packages(ctx context.Context, q store.Querier, version string) ([]Package, error) {
	rows, err := q.QueryContext(ctx, "SELECT platform, size, sha256 FROM release_packages WHERE version = ?", version)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	byPlatform := map[Platform]Package{}
	for rows.Next() {
		var p Platform
		var size int64
		var sha string
		if err := rows.Scan(&p, &size, &sha); err != nil {
			return nil, err
		}
		byPlatform[p] = newPackage(version, p, size, sha)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	out := make([]Package, 0, len(byPlatform))
	for _, p := range Platforms {
		if pkg, ok := byPlatform[p]; ok {
			out = append(out, pkg)
		}
	}
	return out, nil
}

// Insert stores a release and its packages as given, demoting the current
// latest release when r is latest. It checks nothing else: Publish
// validates, and the contract test server seeds the fixture through it.
func Insert(ctx context.Context, q store.Querier, r Release) error {
	if r.Status == StatusLatest {
		if err := demoteLatest(ctx, q); err != nil {
			return err
		}
	}
	dbTypes, err := json.Marshal(r.DBTypes)
	if err != nil {
		return err
	}
	_, err = q.ExecContext(ctx, `INSERT INTO releases
		(version, tag, commit_sha, published_at, status, revoke_reason, notes, db_types)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		r.Version, r.Tag, r.Commit, r.PublishedAt, r.Status, nullable(r.RevokeReason), r.Notes, string(dbTypes))
	if err != nil {
		return fmt.Errorf("insert release %s: %w", r.Version, err)
	}
	for _, p := range r.Packages {
		_, err := q.ExecContext(ctx, `INSERT INTO release_packages (version, platform, file_name, size, sha256)
			VALUES (?, ?, ?, ?, ?)`, r.Version, p.Platform, FileName(r.Version, p.Platform), p.Size, p.SHA256)
		if err != nil {
			return fmt.Errorf("insert release package %s %s: %w", r.Version, p.Platform, err)
		}
	}
	return nil
}

func demoteLatest(ctx context.Context, q store.Querier) error {
	_, err := q.ExecContext(ctx, "UPDATE releases SET status = 'deprecated' WHERE status = 'latest'")
	return err
}

func nullable(s string) any {
	if s == "" {
		return nil
	}
	return s
}
