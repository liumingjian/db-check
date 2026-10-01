// Package downloads owns release package downloads and their download
// records (CONTEXT.md: Download record): who may download which package,
// and the audit entry each download writes. Functions take a store.Querier
// so callers can combine them in one transaction.
package downloads

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"time"

	"dbcheck/reporter/internal/apierr"
	"dbcheck/reporter/internal/releases"
	"dbcheck/reporter/internal/store"
	"dbcheck/reporter/internal/users"
)

// Record is a download record as the console contract's DownloadRecord
// carries it.
type Record struct {
	ID       string            `json:"id"`
	UserID   string            `json:"userId"`
	Version  string            `json:"version"`
	Platform releases.Platform `json:"platform"`
	At       string            `json:"at"`
}

// Start begins u's download of one release package: it checks the release
// is visible to u (releases.Visible), writes the download record at now,
// and returns the open package file for the caller to stream and close.
// A package u may not see answers NotFound, like an unknown one, and
// writes no record.
func Start(ctx context.Context, q store.Querier, dataDir string, u users.User, version string, p releases.Platform, now time.Time) (*os.File, releases.Package, error) {
	pkg, err := visiblePackage(ctx, q, u, version, p)
	if err != nil {
		return nil, releases.Package{}, err
	}
	f, err := os.Open(releases.PackagePath(dataDir, version, p))
	if err != nil {
		return nil, releases.Package{}, fmt.Errorf("open release package %s %s: %w", version, p, err)
	}
	if err := Insert(ctx, q, Record{ID: newID(), UserID: u.ID, Version: version, Platform: p, At: store.FormatTime(now)}); err != nil {
		f.Close()
		return nil, releases.Package{}, err
	}
	return f, pkg, nil
}

func visiblePackage(ctx context.Context, q store.Querier, u users.User, version string, p releases.Platform) (releases.Package, error) {
	notFound := apierr.NotFound(fmt.Sprintf("没有 v%s 的 %s 采集器包", version, p))
	r, err := releases.Get(ctx, q, version)
	var apiErr *apierr.Error
	if errors.As(err, &apiErr) && apiErr.Code == apierr.CodeNotFound {
		return releases.Package{}, notFound
	}
	if err != nil {
		return releases.Package{}, err
	}
	if !releases.Visible(r.Status, u.Role == users.RoleAdmin) {
		return releases.Package{}, notFound
	}
	i := slices.IndexFunc(r.Packages, func(pkg releases.Package) bool { return pkg.Platform == p })
	if i < 0 {
		return releases.Package{}, notFound
	}
	return r.Packages[i], nil
}

// Insert stores a download record as given. Start writes them; the
// contract test server seeds the fixture's through it.
func Insert(ctx context.Context, q store.Querier, r Record) error {
	_, err := q.ExecContext(ctx, `INSERT INTO download_records (id, user_id, version, platform, at)
		VALUES (?, ?, ?, ?, ?)`, r.ID, r.UserID, r.Version, r.Platform, r.At)
	if err != nil {
		return fmt.Errorf("insert download record: %w", err)
	}
	return nil
}

// Filter narrows List; every set field must match. From is inclusive, To
// exclusive; a zero time leaves that end open.
type Filter struct {
	UserID  string
	Version string
	From    time.Time
	To      time.Time
}

// List returns the download records matching f, newest first.
func List(ctx context.Context, q store.Querier, f Filter) ([]Record, error) {
	var where []string
	var args []any
	add := func(cond string, arg any) {
		where = append(where, cond)
		args = append(args, arg)
	}
	if f.UserID != "" {
		add("user_id = ?", f.UserID)
	}
	if f.Version != "" {
		add("version = ?", f.Version)
	}
	if !f.From.IsZero() {
		add("at >= ?", store.FormatTime(f.From))
	}
	if !f.To.IsZero() {
		add("at < ?", store.FormatTime(f.To))
	}
	query := "SELECT id, user_id, version, platform, at FROM download_records"
	if len(where) > 0 {
		query += " WHERE " + strings.Join(where, " AND ")
	}
	rows, err := q.QueryContext(ctx, query+" ORDER BY at DESC, seq DESC", args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	list := []Record{}
	for rows.Next() {
		var r Record
		if err := rows.Scan(&r.ID, &r.UserID, &r.Version, &r.Platform, &r.At); err != nil {
			return nil, err
		}
		list = append(list, r)
	}
	return list, rows.Err()
}

func newID() string {
	b := make([]byte, 8)
	rand.Read(b) // crashes the program rather than return an error
	return "d-" + hex.EncodeToString(b)
}
