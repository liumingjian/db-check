// Package store owns the platform's SQLite database (ADR 0003): opening it
// in the data directory, applying the embedded migrations at startup, and
// running transactions. Domain packages (users, sessions, ...) own their SQL
// and run it through a *DB or the Querier a transaction hands them.
package store

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite"
)

// FileName is the database file inside the data directory.
const FileName = "platform.db"

// migrations holds the ordered schema changes. To add one, drop a file named
// `<NNNN>_<what>.sql` into migrations/, with NNNN the GitHub issue number,
// so branches developed in parallel never pick the same name. See Open for
// the ordering rule. Never edit a migration once merged; add a new one.
//
//go:embed migrations/*.sql
var migrations embed.FS

// DB is the open platform database.
type DB struct {
	*sql.DB
}

// Querier is what domain code runs SQL through: a *DB, or the *sql.Tx that
// Tx passes in, so the same function works inside and outside a transaction.
type Querier interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// Open opens (creating it and dataDir if needed) the database in dataDir and
// applies every embedded migration not yet applied, in file-name order, each
// in its own transaction. Applied migrations are remembered by file name, so a
// migration merged later with a lower number still runs once.
func Open(dataDir string) (*DB, error) {
	sub, err := fs.Sub(migrations, "migrations")
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}
	return open(filepath.Join(dataDir, FileName), sub)
}

func open(path string, schema fs.FS) (*DB, error) {
	// Immediate transactions take the write lock up front, so two writers
	// wait on busy_timeout instead of failing to upgrade a read lock.
	dsn := "file:" + path + "?_pragma=foreign_keys(1)&_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_txlock=immediate"
	sqlDB, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open store: %w", err)
	}
	db := &DB{sqlDB}
	if err := db.migrate(context.Background(), schema); err != nil {
		sqlDB.Close()
		return nil, err
	}
	return db, nil
}

func (db *DB) migrate(ctx context.Context, schema fs.FS) error {
	if _, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		name TEXT PRIMARY KEY,
		applied_at TEXT NOT NULL
	)`); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}
	names, err := fs.Glob(schema, "*.sql") // sorted
	if err != nil {
		return fmt.Errorf("migrate: %w", err)
	}
	for _, name := range names {
		if err := db.applyOnce(ctx, schema, name); err != nil {
			return fmt.Errorf("migrate %s: %w", name, err)
		}
	}
	return nil
}

func (db *DB) applyOnce(ctx context.Context, schema fs.FS, name string) error {
	body, err := fs.ReadFile(schema, name)
	if err != nil {
		return err
	}
	return db.Tx(ctx, func(tx Querier) error {
		var applied int
		if err := tx.QueryRowContext(ctx, "SELECT count(*) FROM schema_migrations WHERE name = ?", name).Scan(&applied); err != nil {
			return err
		}
		if applied > 0 {
			return nil
		}
		if _, err := tx.ExecContext(ctx, string(body)); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, "INSERT INTO schema_migrations (name, applied_at) VALUES (?, ?)", name, FormatTime(time.Now()))
		return err
	})
}

// Tx runs fn in one transaction, committing when fn returns nil and rolling
// back otherwise.
func (db *DB) Tx(ctx context.Context, fn func(tx Querier) error) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()
}

const timeLayout = "2006-01-02T15:04:05.000Z"

// FormatTime is how every time is stored: fixed-width UTC ISO 8601 with
// milliseconds, so stored times compare correctly as text and read as the
// wire format the console expects.
func FormatTime(t time.Time) string {
	return t.UTC().Format(timeLayout)
}

// ParseTime reads a time written by FormatTime.
func ParseTime(s string) (time.Time, error) {
	return time.Parse(timeLayout, s)
}
