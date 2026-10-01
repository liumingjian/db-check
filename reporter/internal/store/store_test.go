package store

import (
	"context"
	"path/filepath"
	"testing"
	"testing/fstest"
)

func TestOpenAppliesEmbeddedMigrationsToEmptyAndMigratedDatabases(t *testing.T) {
	dir := t.TempDir()
	for range 2 {
		db, err := Open(dir)
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		if err := db.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}
}

func TestMigrateAppliesEachMigrationOnceInOrder(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "test.db")
	first := fstest.MapFS{
		"0001_things.sql": {Data: []byte("CREATE TABLE things (name TEXT NOT NULL);")},
		"0002_seed.sql":   {Data: []byte("INSERT INTO things (name) VALUES ('a');")},
	}

	db := openAt(t, path, first)
	db.Close()

	// Reopening an already-migrated database must not rerun 0001 (it would
	// fail) or 0002 (it would duplicate the row); a new 0003 applies alone.
	second := fstest.MapFS{
		"0001_things.sql": first["0001_things.sql"],
		"0002_seed.sql":   first["0002_seed.sql"],
		"0003_more.sql":   {Data: []byte("INSERT INTO things (name) VALUES ('b');")},
	}
	db = openAt(t, path, second)
	defer db.Close()

	var names string
	if err := db.QueryRowContext(ctx, "SELECT group_concat(name, ',') FROM (SELECT name FROM things ORDER BY name)").Scan(&names); err != nil {
		t.Fatalf("query: %v", err)
	}
	if names != "a,b" {
		t.Fatalf("rows after migrations = %q, want %q", names, "a,b")
	}
}

func TestMigrateRollsBackAFailingMigration(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")
	broken := fstest.MapFS{
		"0001_half.sql": {Data: []byte("CREATE TABLE half (x TEXT); INSERT INTO nowhere VALUES (1);")},
	}
	if db, err := open(path, broken); err == nil {
		db.Close()
		t.Fatal("expected the broken migration to fail")
	}

	// Had the failed migration left its table behind, this would fail.
	fixed := fstest.MapFS{
		"0001_half.sql": {Data: []byte("CREATE TABLE half (x TEXT);")},
	}
	db := openAt(t, path, fixed)
	db.Close()
}

func TestTxRollsBackOnError(t *testing.T) {
	ctx := context.Background()
	db := openAt(t, filepath.Join(t.TempDir(), "test.db"), fstest.MapFS{
		"0001_things.sql": {Data: []byte("CREATE TABLE things (name TEXT NOT NULL);")},
	})
	defer db.Close()

	err := db.Tx(ctx, func(tx Querier) error {
		if _, err := tx.ExecContext(ctx, "INSERT INTO things (name) VALUES ('a')"); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, "INSERT INTO things (name) VALUES (NULL)")
		return err
	})
	if err == nil {
		t.Fatal("expected the transaction to fail")
	}
	var count int
	if err := db.QueryRowContext(ctx, "SELECT count(*) FROM things").Scan(&count); err != nil {
		t.Fatalf("query: %v", err)
	}
	if count != 0 {
		t.Fatalf("rows after rollback = %d, want 0", count)
	}
}

func openAt(t *testing.T, path string, migrations fstest.MapFS) *DB {
	t.Helper()
	db, err := open(path, migrations)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	return db
}
