package main

import (
	"bytes"
	"context"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"dbcheck/reporter/internal/store"
	"dbcheck/reporter/internal/users"
)

func noEnv(string) string { return "" }

func TestAdminCreateMakesTheFirstAdminWithATemporaryPassword(t *testing.T) {
	dataDir := t.TempDir()
	var stdout, stderr bytes.Buffer
	code := run([]string{"admin", "create", "--username", "root", "--data-dir", dataDir}, noEnv, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("exit %d, stderr %q", code, stderr.String())
	}
	match := regexp.MustCompile(`临时密码[:：]\s*(\S+)`).FindStringSubmatch(stdout.String())
	if match == nil {
		t.Fatalf("stdout %q does not print the temporary password", stdout.String())
	}

	db, err := store.Open(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	u, err := users.Authenticate(context.Background(), db, "root", match[1])
	if err != nil {
		t.Fatalf("signing in with the printed password: %v", err)
	}
	if u.Role != users.RoleAdmin || u.Status != users.StatusActive || !u.MustChangePassword {
		t.Fatalf("created user = %+v, want an active admin who must change the password", u)
	}
}

// The first deploy runs admin create before db-web has ever started, so the
// data directory may not exist yet.
func TestAdminCreateCreatesAMissingDataDirectory(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "var", "lib", "dbcheck")
	var stdout, stderr bytes.Buffer
	code := run([]string{"admin", "create", "--username", "root", "--data-dir", dataDir}, noEnv, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("exit %d, stderr %q", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "临时密码") {
		t.Fatalf("stdout %q does not print the temporary password", stdout.String())
	}
}

func TestAdminCreateRefusesOnceAnAdminExists(t *testing.T) {
	dataDir := t.TempDir()
	env := func(name string) string {
		if name == "DBCHECK_DATA_DIR" {
			return dataDir
		}
		return ""
	}
	var out bytes.Buffer
	if code := run([]string{"admin", "create", "--username", "root"}, env, &out, &out); code != 0 {
		t.Fatalf("first admin: exit %d, %q", code, out.String())
	}

	var stdout, stderr bytes.Buffer
	code := run([]string{"admin", "create", "--username", "second"}, env, &stdout, &stderr)
	if code == 0 {
		t.Fatal("expected a second admin create to be refused")
	}
	if !strings.Contains(stderr.String(), "管理员已存在") || strings.Contains(stdout.String(), "临时密码") {
		t.Fatalf("stdout %q stderr %q", stdout.String(), stderr.String())
	}
}

func TestAdminCreateNeedsAUsername(t *testing.T) {
	var out bytes.Buffer
	code := run([]string{"admin", "create", "--data-dir", t.TempDir()}, noEnv, &out, &out)
	if code != 2 {
		t.Fatalf("exit %d, want 2 (%q)", code, out.String())
	}
}
