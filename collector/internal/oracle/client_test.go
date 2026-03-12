package oracle

import (
	"dbcheck/collector/internal/cli"
	"strings"
	"testing"
)

func TestBuildDSNUsesSID(t *testing.T) {
	dsn := buildDSN(cli.Config{
		DBHost:     "10.0.0.1",
		DBPort:     1521,
		DBUsername: "system",
		DBPassword: "secret",
		DBName:     "ORCL",
	})
	if !strings.Contains(dsn, "oracle://system:secret@10.0.0.1:1521/") {
		t.Fatalf("unexpected dsn: %s", dsn)
	}
	if !strings.Contains(dsn, "SID=ORCL") {
		t.Fatalf("expected SID in dsn: %s", dsn)
	}
}
