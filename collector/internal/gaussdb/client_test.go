package gaussdb

import (
	"dbcheck/collector/internal/cli"
	"strings"
	"testing"
)

func TestBuildDSNUsesOpenGaussDriverFormat(t *testing.T) {
	dsn := buildDSN(cli.Config{
		DBHost:            "10.0.0.9",
		DBPort:            8000,
		DBUsername:        "admin",
		DBPassword:        "secret",
		DBName:            "postgres",
		SQLTimeoutSeconds: 90,
	})
	for _, token := range []string{
		"host=10.0.0.9",
		"port=8000",
		"user=admin",
		"password=secret",
		"dbname=postgres",
		"connect_timeout=90",
		"sslmode=disable",
	} {
		if !strings.Contains(dsn, token) {
			t.Fatalf("expected dsn to contain %s, got %s", token, dsn)
		}
	}
}
