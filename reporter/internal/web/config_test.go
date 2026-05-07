package web

import "testing"

func TestParseConfigUsesDefaultAPIToken(t *testing.T) {
	cfg, err := ParseConfig(nil, envGetter(map[string]string{
		envDataDir:        t.TempDir(),
		envAllowedOrigins: "http://example.com",
	}))
	if err != nil {
		t.Fatalf("ParseConfig failed: %v", err)
	}
	if cfg.APIToken != defaultAPIToken {
		t.Fatalf("expected default token %q got %q", defaultAPIToken, cfg.APIToken)
	}
}

func TestParseConfigAllowsAPITokenOverride(t *testing.T) {
	cfg, err := ParseConfig(nil, envGetter(map[string]string{
		envDataDir:        t.TempDir(),
		envAllowedOrigins: "http://example.com",
		envAPIToken:       "custom-token",
	}))
	if err != nil {
		t.Fatalf("ParseConfig failed: %v", err)
	}
	if cfg.APIToken != "custom-token" {
		t.Fatalf("expected custom token got %q", cfg.APIToken)
	}
}

func envGetter(values map[string]string) func(string) string {
	return func(key string) string {
		return values[key]
	}
}
