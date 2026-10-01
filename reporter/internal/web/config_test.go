package web

import (
	"strings"
	"testing"
)

func TestRetiredSharedTokenOnlyWarns(t *testing.T) {
	if got := RetiredSettingWarnings(envGetter(nil)); len(got) != 0 {
		t.Fatalf("warnings without the variable = %q", got)
	}
	got := RetiredSettingWarnings(envGetter(map[string]string{"DBCHECK_API_TOKEN": "ATI"}))
	if len(got) != 1 || !strings.Contains(got[0], "DBCHECK_API_TOKEN") || !strings.Contains(got[0], "忽略") {
		t.Fatalf("warnings = %q, want one saying DBCHECK_API_TOKEN is ignored", got)
	}
}

func envGetter(values map[string]string) func(string) string {
	return func(key string) string {
		return values[key]
	}
}
