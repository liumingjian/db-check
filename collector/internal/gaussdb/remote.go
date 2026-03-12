package gaussdb

import (
	"dbcheck/collector/internal/cli"
	"dbcheck/collector/internal/osinfo"
	"fmt"
	"os"
	"strings"
)

const (
	errorInLogWarningThreshold  = 1
	errorInLogCriticalThreshold = 10
	errorInLogContextLines      = 5
	errorInLogMaxRecords        = 2000
)

type remoteRunner interface {
	Run(command string) (string, error)
	Close() error
}

func defaultRunnerFactory(cfg cli.Config) (remoteRunner, error) {
	return osinfo.NewSSHCommandRunner(cfg)
}

func buildItemCommand(cfg cli.Config, item string) string {
	if item == "CheckErrorInLog" {
		return buildRemoteShellCommand(cfg.GaussUser, buildActiveLogCheckScript(cfg))
	}
	script := fmt.Sprintf(
		". \"%s\" && gs_check -i \"%s\" -L 2>&1",
		envFileExpr(cfg.GaussEnvFile),
		escapeDoubleQuoted(item),
	)
	return buildRemoteShellCommand(cfg.GaussUser, script)
}

func buildActiveLogCheckScript(cfg cli.Config) string {
	lines := []string{
		fmt.Sprintf(". \"%s\"", envFileExpr(cfg.GaussEnvFile)),
		`LOG_DIR="$GAUSSLOG"`,
		`ACTIVE_FILES=$(lsof 2>/dev/null | awk -v base="$LOG_DIR" 'index($9, base)==1 && $4 ~ /[0-9]+[rwu]/ {print $9}' | sort -u | grep -E '\.log$|current\.log$' | grep -Ev '/(asp_data|gs_profile|pg_audit|pg_perf|sql_monitor)/' || true)`,
		`MATCH_LINES=""`,
		fmt.Sprintf(`if [ -n "$ACTIVE_FILES" ]; then MATCH_LINES=$(printf '%%s\n' "$ACTIVE_FILES" | xargs grep -H -n -C %d ERROR 2>/dev/null | tail -%d || true); fi`, errorInLogContextLines, errorInLogMaxRecords),
		`ERROR_COUNT=$(printf '%s\n' "$MATCH_LINES" | awk 'NF {count++} END {print count+0}')`,
		`RESULT_STATUS=OK`,
		fmt.Sprintf(`if [ "$ERROR_COUNT" -ge %d ]; then RESULT_STATUS=NG; elif [ "$ERROR_COUNT" -gt %d ]; then RESULT_STATUS=WARNING; fi`, errorInLogCriticalThreshold, errorInLogWarningThreshold),
		`NOW=$(date '+%Y-%m-%d %H:%M:%S')`,
		`printf '%s [NAM] CheckErrorInLog\n' "$NOW"`,
		`printf '%s [STD]\n' "$NOW"`,
		`printf '%s [RST] %s\n' "$NOW" "$RESULT_STATUS"`,
		`printf 'Number of ERROR in log is %s\n' "$ERROR_COUNT"`,
		`printf 'ERROR in log:\n'`,
		`if [ -n "$MATCH_LINES" ]; then printf '%s\n' "$MATCH_LINES" | awk 'NF {printf("%s<NEW_LINE_SEPARATOR>", $0)}'; fi`,
		`printf '\n%s [RAW]\n' "$NOW"`,
		`printf 'active_log_scan(lsof+grep): %s\n' "$ACTIVE_FILES"`,
	}
	return strings.Join(lines, " && ")
}

func buildMetadataCommand(cfg cli.Config) string {
	script := strings.Join(
		[]string{
			fmt.Sprintf(". \"%s\"", envFileExpr(cfg.GaussEnvFile)),
			"echo '__DBCHECK_META_BEGIN__'",
			"gaussdb --version 2>/dev/null || true",
			"echo '__DBCHECK_SPLIT__'",
			"gsql --version 2>/dev/null || true",
			"echo '__DBCHECK_SPLIT__'",
			"gs_check -V 2>/dev/null || true",
			"echo '__DBCHECK_SPLIT__'",
			"env | egrep '^(GAUSS_VERSION|GAUSSHOME|GAUSSLOG|GS_CLUSTER_NAME|PGUSER|PGHOST)=' | sort",
		},
		" && ",
	)
	return buildRemoteShellCommand(cfg.GaussUser, script)
}

func buildRemoteShellCommand(user string, script string) string {
	return fmt.Sprintf("su - %s -c %s", shellQuote(user), shellQuote("bash -lc "+shellQuote(script)))
}

func envFileExpr(path string) string {
	if strings.HasPrefix(path, "~/") {
		return "$HOME/" + escapeDoubleQuoted(strings.TrimPrefix(path, "~/"))
	}
	if trimmed, ok := trimCurrentUserHome(path); ok {
		return "$HOME/" + escapeDoubleQuoted(trimmed)
	}
	return escapeDoubleQuoted(path)
}

func trimCurrentUserHome(path string) (string, bool) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", false
	}
	prefix := home + "/"
	if !strings.HasPrefix(path, prefix) {
		return "", false
	}
	return strings.TrimPrefix(path, prefix), true
}

func escapeDoubleQuoted(value string) string {
	replacer := strings.NewReplacer(`\`, `\\`, `"`, `\"`, "`", "\\`")
	return replacer.Replace(value)
}

func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\"'\"'") + "'"
}
