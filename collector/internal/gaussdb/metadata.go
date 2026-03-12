package gaussdb

import "strings"

const metadataSplitToken = "__DBCHECK_SPLIT__"

func parseMetadata(output string, cfg gaussConfig) map[string]any {
	parts := strings.Split(output, metadataSplitToken)
	meta := map[string]any{
		"gauss_user":     cfg.GaussUser,
		"gauss_env_file": cfg.GaussEnvFile,
	}
	if len(parts) > 0 {
		meta["gaussdb_version"] = compactLines(parts[0])
	}
	if len(parts) > 1 {
		meta["gsql_version"] = compactLines(parts[1])
	}
	if len(parts) > 2 {
		meta["gs_check_version"] = compactLines(parts[2])
	}
	if len(parts) > 3 {
		applyEnvMetadata(meta, parts[3])
	}
	if version := extractKernelVersion(asString(meta["gaussdb_version"])); version != "" {
		meta["version"] = version
	}
	return meta
}

func compactLines(content string) string {
	lines := strings.Split(strings.ReplaceAll(content, "\r\n", "\n"), "\n")
	filtered := make([]string, 0, len(lines))
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || trimmed == "__DBCHECK_META_BEGIN__" {
			continue
		}
		filtered = append(filtered, trimmed)
	}
	return strings.Join(filtered, " ")
}

func applyEnvMetadata(meta map[string]any, content string) {
	lines := strings.Split(strings.ReplaceAll(content, "\r\n", "\n"), "\n")
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		parts := strings.SplitN(trimmed, "=", 2)
		if len(parts) != 2 {
			continue
		}
		meta[strings.ToLower(parts[0])] = parts[1]
	}
}

func extractKernelVersion(content string) string {
	fields := strings.Fields(content)
	for index, field := range fields {
		if field == "Kernel" && index+1 < len(fields) {
			return strings.TrimSpace(fields[index+1])
		}
	}
	return ""
}
