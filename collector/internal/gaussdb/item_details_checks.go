package gaussdb

import (
	"regexp"
	"strconv"
	"strings"
)

var (
	formulaPattern    = regexp.MustCompile(`max_locks_per_transaction\[(\d+)\]\s*\*\s*\(max_connections\[(\d+)\]\s*\+\s*max_prepared_transactions\[(\d+)\]\)\s*=\s*(\d+)`)
	sha256Pattern     = regexp.MustCompile(`sha256sum:\s*([a-fA-F0-9]+)`)
	pidPattern        = regexp.MustCompile(`\bpid[:=]?\s*(\d+)\b`)
	errorCountPattern = regexp.MustCompile(`Number of ERROR in log is\s+(\d+)`)
	dbSizePattern     = regexp.MustCompile(`TOP\s+\d+\s*-\s*DB:\s*([^,]+),\s*Size:\s*([^\s]+)`)
	tableSizePattern  = regexp.MustCompile(`Table:\s*([^,]+),\s*Size:\s*([^\s]+)`)
)

func parseGUCValueDetails(summary string) map[string]any {
	match := formulaPattern.FindStringSubmatch(summary)
	if len(match) != 5 {
		return map[string]any{}
	}
	return map[string]any{
		"max_locks_per_transaction": parseInt(match[1]),
		"max_connections":           parseInt(match[2]),
		"max_prepared_transactions": parseInt(match[3]),
		"computed_value":            parseInt(match[4]),
		"configuration_reasonable":  strings.Contains(summary, "reasonable"),
	}
}

func parseClusterStateDetails(summary string) map[string]any {
	lines := splitLines(summary)
	details := map[string]any{}
	nodes := make([]map[string]any, 0)
	for _, line := range lines {
		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			continue
		}
		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])
		if key == "Cluster status" {
			continue
		}
		switch key {
		case "cluster_state":
			details["cluster_state"] = value
		case "redistributing":
			details["redistributing"] = value
		case "balanced":
			details["balanced"] = value
		default:
			nodes = append(nodes, map[string]any{"node": key, "status": value})
		}
	}
	if len(nodes) > 0 {
		details["nodes"] = nodes
		details["node_count"] = len(nodes)
	}
	return details
}

func parseIntegrityDetails(parsed parsedOutput) map[string]any {
	text := strings.TrimSpace(parsed.Raw + "\n" + parsed.Summary)
	match := sha256Pattern.FindStringSubmatch(text)
	if len(match) != 2 {
		return map[string]any{}
	}
	return map[string]any{"sha256": match[1]}
}

func parseOMMonitorDetails(parsed parsedOutput) map[string]any {
	text := strings.TrimSpace(parsed.Raw + "\n" + parsed.Summary)
	match := pidPattern.FindStringSubmatch(text)
	if len(match) != 2 {
		return map[string]any{}
	}
	return map[string]any{"pid": match[1]}
}

func parseSysTableDetails(parsed parsedOutput) map[string]any {
	lines := splitLines(parsed.Raw)
	rows := make([]map[string]any, 0)
	for _, line := range lines {
		fields := strings.Fields(line)
		if len(fields) != 6 || fields[0] == "Instance" {
			continue
		}
		rows = append(rows, map[string]any{
			"instance":      fields[0],
			"table_name":    fields[1],
			"size_bytes":    parseInt(fields[2]),
			"row_count":     parseInt(fields[3]),
			"avg_width":     parseInt(fields[4]),
			"row_width_sum": parseInt(fields[5]),
		})
	}
	if len(rows) == 0 {
		return map[string]any{}
	}
	return map[string]any{"tables": rows, "table_count": len(rows)}
}

func parseErrorInLogDetails(parsed parsedOutput) map[string]any {
	text := strings.TrimSpace(parsed.Summary + "\n" + parsed.Raw)
	match := errorCountPattern.FindStringSubmatch(text)
	if len(match) != 2 {
		return map[string]any{}
	}
	lines := strings.Split(text, "<NEW_LINE_SEPARATOR>")
	samples := make([]string, 0, len(lines))
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || errorCountPattern.MatchString(trimmed) {
			continue
		}
		samples = append(samples, trimmed)
		if len(samples) == 10 {
			break
		}
	}
	return map[string]any{
		"error_count":  parseInt(match[1]),
		"sample_lines": samples,
	}
}

func parseKeyDBTableSizeDetails(parsed parsedOutput) map[string]any {
	text := strings.TrimSpace(parsed.Raw + "\n" + parsed.Summary)
	databases := make([]map[string]any, 0)
	for _, match := range dbSizePattern.FindAllStringSubmatch(text, -1) {
		if len(match) != 3 {
			continue
		}
		sizeValue, sizeUnit := splitSize(match[2])
		databases = append(databases, map[string]any{
			"database":   strings.TrimSpace(match[1]),
			"size_value": sizeValue,
			"size_unit":  sizeUnit,
		})
	}
	tables := make([]map[string]any, 0)
	for _, match := range tableSizePattern.FindAllStringSubmatch(text, -1) {
		if len(match) != 3 {
			continue
		}
		sizeValue, sizeUnit := splitSize(match[2])
		tables = append(tables, map[string]any{
			"table_name": strings.TrimSpace(match[1]),
			"size_value": sizeValue,
			"size_unit":  sizeUnit,
		})
	}
	if len(databases) == 0 && len(tables) == 0 {
		return map[string]any{}
	}
	return map[string]any{
		"databases":      databases,
		"database_count": len(databases),
		"tables":         tables,
		"table_count":    len(tables),
	}
}

func splitLines(content string) []string {
	return strings.Split(strings.ReplaceAll(content, "\r\n", "\n"), "\n")
}

func parseInt(raw string) int {
	value, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil {
		return 0
	}
	return value
}

func splitSize(raw string) (float64, string) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return 0, ""
	}
	index := 0
	for index < len(trimmed) && ((trimmed[index] >= '0' && trimmed[index] <= '9') || trimmed[index] == '.') {
		index++
	}
	value, err := strconv.ParseFloat(trimmed[:index], 64)
	if err != nil {
		return 0, ""
	}
	return value, strings.TrimSpace(trimmed[index:])
}
