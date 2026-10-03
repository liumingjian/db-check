package oracle

import (
	"context"
	"fmt"
	"sort"
)

func (c *metricsCollector) collectAlertLog(ctx context.Context, version map[string]any) map[string]any {
	payload := map[string]any{"window_days": 7, "record_limit": 200, "critical_count": 0, "warning_count": 0, "errors": rowsPayload([]map[string]any{})}
	if major, _ := version["major"].(int); major < 12 {
		c.markUnavailable("oracle.alert_log", "此版本不采集诊断告警日志或版本探测失败", "请客户 DBA 使用 ADRCI 的 SHOW ALERT 或数据库 alert 日志，手工检查最近七天 ORA 错误。确认版本后可重新采集。")
		return payload
	}
	rows := c.queryRows(ctx, "oracle.alert_log", alertLogQuery)
	errors, critical, warning := summarizeAlertErrors(rows)
	payload["errors"] = rowsPayload(errors)
	payload["matching_records"] = len(rows)
	payload["limit_reached"] = len(rows) == 200
	payload["critical_count"], payload["warning_count"] = critical, warning
	return payload
}

// Counts refer to matching records, with each code counted once per record.
func summarizeAlertErrors(rows []map[string]any) ([]map[string]any, int, int) {
	byCode := map[string]map[string]any{}
	critical, warning := 0, 0
	for _, row := range rows {
		seen := map[string]bool{}
		for _, code := range oracleCode.FindAllString(fmt.Sprint(row["message_text"]), -1) {
			if seen[code] {
				continue
			}
			seen[code] = true
			severity := "warning"
			switch code {
			case "ORA-00600", "ORA-07445", "ORA-01555", "ORA-04031":
				severity = "critical"
			}
			if severity == "critical" {
				critical++
			} else {
				warning++
			}
			if item, ok := byCode[code]; ok {
				item["count"] = item["count"].(int) + 1
			} else {
				byCode[code] = map[string]any{"error_code": code, "severity": severity, "count": 1, "latest_time": row["originating_timestamp"], "latest_message": row["message_text"]}
			}
		}
	}
	codes := []string{}
	for code := range byCode {
		codes = append(codes, code)
	}
	sort.Strings(codes)
	result := []map[string]any{}
	for _, code := range codes {
		result = append(result, byCode[code])
	}
	return result, critical, warning
}

const alertLogQuery = `
SELECT * FROM (
 SELECT originating_timestamp AS "originating_timestamp", record_id AS "record_id", message_text AS "message_text"
 FROM v$diag_alert_ext
 WHERE originating_timestamp >= SYSTIMESTAMP - INTERVAL '7' DAY
   AND REGEXP_LIKE(message_text, 'ORA-[0-9]{5}')
 ORDER BY originating_timestamp DESC, record_id DESC
) WHERE ROWNUM <= 200`
