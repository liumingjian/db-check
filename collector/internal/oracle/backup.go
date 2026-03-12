package oracle

import "context"

func (c *metricsCollector) collectBackup(ctx context.Context) map[string]any {
	payload := map[string]any{
		"jobs": rowsPayload(c.queryRows(
			ctx,
			"oracle.backup.jobs",
			`SELECT session_key AS "session_key", input_type AS "input_type", status AS "status", start_time AS "start_time", end_time AS "end_time", ROUND(elapsed_seconds / 3600, 2) AS "hours" FROM v$rman_backup_job_details ORDER BY session_key DESC`,
		)),
		"archive_log_mode": c.queryString(ctx, "oracle.backup.archive_log_mode", `SELECT log_mode AS "archive_log_mode" FROM v$database`),
	}
	return mergeMaps(payload, c.collectRecoveryInfo(ctx))
}
