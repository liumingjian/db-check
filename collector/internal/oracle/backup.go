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
	// A missing successful data backup exceeds both age thresholds.
	payload["successful_backup_age_hours"] = c.queryFloat64(ctx, "oracle.backup.successful_backup_age_hours", `SELECT NVL((SYSDATE-MAX(end_time))*24, 1000000000) FROM v$rman_backup_job_details WHERE status='COMPLETED' AND input_type IN ('DB FULL','DB INCR')`)
	payload["failed_jobs"] = rowsPayload(c.queryRows(ctx, "oracle.backup.failed_jobs", `SELECT session_key AS "session_key", input_type AS "input_type", status AS "status", start_time AS "start_time", end_time AS "end_time" FROM v$rman_backup_job_details WHERE start_time >= SYSDATE-7 AND status IN ('FAILED','COMPLETED WITH ERRORS') ORDER BY start_time DESC`))
	payload["flashback_on"] = c.queryString(ctx, "oracle.backup.flashback_on", `SELECT flashback_on FROM v$database`)
	payload["block_corruption"] = rowsPayload(c.queryRows(ctx, "oracle.backup.block_corruption", `SELECT file# AS "file_number", block# AS "block_number", blocks AS "blocks", corruption_type AS "corruption_type" FROM v$database_block_corruption ORDER BY file#, block#`))
	return mergeMaps(payload, c.collectRecoveryInfo(ctx))
}
