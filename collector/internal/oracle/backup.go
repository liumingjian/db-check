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
	rows := c.queryRows(ctx, "oracle.backup.successful_backup_age_hours", `SELECT (SYSDATE-MAX(end_time))*24 AS "age_hours", COUNT(end_time) AS "backup_count" FROM v$rman_backup_job_details WHERE status='COMPLETED' AND input_type IN ('DB FULL','DB INCR')`)
	payload["successful_backup_age_hours"] = nil
	payload["successful_data_backup"] = nil
	if len(rows) == 1 {
		payload["successful_backup_age_hours"] = rows[0]["age_hours"]
		payload["successful_data_backup"] = rows[0]["backup_count"] != int64(0) && rows[0]["backup_count"] != float64(0) && rows[0]["backup_count"] != "0"
	}
	c.availability["db.backup.successful_data_backup"] = c.availability["db.backup.successful_backup_age_hours"]
	payload["failed_jobs"] = rowsPayload(c.queryRows(ctx, "oracle.backup.failed_jobs", `SELECT session_key AS "session_key", input_type AS "input_type", status AS "status", start_time AS "start_time", end_time AS "end_time" FROM v$rman_backup_job_details WHERE start_time >= SYSDATE-7 AND status IN ('FAILED','COMPLETED WITH ERRORS') ORDER BY start_time DESC`))
	payload["flashback_on"] = c.queryString(ctx, "oracle.backup.flashback_on", `SELECT flashback_on FROM v$database`)
	payload["block_corruption"] = rowsPayload(c.queryRows(ctx, "oracle.backup.block_corruption", `SELECT file# AS "file_number", block# AS "block_number", blocks AS "blocks", corruption_type AS "corruption_type" FROM v$database_block_corruption ORDER BY file#, block#`))
	return mergeMaps(payload, c.collectRecoveryInfo(ctx))
}
