package oracle

import "context"

func (c *metricsCollector) collectRecoveryInfo(ctx context.Context) map[string]any {
	return map[string]any{
		"archive_destinations":       rowsPayload(c.queryRows(ctx, "oracle.backup.archive_destinations", archiveDestinationsQuery)),
		"archive_destination_errors": rowsPayload(c.queryRows(ctx, "oracle.backup.archive_destination_errors", archiveDestinationErrorsQuery)),
		"archive_log_summary":        rowsPayload(c.queryRows(ctx, "oracle.backup.archive_log_summary", archiveLogSummaryQuery)),
		"recovery_area":              rowsPayload(c.queryRows(ctx, "oracle.backup.recovery_area", recoveryAreaQuery)),
	}
}

const archiveDestinationsQuery = `
SELECT dest_name AS "dest_name",
       status AS "status",
       destination AS "destination",
       target AS "target",
       archiver AS "archiver",
       error AS "error"
  FROM v$archive_dest_status
 WHERE destination IS NOT NULL`

const recoveryAreaQuery = `
SELECT name AS "name",
       ROUND(space_limit/1024/1024/1024, 2) AS "space_limit_gb",
       ROUND(space_used/1024/1024/1024, 2) AS "space_used_gb",
       ROUND(space_reclaimable/1024/1024/1024, 2) AS "space_reclaimable_gb",
       ROUND(space_used / NULLIF(space_limit, 0) * 100, 2) AS "space_used_pct",
       number_of_files AS "number_of_files"
  FROM v$recovery_file_dest`

const archiveDestinationErrorsQuery = `
SELECT dest_name AS "dest_name",
       status AS "status",
       destination AS "destination",
       error AS "error"
  FROM v$archive_dest_status
 WHERE destination IS NOT NULL
   AND error IS NOT NULL
   AND TRIM(error) <> ''
   AND UPPER(error) <> 'NO ERROR'`

const archiveLogSummaryQuery = `
SELECT COUNT(*) AS "archive_count",
       ROUND(SUM(blocks * block_size) / 1024 / 1024 / 1024, 2) AS "archive_size_gb",
       TO_CHAR(MIN(first_time), 'yyyy-mm-dd hh24:mi:ss') AS "oldest_archive_time",
       TO_CHAR(MAX(first_time), 'yyyy-mm-dd hh24:mi:ss') AS "newest_archive_time"
  FROM v$archived_log
 WHERE deleted = 'NO'`
