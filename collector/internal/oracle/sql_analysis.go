package oracle

import (
	"context"
	"fmt"
)

func (c *metricsCollector) collectSQLAnalysis(ctx context.Context) map[string]any {
	payload := map[string]any{
		"top_sql_by_elapsed_time": rowsPayload(c.queryRows(ctx, "oracle.sql.top_elapsed", topSQLByElapsedTimeQuery(c.cfg.TopN))),
		"top_sql_by_buffer_gets":  rowsPayload(c.queryRows(ctx, "oracle.sql.top_buffer_gets", topSQLByBufferGetsQuery(c.cfg.TopN))),
		"top_sql_by_disk_reads":   rowsPayload(c.queryRows(ctx, "oracle.sql.top_disk_reads", topSQLByDiskReadsQuery(c.cfg.TopN))),
		"top_sql_by_executions":   rowsPayload(c.queryRows(ctx, "oracle.sql.top_executions", topSQLByExecutionsQuery(c.cfg.TopN))),
	}
	return mergeMaps(payload, c.collectSQLSummary(ctx))
}

func topSQLByElapsedTimeQuery(topN int) string {
	return `
SELECT * FROM (
  SELECT parsing_schema_name AS "owner",
         sql_fulltext AS "sql_text",
         ROUND(elapsed_time/1000/1000, 2) AS "elapsed_time_sec",
         ROUND(cpu_time/1000/1000, 2) AS "cpu_time_sec",
         executions AS "executions",
         buffer_gets AS "buffer_gets",
         disk_reads AS "disk_reads"
    FROM v$sqlarea
   WHERE elapsed_time/1000/1000 > 5
   ORDER BY elapsed_time DESC
) WHERE ROWNUM <= ` + formatTopN(topN)
}

func topSQLByBufferGetsQuery(topN int) string {
	return `
SELECT * FROM (
  SELECT disk_reads AS "disk_reads",
         buffer_gets AS "buffer_gets",
         executions AS "executions",
         hash_value AS "hash_value",
         sql_text AS "sql_text"
    FROM v$sqlarea
   WHERE executions > 0
   ORDER BY buffer_gets DESC
) WHERE ROWNUM <= ` + formatTopN(topN)
}

func topSQLByDiskReadsQuery(topN int) string {
	return `
SELECT * FROM (
  SELECT disk_reads AS "disk_reads",
         buffer_gets AS "buffer_gets",
         executions AS "executions",
         hash_value AS "hash_value",
         sql_text AS "sql_text"
    FROM v$sqlarea
   WHERE executions > 0
   ORDER BY disk_reads DESC
) WHERE ROWNUM <= ` + formatTopN(topN)
}

func topSQLByExecutionsQuery(topN int) string {
	return `
SELECT * FROM (
  SELECT executions AS "executions",
         buffer_gets AS "buffer_gets",
         disk_reads AS "disk_reads",
         hash_value AS "hash_value",
         sql_text AS "sql_text"
    FROM v$sqlarea
   WHERE executions > 0
   ORDER BY executions DESC
) WHERE ROWNUM <= ` + formatTopN(topN)
}

func formatTopN(topN int) string {
	if topN <= 0 {
		return "20"
	}
	return fmt.Sprintf("%d", topN)
}
