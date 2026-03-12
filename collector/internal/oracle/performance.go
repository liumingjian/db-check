package oracle

import "context"

func (c *metricsCollector) collectPerformance(ctx context.Context) map[string]any {
	payload := map[string]any{
		"active_sessions": rowsPayload(c.queryRows(
			ctx,
			"oracle.performance.active_sessions",
			`SELECT inst_id AS "inst_id", COUNT(1) AS "active_sessions" FROM gv$session WHERE status='ACTIVE' GROUP BY inst_id`,
		)),
		"resource_limits":     rowsPayload(c.queryRows(ctx, "oracle.performance.resource_limits", resourceLimitsQuery)),
		"redo_switch_daily":   rowsPayload(c.queryRows(ctx, "oracle.performance.redo_switch_daily", redoSwitchDailyQuery)),
		"instance_efficiency": rowsPayload(c.queryRows(ctx, "oracle.performance.instance_efficiency", instanceEfficiencyQuery)),
		"tablespace_io_stats": rowsPayload(c.queryRows(
			ctx,
			"oracle.performance.tablespace_io_stats",
			`SELECT df.tablespace_name AS "tablespace_name", df.file_name AS "file_name", f.phyrds AS "phyrds", f.phyblkrd AS "phyblkrd", f.phywrts AS "phywrts", f.phyblkwrt AS "phyblkwrt" FROM v$filestat f, dba_data_files df WHERE f.file# = df.file_id ORDER BY df.tablespace_name`,
		)),
	}
	payload = mergeMaps(payload, c.collectSessionActivity(ctx))
	payload = mergeMaps(payload, c.collectWaitMetrics(ctx))
	return mergeMaps(payload, c.collectPerformanceExtra(ctx))
}

const resourceLimitsQuery = `
SELECT TO_CHAR(inst_id) AS "inst_id",
       resource_name AS "resource_name",
       current_utilization AS "current_utilization",
       max_utilization AS "max_utilization",
       limit_value AS "limit_value"
  FROM gv$resource_limit
 WHERE resource_name IN ('processes', 'sessions', 'parallel_max_servers')`

const redoSwitchDailyQuery = `
SELECT TO_CHAR(first_time,'yyyy-mm-dd') AS "switch_date",
       COUNT(1) AS "switch_count"
  FROM v$log_history
 WHERE first_time >= TRUNC(SYSDATE) - 18
 GROUP BY TO_CHAR(first_time,'yyyy-mm-dd')
 ORDER BY 1`

const instanceEfficiencyQuery = `
SELECT (SELECT value FROM v$sysstat WHERE name = 'db block gets') AS "db_block_gets",
       (SELECT value FROM v$sysstat WHERE name = 'consistent gets') AS "consistent_gets",
       ROUND((SELECT value FROM v$sysstat WHERE name = 'db block reads') / NULLIF((SELECT value FROM v$sysstat WHERE name = 'db block gets'), 0) * 100, 2) AS "db_block_reads_pct",
       ROUND((SELECT value FROM v$sysstat WHERE name = 'db block writes') / NULLIF((SELECT value FROM v$sysstat WHERE name = 'db block reads'), 0) * 100, 2) AS "db_block_writes_pct"
  FROM dual`
