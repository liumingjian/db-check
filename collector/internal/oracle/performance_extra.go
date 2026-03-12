package oracle

import "context"

func (c *metricsCollector) collectPerformanceExtra(ctx context.Context) map[string]any {
	return map[string]any{
		"metric_overview": rowsPayload(c.queryRows(ctx, "oracle.performance.metric_overview", metricOverviewQuery)),
		"undo_stats": rowsPayload(c.queryRows(
			ctx,
			"oracle.performance.undo_stats",
			undoStatsQuery(c.cfg.TopN),
		)),
		"undo_tablespace_usage": rowsPayload(c.queryRows(
			ctx,
			"oracle.performance.undo_tablespace_usage",
			undoTablespaceUsageQuery,
		)),
		"sga_resize_ops": rowsPayload(c.queryRows(
			ctx,
			"oracle.performance.sga_resize_ops",
			sgaResizeOpsQuery(c.cfg.TopN),
		)),
		"redo_nowait_pct": c.queryFloat64(ctx, "oracle.performance.redo_nowait_pct", redoNowaitQuery),
	}
}

const metricOverviewQuery = `
SELECT metric_name AS "metric_name",
       ROUND(average, 2) AS "average_value",
       metric_unit AS "metric_unit"
  FROM v$sysmetric_summary
 WHERE metric_name IN (
       'Host CPU Utilization (%)',
       'Database CPU Time Ratio',
       'Executions Per Sec',
       'Physical Reads Per Sec',
       'Physical Writes Per Sec',
       'Logical Reads Per Sec',
       'Redo Generated Per Sec',
       'User Calls Per Sec',
       'Logons Per Sec'
 )
 ORDER BY metric_name`

func undoStatsQuery(topN int) string {
	return `
SELECT * FROM (
  SELECT TO_CHAR(begin_time, 'yyyy-mm-dd hh24:mi:ss') AS "begin_time",
         TO_CHAR(end_time, 'yyyy-mm-dd hh24:mi:ss') AS "end_time",
         undoblks AS "undoblks",
         txncount AS "txncount",
         maxquerylen AS "maxquerylen",
         ssolderrcnt AS "ssolderrcnt",
         nospaceerrcnt AS "nospaceerrcnt"
    FROM v$undostat
   ORDER BY begin_time DESC
) WHERE ROWNUM <= ` + formatTopN(topN)
}

const undoTablespaceUsageQuery = `
WITH undo_datafiles AS (
    SELECT tablespace_name, SUM(bytes) AS total_bytes
      FROM dba_data_files
     WHERE tablespace_name IN (
           SELECT value
             FROM v$parameter
            WHERE name = 'undo_tablespace'
     )
     GROUP BY tablespace_name
),
undo_extents AS (
    SELECT tablespace_name,
           SUM(bytes) AS used_bytes,
           SUM(CASE WHEN status = 'ACTIVE' THEN bytes ELSE 0 END) AS active_bytes,
           SUM(CASE WHEN status = 'UNEXPIRED' THEN bytes ELSE 0 END) AS unexpired_bytes,
           SUM(CASE WHEN status = 'EXPIRED' THEN bytes ELSE 0 END) AS expired_bytes
      FROM dba_undo_extents
     GROUP BY tablespace_name
)
SELECT d.tablespace_name AS "tablespace_name",
       ROUND(d.total_bytes / 1024 / 1024 / 1024, 2) AS "total_size_gb",
       ROUND(NVL(e.used_bytes, 0) / 1024 / 1024 / 1024, 2) AS "used_size_gb",
       ROUND(NVL(e.active_bytes, 0) / 1024 / 1024 / 1024, 2) AS "active_size_gb",
       ROUND(NVL(e.unexpired_bytes, 0) / 1024 / 1024 / 1024, 2) AS "unexpired_size_gb",
       ROUND(NVL(e.expired_bytes, 0) / 1024 / 1024 / 1024, 2) AS "expired_size_gb",
       ROUND(NVL(e.used_bytes, 0) / NULLIF(d.total_bytes, 0) * 100, 2) AS "usage_percent"
  FROM undo_datafiles d
  LEFT JOIN undo_extents e ON e.tablespace_name = d.tablespace_name
 ORDER BY d.tablespace_name`

func sgaResizeOpsQuery(topN int) string {
	return `
SELECT * FROM (
  SELECT component AS "component",
         oper_type AS "oper_type",
         oper_mode AS "oper_mode",
         parameter AS "parameter",
         ROUND(initial_size / 1024 / 1024, 2) AS "initial_size_mb",
         ROUND(target_size / 1024 / 1024, 2) AS "target_size_mb",
         ROUND(final_size / 1024 / 1024, 2) AS "final_size_mb",
         TO_CHAR(start_time, 'yyyy-mm-dd hh24:mi:ss') AS "start_time",
         TO_CHAR(end_time, 'yyyy-mm-dd hh24:mi:ss') AS "end_time",
         status AS "status"
    FROM v$sga_resize_ops
   ORDER BY start_time DESC
) WHERE ROWNUM <= ` + formatTopN(topN)
}

const redoNowaitQuery = `
SELECT ROUND(
           (1 - retries.value / NULLIF(entries.value, 0)) * 100,
           2
       ) AS "redo_nowait_pct"
  FROM (
        SELECT value
          FROM v$sysstat
         WHERE name = 'redo buffer allocation retries'
  ) retries,
  (
        SELECT value
          FROM v$sysstat
         WHERE name = 'redo entries'
  ) entries`
