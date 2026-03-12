package oracle

import "context"

func (c *metricsCollector) collectSQLSummary(ctx context.Context) map[string]any {
	return map[string]any{
		"high_parse_count_sql": rowsPayload(c.queryRows(
			ctx,
			"oracle.sql.high_parse_count_sql",
			highParseCountSQLQuery(c.cfg.TopN),
		)),
		"high_version_count_sql": rowsPayload(c.queryRows(
			ctx,
			"oracle.sql.high_version_count_sql",
			highVersionCountSQLQuery(c.cfg.TopN),
		)),
		"sql_with_executions_ratio_pct": c.queryFloat64(
			ctx,
			"oracle.sql.sql_with_executions_ratio_pct",
			sqlWithExecutionsRatioQuery,
		),
		"memory_for_sql_with_executions_ratio_pct": c.queryFloat64(
			ctx,
			"oracle.sql.memory_for_sql_with_executions_ratio_pct",
			memoryForSQLWithExecutionsRatioQuery,
		),
	}
}

func highParseCountSQLQuery(topN int) string {
	return `
SELECT * FROM (
  SELECT sql_id AS "sql_id",
         executions AS "executions",
         parse_calls AS "parse_calls",
         version_count AS "version_count",
         sharable_mem AS "sharable_mem",
         sorts AS "sorts",
         sql_text AS "sql_text"
    FROM v$sqlarea
   WHERE parse_calls > 0
     AND executions > 10000
     AND executions / parse_calls < 1.1
   ORDER BY parse_calls DESC
) WHERE ROWNUM <= ` + formatTopN(topN)
}

func highVersionCountSQLQuery(topN int) string {
	return `
SELECT * FROM (
  SELECT sql_id AS "sql_id",
         version_count AS "version_count",
         executions AS "executions",
         parse_calls AS "parse_calls",
         sharable_mem AS "sharable_mem",
         sorts AS "sorts",
         sql_text AS "sql_text"
    FROM v$sqlarea
   WHERE version_count > 50
   ORDER BY version_count DESC
) WHERE ROWNUM <= ` + formatTopN(topN)
}

const sqlWithExecutionsRatioQuery = `
SELECT ROUND(
           COUNT(CASE WHEN executions > 1 THEN 1 END) / NULLIF(COUNT(*), 0) * 100,
           2
       ) AS "sql_with_executions_ratio_pct"
  FROM v$sql`

const memoryForSQLWithExecutionsRatioQuery = `
SELECT ROUND(
           SUM(CASE WHEN executions > 1 THEN sharable_mem ELSE 0 END) / NULLIF(SUM(sharable_mem), 0) * 100,
           2
       ) AS "memory_for_sql_with_executions_ratio_pct"
  FROM v$sql`
