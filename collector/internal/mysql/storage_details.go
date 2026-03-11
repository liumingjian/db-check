package mysql

import "context"

func (c *metricsCollector) wideTables(ctx context.Context) []map[string]any {
	query := `
SELECT table_schema,
       table_name,
       COUNT(*) AS column_count
FROM information_schema.columns
WHERE table_schema NOT IN ('mysql', 'information_schema', 'performance_schema', 'sys')
GROUP BY table_schema, table_name
HAVING COUNT(*) > ?
ORDER BY column_count DESC, table_schema, table_name
LIMIT ?`
	return c.queryRows(ctx, "storage.wide_tables", query, wideTableColumnsThreshold, effectiveTopN(c.cfg.TopN))
}

func (c *metricsCollector) mixedEngineTables(ctx context.Context) []map[string]any {
	query := `
SELECT table_schema,
       table_name,
       engine
FROM information_schema.tables
WHERE table_schema NOT IN ('mysql', 'information_schema', 'performance_schema', 'sys')
  AND table_type = 'BASE TABLE'
  AND UPPER(COALESCE(engine, '')) <> 'INNODB'
ORDER BY table_schema, table_name
LIMIT ?`
	return c.queryRows(ctx, "storage.mixed_engines", query, effectiveTopN(c.cfg.TopN))
}

func (c *metricsCollector) autoIncrementUsageRows(ctx context.Context) []map[string]any {
	query := `
SELECT t.table_schema,
       t.table_name,
       c.column_name,
       ROUND(
         CASE
           WHEN c.data_type = 'tinyint' THEN t.auto_increment / 127 * 100
           WHEN c.data_type = 'smallint' THEN t.auto_increment / 32767 * 100
           WHEN c.data_type = 'mediumint' THEN t.auto_increment / 8388607 * 100
           WHEN c.data_type = 'int' THEN t.auto_increment / 2147483647 * 100
           WHEN c.data_type = 'bigint' THEN t.auto_increment / 9223372036854775807 * 100
           ELSE 0
         END,
         2
       ) AS usage_percent
FROM information_schema.tables t
JOIN information_schema.columns c
  ON c.table_schema = t.table_schema
 AND c.table_name = t.table_name
WHERE t.auto_increment IS NOT NULL
  AND c.extra LIKE '%auto_increment%'
  AND t.table_schema NOT IN ('mysql', 'information_schema', 'performance_schema', 'sys')
ORDER BY usage_percent DESC, t.table_schema, t.table_name
LIMIT ?`
	return c.queryRows(ctx, "storage.auto_increment_usage", query, effectiveTopN(c.cfg.TopN))
}
