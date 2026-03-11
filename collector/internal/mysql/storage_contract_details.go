package mysql

import "context"

func (c *metricsCollector) topIndexesBySize(ctx context.Context) []map[string]any {
	if !c.tableExists(ctx, "mysql", "innodb_index_stats") {
		return []map[string]any{}
	}
	query := `
SELECT database_name AS table_schema,
       table_name,
       index_name,
       ROUND(stat_value * @@innodb_page_size / 1024 / 1024, 2) AS size_mb
FROM mysql.innodb_index_stats
WHERE stat_name = 'size'
  AND database_name NOT IN ('mysql', 'information_schema', 'performance_schema', 'sys')
ORDER BY stat_value DESC, database_name, table_name, index_name
LIMIT ?`
	return c.queryRows(ctx, "storage.top_indexes_by_size", query, effectiveTopN(c.cfg.TopN))
}

func (c *metricsCollector) tablesWithManyIndexes(ctx context.Context) []map[string]any {
	query := `
SELECT table_schema,
       table_name,
       COUNT(DISTINCT index_name) AS index_count
FROM information_schema.statistics
WHERE table_schema NOT IN ('mysql', 'information_schema', 'performance_schema', 'sys')
GROUP BY table_schema, table_name
HAVING COUNT(DISTINCT index_name) > ?
ORDER BY index_count DESC, table_schema, table_name
LIMIT ?`
	return c.queryRows(ctx, "storage.tables_with_many_indexes", query, manyIndexesThreshold, effectiveTopN(c.cfg.TopN))
}

func (c *metricsCollector) wideCompositeIndexes(ctx context.Context) []map[string]any {
	query := `
SELECT table_schema,
       table_name,
       index_name,
       COUNT(*) AS column_count
FROM information_schema.statistics
WHERE table_schema NOT IN ('mysql', 'information_schema', 'performance_schema', 'sys')
GROUP BY table_schema, table_name, index_name
HAVING COUNT(*) > ?
ORDER BY column_count DESC, table_schema, table_name, index_name
LIMIT ?`
	return c.queryRows(ctx, "storage.wide_composite_indexes", query, wideCompositeThreshold, effectiveTopN(c.cfg.TopN))
}
