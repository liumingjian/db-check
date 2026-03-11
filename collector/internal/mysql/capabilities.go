package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

func (c *metricsCollector) performanceSchemaTableExists(ctx context.Context, table string) bool {
	return c.tableExists(ctx, "performance_schema", table)
}

func (c *metricsCollector) performanceSchemaColumnExists(ctx context.Context, table string, column string) bool {
	return c.columnExists(ctx, "performance_schema", table, column)
}

func (c *metricsCollector) tableExists(ctx context.Context, schema string, table string) bool {
	if c.tableCache == nil {
		c.tableCache = map[string]bool{}
	}
	cacheKey := fmt.Sprintf("%s.%s", schema, table)
	if value, ok := c.tableCache[cacheKey]; ok {
		return value
	}
	query := `
SELECT COUNT(*)
FROM information_schema.tables
WHERE table_schema = ?
  AND table_name = ?`
	var count sql.NullInt64
	if err := c.db.QueryRowContext(ctx, query, schema, table).Scan(&count); err != nil {
		c.addErr("capabilities.table_exists", err)
		c.tableCache[cacheKey] = false
		return false
	}
	exists := count.Valid && count.Int64 > 0
	c.tableCache[cacheKey] = exists
	return exists
}

func (c *metricsCollector) columnExists(ctx context.Context, schema string, table string, column string) bool {
	if c.columnCache == nil {
		c.columnCache = map[string]bool{}
	}
	cacheKey := fmt.Sprintf("%s.%s.%s", schema, table, column)
	if value, ok := c.columnCache[cacheKey]; ok {
		return value
	}
	query := `
SELECT COUNT(*)
FROM information_schema.columns
WHERE table_schema = ?
  AND table_name = ?
  AND column_name = ?`
	var count sql.NullInt64
	if err := c.db.QueryRowContext(ctx, query, schema, table, column).Scan(&count); err != nil {
		c.addErr("capabilities.column_exists", err)
		c.columnCache[cacheKey] = false
		return false
	}
	exists := count.Valid && count.Int64 > 0
	c.columnCache[cacheKey] = exists
	return exists
}

func (c *metricsCollector) serverVersionMajor(ctx context.Context) int64 {
	raw := strings.TrimSpace(c.variableString(ctx, "version"))
	versionPart := strings.SplitN(raw, "-", 2)[0]
	return parseInt64(strings.SplitN(versionPart, ".", 2)[0])
}
