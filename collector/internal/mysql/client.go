package mysql

import (
	"context"
	"database/sql"
	"dbcheck/collector/internal/cli"
	"fmt"
	"time"
)

const (
	percentBase               = 100.0
	millisecondsPerSecond     = 1000.0
	nanoToMillisecondDivisor  = 1_000_000.0
	picoToMillisecondDivisor  = 1_000_000_000.0
	secondsPerHour            = 3600.0
	secondsPerDay             = 86400.0
	bytesPerMiB               = 1024.0 * 1024.0
	bytesPerGiB               = 1024.0 * 1024.0 * 1024.0
	defaultTopLimit           = 20
	longTransactionSeconds    = 60
	largeTableRowsThreshold   = 10000
	wideTableColumnsThreshold = 50
	manyIndexesThreshold      = 6
	wideCompositeThreshold    = 4
	ibdataPattern             = "ibdata%"
	undoTablespacePattern     = "%undo%"
	tempTablespacePattern     = "%temp%"
	ibtmpPattern              = "%ibtmp%"
	innodbLogPattern          = "%ib_logfile%"
)

type metricsCollector struct {
	db          *sql.DB
	cfg         cli.Config
	varCache    map[string]string
	statCache   map[string]string
	tableCache  map[string]bool
	columnCache map[string]bool
	errors      []string
}

func newMetricsCollector(db *sql.DB, cfg cli.Config) *metricsCollector {
	return &metricsCollector{db: db, cfg: cfg, errors: []string{}}
}

func (c *metricsCollector) collectAll(ctx context.Context) map[string]any {
	return map[string]any{
		"basic_info":   c.collectBasicInfo(ctx),
		"config_check": c.collectConfigCheck(ctx),
		"performance":  c.collectPerformance(ctx),
		"replication":  c.collectReplication(ctx),
		"security":     c.collectSecurity(ctx),
		"backup":       c.collectBackup(ctx),
		"sql_analysis": c.collectSQLAnalysis(ctx),
		"storage":      c.collectStorage(ctx),
	}
}

func (c *metricsCollector) addErr(scope string, err error) {
	if err == nil {
		return
	}
	c.errors = append(c.errors, fmt.Sprintf("%s: %v", scope, err))
}

func (c *metricsCollector) loadVariables(ctx context.Context) map[string]string {
	if c.varCache != nil {
		return c.varCache
	}
	result, err := c.queryKeyValue(ctx, "SHOW GLOBAL VARIABLES")
	if err != nil {
		c.addErr("show global variables", err)
		result = map[string]string{}
	}
	c.varCache = result
	return c.varCache
}

func (c *metricsCollector) loadStatus(ctx context.Context) map[string]string {
	if c.statCache != nil {
		return c.statCache
	}
	result, err := c.queryKeyValue(ctx, "SHOW GLOBAL STATUS")
	if err != nil {
		c.addErr("show global status", err)
		result = map[string]string{}
	}
	c.statCache = result
	return c.statCache
}

func (c *metricsCollector) variableString(ctx context.Context, keys ...string) string {
	vars := c.loadVariables(ctx)
	for _, key := range keys {
		if value, ok := vars[key]; ok {
			return value
		}
	}
	return ""
}

func (c *metricsCollector) variableInt64(ctx context.Context, keys ...string) int64 {
	return parseInt64(c.variableString(ctx, keys...))
}

func (c *metricsCollector) statusString(ctx context.Context, keys ...string) string {
	stats := c.loadStatus(ctx)
	for _, key := range keys {
		if value, ok := stats[key]; ok {
			return value
		}
	}
	return ""
}

func (c *metricsCollector) statusInt64(ctx context.Context, keys ...string) int64 {
	return parseInt64(c.statusString(ctx, keys...))
}

func (c *metricsCollector) statusFloat64(ctx context.Context, keys ...string) float64 {
	return parseFloat64(c.statusString(ctx, keys...))
}

func (c *metricsCollector) queryKeyValue(ctx context.Context, query string) (map[string]string, error) {
	rows, err := c.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := map[string]string{}
	for rows.Next() {
		var key string
		var value sql.NullString
		if err := rows.Scan(&key, &value); err != nil {
			return nil, err
		}
		if value.Valid {
			result[key] = value.String
			continue
		}
		result[key] = ""
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func (c *metricsCollector) queryInt64(ctx context.Context, scope string, query string, args ...any) int64 {
	var value sql.NullInt64
	if err := c.db.QueryRowContext(ctx, query, args...).Scan(&value); err != nil {
		c.addErr(scope, err)
		return 0
	}
	if value.Valid {
		return value.Int64
	}
	return 0
}

func (c *metricsCollector) queryFloat64(ctx context.Context, scope string, query string, args ...any) float64 {
	var value sql.NullFloat64
	if err := c.db.QueryRowContext(ctx, query, args...).Scan(&value); err != nil {
		c.addErr(scope, err)
		return 0
	}
	if value.Valid {
		return value.Float64
	}
	return 0
}

func (c *metricsCollector) queryString(ctx context.Context, scope string, query string, args ...any) string {
	var value sql.NullString
	if err := c.db.QueryRowContext(ctx, query, args...).Scan(&value); err != nil {
		c.addErr(scope, err)
		return ""
	}
	if value.Valid {
		return value.String
	}
	return ""
}

func (c *metricsCollector) queryRows(ctx context.Context, scope string, query string, args ...any) []map[string]any {
	rows, err := c.db.QueryContext(ctx, query, args...)
	if err != nil {
		c.addErr(scope, err)
		return []map[string]any{}
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		c.addErr(scope, err)
		return []map[string]any{}
	}
	result := []map[string]any{}
	for rows.Next() {
		item, scanErr := scanRow(columns, rows)
		if scanErr != nil {
			c.addErr(scope, scanErr)
			return result
		}
		result = append(result, item)
	}
	if err := rows.Err(); err != nil {
		c.addErr(scope, err)
	}
	return result
}

func scanRow(columns []string, rows *sql.Rows) (map[string]any, error) {
	values := make([]any, len(columns))
	valuePtrs := make([]any, len(columns))
	for idx := range columns {
		valuePtrs[idx] = &values[idx]
	}
	if err := rows.Scan(valuePtrs...); err != nil {
		return nil, err
	}
	item := make(map[string]any, len(columns))
	for idx, name := range columns {
		item[name] = toJSONValue(values[idx])
	}
	return item, nil
}

func toJSONValue(value any) any {
	switch typed := value.(type) {
	case nil:
		return nil
	case []byte:
		return string(typed)
	case time.Time:
		return typed.Format(time.RFC3339Nano)
	default:
		return typed
	}
}

func effectiveTopN(value int) int {
	if value > 0 {
		return value
	}
	return defaultTopLimit
}

func rowsPayload(rows []map[string]any) map[string]any {
	return map[string]any{"items": rows}
}

func normalizeOnOff(raw string) string {
	if parseOnOff(raw) {
		return "ON"
	}
	return "OFF"
}
