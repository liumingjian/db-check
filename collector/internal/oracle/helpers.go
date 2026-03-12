package oracle

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

func (c *metricsCollector) addErr(scope string, err error) {
	if err == nil {
		return
	}
	c.errors = append(c.errors, fmt.Sprintf("%s: %v", scope, err))
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

func rowsPayload(rows []map[string]any) map[string]any {
	return map[string]any{
		"items": rows,
		"count": len(rows),
	}
}

func mergeMaps(left map[string]any, right map[string]any) map[string]any {
	merged := make(map[string]any, len(left)+len(right))
	for key, value := range left {
		merged[key] = value
	}
	for key, value := range right {
		merged[key] = value
	}
	return merged
}

func yesNoToBool(raw string) bool {
	return raw == "Yes" || raw == "YES" || raw == "Y" || raw == "TRUE"
}
