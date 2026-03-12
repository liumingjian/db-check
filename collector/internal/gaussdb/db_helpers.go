package gaussdb

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

func queryRows(ctx context.Context, db *sql.DB, timeoutSeconds int, query string) ([]map[string]any, error) {
	queryCtx, cancel := context.WithTimeout(ctx, time.Duration(timeoutSeconds)*time.Second)
	defer cancel()
	rows, err := db.QueryContext(queryCtx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	result := make([]map[string]any, 0)
	for rows.Next() {
		item, scanErr := scanRow(columns, rows)
		if scanErr != nil {
			return nil, scanErr
		}
		result = append(result, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func scanRow(columns []string, rows *sql.Rows) (map[string]any, error) {
	values := make([]any, len(columns))
	pointers := make([]any, len(columns))
	for index := range columns {
		pointers[index] = &values[index]
	}
	if err := rows.Scan(pointers...); err != nil {
		return nil, err
	}
	item := make(map[string]any, len(columns))
	for index, name := range columns {
		item[name] = toJSONValue(values[index])
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

func queryError(scope string, err error) error {
	return fmt.Errorf("%s: %w", scope, err)
}
