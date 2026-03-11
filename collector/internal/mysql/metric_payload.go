package mysql

import "fmt"

const (
	payloadCountKey    = "count"
	payloadMaxValueKey = "max_value"
)

func rowsPayloadWithStats(rows []map[string]any, maxValue float64) map[string]any {
	payload := rowsPayload(rows)
	payload[payloadCountKey] = len(rows)
	payload[payloadMaxValueKey] = maxValue
	return payload
}

func maxFieldValue(rows []map[string]any, keys ...string) float64 {
	maxValue := 0.0
	for _, row := range rows {
		value := rowNumericValue(row, keys...)
		if value > maxValue {
			maxValue = value
		}
	}
	return maxValue
}

func rowNumericValue(row map[string]any, keys ...string) float64 {
	for _, key := range keys {
		if value, ok := row[key]; ok {
			return anyToFloat64(value)
		}
	}
	return 0
}

func anyToFloat64(value any) float64 {
	switch typed := value.(type) {
	case int:
		return float64(typed)
	case int64:
		return float64(typed)
	case float64:
		return typed
	case []byte:
		return parseFloat64(string(typed))
	case string:
		return parseFloat64(typed)
	default:
		return parseFloat64(fmt.Sprint(value))
	}
}
