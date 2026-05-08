package gaussdb

import (
	"strconv"
	"strings"
)

const (
	statusOK   = "OK"
	statusNG   = "NG"
	statusNone = "NONE"
)

func formatAny(value any) string {
	switch typed := value.(type) {
	case int:
		return strconv.Itoa(typed)
	case int64:
		return strconv.FormatInt(typed, 10)
	case float64:
		return strconv.FormatFloat(typed, 'f', -1, 64)
	case float32:
		return strconv.FormatFloat(float64(typed), 'f', -1, 64)
	case string:
		return strings.TrimSpace(typed)
	default:
		return ""
	}
}
