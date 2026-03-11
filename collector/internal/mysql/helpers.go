package mysql

import (
	"strconv"
	"strings"
)

func parseInt64(raw string) int64 {
	if strings.TrimSpace(raw) == "" {
		return 0
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err == nil {
		return value
	}
	floatValue, floatErr := strconv.ParseFloat(raw, 64)
	if floatErr != nil {
		return 0
	}
	return int64(floatValue)
}

func parseFloat64(raw string) float64 {
	if strings.TrimSpace(raw) == "" {
		return 0
	}
	value, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return 0
	}
	return value
}

func parseOnOff(raw string) bool {
	normalized := strings.ToLower(strings.TrimSpace(raw))
	return normalized == "on" || normalized == "1" || normalized == "yes" || normalized == "true"
}

func safeRatio(numerator float64, denominator float64) float64 {
	if denominator == 0 {
		return 0
	}
	return numerator / denominator
}

func safePercent(numerator float64, denominator float64) float64 {
	return safeRatio(numerator, denominator) * percentBase
}

func safePerSecond(value float64, uptimeSeconds int64) float64 {
	if uptimeSeconds <= 0 {
		return 0
	}
	return value / float64(uptimeSeconds)
}

func clampNonNegative(value float64) float64 {
	if value < 0 {
		return 0
	}
	return value
}
