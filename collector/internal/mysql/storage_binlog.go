package mysql

import (
	"context"
	"database/sql"
	"fmt"
)

func (c *metricsCollector) binlogDiskUsageGiB(ctx context.Context) float64 {
	rows, err := c.db.QueryContext(ctx, "SHOW BINARY LOGS")
	if err != nil {
		c.addErr("storage.binlog_disk_usage_bytes", err)
		return 0
	}
	defer rows.Close()

	total := float64(0)
	columns, err := rows.Columns()
	if err != nil {
		c.addErr("storage.binlog_disk_usage_bytes", err)
		return 0
	}
	for rows.Next() {
		size, scanErr := scanBinaryLogSize(rows, len(columns))
		if scanErr != nil {
			c.addErr("storage.binlog_disk_usage_bytes", scanErr)
			return 0
		}
		total += size
	}
	return c.bytesToGiB(total)
}

func scanBinaryLogSize(rows *sql.Rows, columnCount int) (float64, error) {
	var logName sql.NullString
	var fileSize sql.NullFloat64
	switch columnCount {
	case 2:
		if err := rows.Scan(&logName, &fileSize); err != nil {
			return 0, err
		}
	case 3:
		var encrypted sql.NullString
		if err := rows.Scan(&logName, &fileSize, &encrypted); err != nil {
			return 0, err
		}
	default:
		return 0, fmt.Errorf("unexpected SHOW BINARY LOGS column count: %d", columnCount)
	}
	if !fileSize.Valid {
		return 0, nil
	}
	return fileSize.Float64, nil
}

func (c *metricsCollector) logFileSizesGiB(ctx context.Context) float64 {
	redoCapacity := c.variableInt64(ctx, "innodb_redo_log_capacity")
	if redoCapacity > 0 {
		return c.bytesToGiB(float64(redoCapacity))
	}
	logFileSize := c.variableInt64(ctx, "innodb_log_file_size")
	logFilesInGroup := c.variableInt64(ctx, "innodb_log_files_in_group")
	if logFilesInGroup <= 0 {
		logFilesInGroup = 2
	}
	return c.bytesToGiB(float64(logFileSize * logFilesInGroup))
}

func (c *metricsCollector) bytesToMiB(value float64) float64 {
	return clampNonNegative(value / bytesPerMiB)
}

func (c *metricsCollector) bytesToGiB(value float64) float64 {
	return clampNonNegative(value / bytesPerGiB)
}
