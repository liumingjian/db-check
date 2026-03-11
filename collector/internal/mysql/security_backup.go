package mysql

import "context"

func (c *metricsCollector) collectSecurity(ctx context.Context) map[string]any {
	rootRemote := c.queryInt64(ctx, "security.root_remote_login", `
SELECT COUNT(*)
FROM mysql.user
WHERE user = 'root' AND host NOT IN ('localhost', '127.0.0.1', '::1')`)
	emptyPasswords := c.queryRows(ctx, "security.empty_password_users", `
SELECT user, host, plugin
FROM mysql.user
WHERE user <> '' AND (authentication_string IS NULL OR authentication_string = '')`)
	superUsers := c.queryRows(ctx, "security.super_privilege_users", `
SELECT user, host
FROM mysql.user
WHERE super_priv = 'Y'
  AND user NOT IN ('root', 'mysql.session', 'mysql.sys', 'mysql.infoschema')`)
	allDBUsers := c.queryRows(ctx, "security.all_db_privilege_users", `
SELECT user, host, db
FROM mysql.db
WHERE Select_priv='Y' AND Insert_priv='Y' AND Update_priv='Y' AND Delete_priv='Y'`)
	anonymous := c.queryRows(ctx, "security.anonymous_users", `
SELECT user, host
FROM mysql.user
WHERE user = ''`)
	authPlugins := c.queryRows(ctx, "security.auth_plugin_check", `
SELECT plugin, COUNT(*) AS user_count
FROM mysql.user
GROUP BY plugin
ORDER BY user_count DESC`)
	legacyAuthPlugins := c.queryRows(ctx, "security.legacy_auth_plugin_users", `
SELECT user, host, plugin
FROM mysql.user
WHERE plugin IN ('mysql_native_password', 'sha256_password', 'mysql_old_password')`)
	passwordPolicy := c.passwordPolicy(ctx)
	lockout := c.loginFailureLockoutEnabled(ctx)
	sslEnabled := c.sslEnabled(ctx)
	return map[string]any{
		"root_remote_login":        rootRemote > 0,
		"empty_password_users":     rowsPayload(emptyPasswords),
		"password_policy":          passwordPolicy,
		"password_expiry_days":     c.variableInt64(ctx, "default_password_lifetime"),
		"login_failure_lockout":    lockout,
		"super_privilege_users":    rowsPayload(superUsers),
		"all_db_privilege_users":   rowsPayload(allDBUsers),
		"anonymous_users":          rowsPayload(anonymous),
		"test_database_exists":     c.testDatabaseExists(ctx),
		"auth_plugin_check":        rowsPayload(authPlugins),
		"legacy_auth_plugin_users": rowsPayload(legacyAuthPlugins),
		"ssl_enabled":              sslEnabled,
		"audit_log_enabled":        normalizeOnOff(c.variableString(ctx, "audit_log")),
		"binlog_enabled":           normalizeOnOff(c.variableString(ctx, "log_bin")),
		"local_infile":             normalizeOnOff(c.variableString(ctx, "local_infile")),
		"skip_name_resolve":        normalizeOnOff(c.variableString(ctx, "skip_name_resolve")),
		"symbolic_links":           normalizeOnOff(c.variableString(ctx, "symbolic-links", "symbolic_links", "have_symlink")),
	}
}

func (c *metricsCollector) passwordPolicy(ctx context.Context) map[string]any {
	policy := c.variableString(ctx, "validate_password.policy")
	return map[string]any{
		"enabled":            policy != "",
		"policy":             policy,
		"length":             c.variableInt64(ctx, "validate_password.length"),
		"mixed_case_count":   c.variableInt64(ctx, "validate_password.mixed_case_count"),
		"number_count":       c.variableInt64(ctx, "validate_password.number_count"),
		"special_char_count": c.variableInt64(ctx, "validate_password.special_char_count"),
	}
}

func (c *metricsCollector) loginFailureLockoutEnabled(ctx context.Context) bool {
	threshold := c.variableInt64(ctx, "connection_control_failed_connections_threshold")
	if threshold > 0 {
		return true
	}
	maxAttempts := c.variableInt64(ctx, "failed_login_attempts")
	return maxAttempts > 0
}

func (c *metricsCollector) testDatabaseExists(ctx context.Context) bool {
	query := "SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name = 'test'"
	count := c.queryInt64(ctx, "security.test_database_exists", query)
	return count > 0
}

func (c *metricsCollector) sslEnabled(ctx context.Context) string {
	requireTransport := c.variableString(ctx, "require_secure_transport")
	if parseOnOff(requireTransport) {
		return "ON"
	}
	haveSSL := c.variableString(ctx, "have_ssl")
	if haveSSL == "YES" {
		return "ON"
	}
	return "OFF"
}

func (c *metricsCollector) collectBackup(ctx context.Context) map[string]any {
	historyExists := c.backupHistoryExists(ctx)
	strategy := historyExists || parseOnOff(c.variableString(ctx, "log_bin"))
	lastAgeHours := c.lastFullBackupAgeHours(ctx, historyExists)
	integrity := c.lastBackupIntegrity(ctx, historyExists)
	sizeTrend := c.backupSizeTrend(ctx, historyExists)
	retentionDays := c.binlogRetentionDays(ctx)
	binlogBytes := c.statusFloat64(ctx, "Binlog_bytes_written")
	uptime := c.statusInt64(ctx, "Uptime")
	return map[string]any{
		"strategy_exists":            strategy,
		"last_full_backup_age_hours": lastAgeHours,
		"last_backup_integrity":      integrity,
		"backup_size_trend":          rowsPayload(sizeTrend),
		"binlog_retention_policy":    retentionDays,
		"binlog_format":              c.variableString(ctx, "binlog_format"),
		"binlog_write_rate":          safePerSecond(binlogBytes, uptime),
	}
}

func (c *metricsCollector) backupHistoryExists(ctx context.Context) bool {
	query := `
SELECT COUNT(*)
FROM information_schema.tables
WHERE table_schema = ? AND table_name = 'backup_history'`
	count := c.queryInt64(ctx, "backup.history_exists", query, c.cfg.DBName)
	return count > 0
}

func (c *metricsCollector) lastFullBackupAgeHours(ctx context.Context, historyExists bool) any {
	if !historyExists {
		return nil
	}
	query := `
SELECT TIMESTAMPDIFF(HOUR, MAX(backup_time), NOW())
FROM backup_history
WHERE backup_type = 'full'`
	return c.queryInt64(ctx, "backup.last_full_backup_age_hours", query)
}

func (c *metricsCollector) lastBackupIntegrity(ctx context.Context, historyExists bool) any {
	if !historyExists {
		return nil
	}
	query := `
SELECT is_valid
FROM backup_history
ORDER BY backup_time DESC
LIMIT 1`
	value := c.queryInt64(ctx, "backup.last_backup_integrity", query)
	return value == 1
}

func (c *metricsCollector) backupSizeTrend(ctx context.Context, historyExists bool) []map[string]any {
	if !historyExists {
		return []map[string]any{}
	}
	query := `
SELECT backup_time, backup_size_mb
FROM backup_history
ORDER BY backup_time DESC
LIMIT 7`
	return c.queryRows(ctx, "backup.backup_size_trend", query)
}

func (c *metricsCollector) binlogRetentionDays(ctx context.Context) float64 {
	seconds := c.variableInt64(ctx, "binlog_expire_logs_seconds")
	if seconds > 0 {
		return float64(seconds) / secondsPerDay
	}
	days := c.variableInt64(ctx, "expire_logs_days")
	return float64(days)
}
