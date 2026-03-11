package mysql

import (
	"context"
	"fmt"
	"strings"
)

func (c *metricsCollector) collectReplication(ctx context.Context) map[string]any {
	replicaStatus := c.replicaStatus(ctx)
	enabled := len(replicaStatus) > 0
	ioRunning := isReplicaIORunning(replicaStatus)
	secondsBehind := parseInt64(readMapString(replicaStatus, "Seconds_Behind_Source", "Seconds_Behind_Master"))
	semiSyncTimeout := c.variableInt64(ctx, "rpl_semi_sync_master_timeout", "rpl_semi_sync_source_timeout")
	parallelWorkers := c.variableInt64(ctx, "replica_parallel_workers", "slave_parallel_workers")
	relayLogSpace := parseInt64(readMapString(replicaStatus, "Relay_Log_Space"))
	filters := c.replicationFilters(ctx)
	mgr := c.groupReplicationMetrics(ctx)
	return map[string]any{
		"enabled":                 enabled,
		"io_thread_running":       ioRunning,
		"seconds_behind_master":   secondsBehind,
		"gtid_consistent":         c.gtidConsistent(ctx),
		"semi_sync_status":        normalizeOnOff(c.variableString(ctx, "rpl_semi_sync_master_enabled", "rpl_semi_sync_source_enabled")),
		"semi_sync_timeout_ms":    semiSyncTimeout,
		"parallel_workers":        parallelWorkers,
		"mgr":                     mgr,
		"relay_log_space_bytes":   relayLogSpace,
		"filter_rules":            filters,
		"has_filter_rules":        hasNonEmptyStringValue(filters),
		"replica_status_snapshot": replicaStatus,
	}
}

func (c *metricsCollector) replicaStatus(ctx context.Context) map[string]any {
	if c.serverVersionMajor(ctx) >= 8 {
		status := c.fetchReplicaStatus(ctx, "SHOW REPLICA STATUS")
		if len(status) > 0 {
			return status
		}
	}
	return c.fetchReplicaStatus(ctx, "SHOW SLAVE STATUS")
}

func (c *metricsCollector) fetchReplicaStatus(ctx context.Context, statement string) map[string]any {
	rows := c.queryRows(ctx, "replication.status", statement)
	if len(rows) == 0 {
		return map[string]any{}
	}
	return rows[0]
}

func isReplicaIORunning(status map[string]any) bool {
	value := strings.ToLower(readMapString(status, "Replica_IO_Running", "Slave_IO_Running"))
	return value == "yes" || value == "on" || value == "true"
}

func (c *metricsCollector) gtidConsistent(ctx context.Context) bool {
	mode := strings.ToUpper(strings.TrimSpace(c.variableString(ctx, "gtid_mode")))
	enforce := parseOnOff(c.variableString(ctx, "enforce_gtid_consistency"))
	return mode == "ON" && enforce
}

func (c *metricsCollector) replicationFilters(ctx context.Context) map[string]any {
	return map[string]any{
		"replicate_do_db":         c.variableString(ctx, "replicate_do_db"),
		"replicate_ignore_db":     c.variableString(ctx, "replicate_ignore_db"),
		"replicate_do_table":      c.variableString(ctx, "replicate_do_table"),
		"replicate_wild_do_table": c.variableString(ctx, "replicate_wild_do_table"),
		"binlog_do_db":            c.variableString(ctx, "binlog_do_db"),
		"binlog_ignore_db":        c.variableString(ctx, "binlog_ignore_db"),
	}
}

func (c *metricsCollector) groupReplicationMetrics(ctx context.Context) map[string]any {
	if !c.performanceSchemaTableExists(ctx, "replication_group_members") {
		return map[string]any{
			"enabled":           false,
			"member_state":      "",
			"member_role":       "",
			"consistency_level": "",
		}
	}
	if !c.performanceSchemaColumnExists(ctx, "replication_group_members", "MEMBER_ROLE") {
		query := `
SELECT MEMBER_STATE
FROM performance_schema.replication_group_members
WHERE MEMBER_ID = @@server_uuid`
		rows := c.queryRows(ctx, "replication.mgr", query)
		memberState := ""
		if len(rows) > 0 {
			memberState = readMapString(rows[0], "MEMBER_STATE")
		}
		return map[string]any{
			"enabled":           len(rows) > 0,
			"member_state":      memberState,
			"member_role":       "",
			"consistency_level": c.variableString(ctx, "group_replication_consistency"),
		}
	}
	query := `
SELECT MEMBER_STATE, MEMBER_ROLE
FROM performance_schema.replication_group_members
WHERE MEMBER_ID = @@server_uuid`
	rows := c.queryRows(ctx, "replication.mgr", query)
	memberState := ""
	memberRole := ""
	enabled := len(rows) > 0
	if len(rows) > 0 {
		memberState = readMapString(rows[0], "MEMBER_STATE")
		memberRole = readMapString(rows[0], "MEMBER_ROLE")
	}
	return map[string]any{
		"enabled":           enabled,
		"member_state":      memberState,
		"member_role":       memberRole,
		"consistency_level": c.variableString(ctx, "group_replication_consistency"),
	}
}

func hasNonEmptyStringValue(values map[string]any) bool {
	for _, raw := range values {
		if strings.TrimSpace(readMapString(map[string]any{"value": raw}, "value")) != "" {
			return true
		}
	}
	return false
}

func readMapString(input map[string]any, keys ...string) string {
	for _, key := range keys {
		value, ok := input[key]
		if !ok {
			continue
		}
		switch typed := value.(type) {
		case nil:
			return ""
		case string:
			return typed
		default:
			return strings.TrimSpace(fmt.Sprint(typed))
		}
	}
	return ""
}
