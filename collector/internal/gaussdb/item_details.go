package gaussdb

import (
	"encoding/json"
	"slices"
	"strconv"
	"strings"
)

type parameterGroup struct {
	Key    string
	Title  string
	Params []parameterSpec
}

type parameterSpec struct {
	Name  string
	Label string
}

var keyParameterGroups = [...]parameterGroup{
	{
		Key:   "memory_connection",
		Title: "内存与连接参数",
		Params: []parameterSpec{
			{Name: "max_connections", Label: "最大连接数"},
			{Name: "shared_buffers", Label: "共享缓冲区"},
			{Name: "work_mem", Label: "会话工作内存"},
			{Name: "maintenance_work_mem", Label: "维护工作内存"},
			{Name: "temp_buffers", Label: "临时缓冲区"},
			{Name: "wal_buffers", Label: "WAL 缓冲区"},
		},
	},
	{
		Key:   "checkpoint_wal",
		Title: "检查点与 WAL 参数",
		Params: []parameterSpec{
			{Name: "checkpoint_timeout", Label: "检查点超时"},
			{Name: "checkpoint_segments", Label: "检查点段数"},
			{Name: "checkpoint_warning", Label: "检查点告警阈值"},
			{Name: "checkpoint_flush_after", Label: "检查点刷盘粒度"},
			{Name: "wal_level", Label: "WAL 级别"},
			{Name: "full_page_writes", Label: "全页写"},
		},
	},
	{
		Key:   "replication_ha",
		Title: "高可用与复制参数",
		Params: []parameterSpec{
			{Name: "synchronous_commit", Label: "同步提交"},
			{Name: "synchronous_standby_names", Label: "同步备机配置"},
			{Name: "max_wal_senders", Label: "WAL Sender 数量"},
			{Name: "max_replication_slots", Label: "复制槽数量"},
			{Name: "wal_keep_segments", Label: "WAL 保留段数"},
			{Name: "replication_type", Label: "复制模式"},
		},
	},
	{
		Key:   "security_audit",
		Title: "安全与审计参数",
		Params: []parameterSpec{
			{Name: "ssl", Label: "SSL 开关"},
			{Name: "audit_enabled", Label: "审计开关"},
			{Name: "audit_login_logout", Label: "登录登出审计"},
			{Name: "audit_resource_policy", Label: "资源策略审计"},
			{Name: "auth_iteration_count", Label: "密码迭代次数"},
			{Name: "password_encryption_type", Label: "密码加密类型"},
		},
	},
	{
		Key:   "compatibility_workload",
		Title: "兼容性与负载参数",
		Params: []parameterSpec{
			{Name: "sql_compatibility", Label: "SQL 兼容模式"},
			{Name: "behavior_compat_options", Label: "兼容行为选项"},
			{Name: "use_workload_manager", Label: "负载管理"},
			{Name: "track_activities", Label: "活动跟踪"},
			{Name: "track_counts", Label: "统计跟踪"},
			{Name: "thread_pool_attr", Label: "线程池配置"},
		},
	},
}

func parseItemDetails(item string, parsed parsedOutput) map[string]any {
	switch item {
	case "CheckGUCConsistent":
		return parseGUCConsistencyDetails(parsed.Summary)
	case "CheckGUCValue":
		return parseGUCValueDetails(parsed.Summary)
	case "CheckClusterState":
		return parseClusterStateDetails(parsed.Summary)
	case "CheckIntegrity":
		return parseIntegrityDetails(parsed)
	case "CheckOMMonitor":
		return parseOMMonitorDetails(parsed)
	case "CheckSysTable":
		return parseSysTableDetails(parsed)
	case "CheckErrorInLog":
		return parseErrorInLogDetails(parsed)
	case "CheckKeyDBTableSize":
		return parseKeyDBTableSizeDetails(parsed)
	default:
		return map[string]any{}
	}
}

func summarizeItem(item string, parsed parsedOutput, details map[string]any) string {
	if len(details) == 0 {
		return parsed.Summary
	}
	switch item {
	case "CheckGUCConsistent":
		groupCount, _ := details["key_parameter_group_count"].(int)
		diffCount, _ := details["key_inconsistent_parameter_count"].(int)
		if diffCount == 0 {
			return "关键参数一致性正常"
		}
		return "已分析 " + formatInt(groupCount) + " 类关键参数，发现 " + formatInt(diffCount) + " 个关键参数存在差异"
	case "CheckGUCValue":
		if computed, ok := details["computed_value"]; ok {
			return "锁资源预算值 " + formatAny(computed) + "，" + parsed.Summary
		}
	case "CheckIntegrity":
		if checksum := strings.TrimSpace(asString(details["sha256"])); checksum != "" {
			return "数据一致性检查正常，SHA256=" + checksum
		}
	case "CheckOMMonitor":
		if pid := strings.TrimSpace(asString(details["pid"])); pid != "" {
			return "om_monitor 进程正常，PID=" + pid
		}
	case "CheckSysTable":
		if count, ok := details["table_count"].(int); ok {
			return "已检查 " + formatInt(count) + " 张系统表"
		}
	case "CheckErrorInLog":
		if count, ok := details["error_count"].(int); ok {
			return "最近运行日志 ERROR 数量 " + formatInt(count)
		}
	case "CheckKeyDBTableSize":
		if dbCount, ok := details["database_count"].(int); ok {
			return "已分析 " + formatInt(dbCount) + " 个数据库的大表分布"
		}
	}
	return parsed.Summary
}

func formatInt(value int) string {
	return strconv.Itoa(value)
}

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
		return strings.TrimSpace(asString(value))
	}
}

func parseGUCConsistencyDetails(summary string) map[string]any {
	payloadText := firstJSONObject(summary)
	if payloadText == "" {
		return map[string]any{}
	}
	instances := map[string]map[string]string{}
	if err := json.Unmarshal([]byte(payloadText), &instances); err != nil {
		return map[string]any{}
	}
	if len(instances) == 0 {
		return map[string]any{}
	}
	names := sortedInstanceNames(instances)
	groupPayloads := make([]map[string]any, 0, len(keyParameterGroups))
	differences := make([]map[string]any, 0)
	for _, group := range keyParameterGroups {
		parameters := buildGroupedParameters(instances, group.Params)
		if len(parameters) == 0 {
			continue
		}
		groupPayloads = append(groupPayloads, map[string]any{
			"group_key":  group.Key,
			"title":      group.Title,
			"parameters": parameters,
		})
		differences = append(differences, collectInconsistencies(parameters)...)
	}
	return map[string]any{
		"instance_names":                   names,
		"instance_count":                   len(instances),
		"parameter_count":                  len(instances[names[0]]),
		"key_parameter_group_count":        len(groupPayloads),
		"key_groups":                       groupPayloads,
		"key_inconsistencies":              differences,
		"key_inconsistent_parameter_count": len(differences),
	}
}

func buildGroupedParameters(instances map[string]map[string]string, specs []parameterSpec) []map[string]any {
	names := sortedInstanceNames(instances)
	parameters := make([]map[string]any, 0, len(specs))
	for _, spec := range specs {
		values := make([]map[string]any, 0, len(names))
		unique := map[string]struct{}{}
		representative := ""
		for _, instance := range names {
			value := strings.TrimSpace(instances[instance][spec.Name])
			if value == "" {
				continue
			}
			if representative == "" {
				representative = value
			}
			unique[value] = struct{}{}
			values = append(values, map[string]any{
				"instance": instance,
				"value":    value,
			})
		}
		if len(values) == 0 {
			continue
		}
		parameters = append(parameters, map[string]any{
			"parameter":            spec.Name,
			"label":                spec.Label,
			"representative_value": representative,
			"consistent":           len(unique) == 1,
			"distinct_value_count": len(unique),
			"instance_values":      values,
		})
	}
	return parameters
}

func collectInconsistencies(parameters []map[string]any) []map[string]any {
	items := make([]map[string]any, 0)
	for _, parameter := range parameters {
		consistent, _ := parameter["consistent"].(bool)
		if consistent {
			continue
		}
		items = append(items, map[string]any{
			"parameter":            parameter["parameter"],
			"label":                parameter["label"],
			"instance_values":      parameter["instance_values"],
			"distinct_value_count": parameter["distinct_value_count"],
		})
	}
	return items
}

func firstJSONObject(summary string) string {
	start := strings.Index(summary, "{")
	if start < 0 {
		return ""
	}
	depth := 0
	inString := false
	escaped := false
	for index := start; index < len(summary); index++ {
		char := summary[index]
		if inString {
			if escaped {
				escaped = false
				continue
			}
			if char == '\\' {
				escaped = true
				continue
			}
			if char == '"' {
				inString = false
			}
			continue
		}
		switch char {
		case '"':
			inString = true
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				return summary[start : index+1]
			}
		}
	}
	return ""
}

func sortedInstanceNames(instances map[string]map[string]string) []string {
	names := make([]string, 0, len(instances))
	for name := range instances {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}
