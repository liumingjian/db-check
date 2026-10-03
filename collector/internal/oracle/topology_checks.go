package oracle

import (
	"context"
	"dbcheck/collector/internal/osinfo"
	"fmt"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"time"
)

func (c *metricsCollector) topologyEnabled(topology map[string]any, facet string, expected any, scopes ...string) bool {
	if topology[facet] == expected {
		return true
	}
	if topology[facet] == nil || topology[facet] == "unknown" {
		for _, scope := range scopes {
			c.markUnavailable(scope, "部署形态未确定", "请 DBA 修复部署形态探测视图的只读权限后重新采集。")
		}
	}
	return false
}

var lagInterval = regexp.MustCompile(`^\+?(\d+)\s+(\d{2}):(\d{2}):(\d{2})(?:\.\d+)?$`)

func lagSeconds(value string) (float64, bool) {
	parts := lagInterval.FindStringSubmatch(strings.TrimSpace(value))
	if parts == nil {
		return 0, false
	}
	numbers := make([]int, 4)
	for i := range numbers {
		numbers[i], _ = strconv.Atoi(parts[i+1])
	}
	if numbers[1] > 23 || numbers[2] > 59 || numbers[3] > 59 {
		return 0, false
	}
	return float64(numbers[0])*86400 + float64(numbers[1]*3600+numbers[2]*60+numbers[3]), true
}

func (c *metricsCollector) collectDataGuard(ctx context.Context, topology map[string]any) map[string]any {
	payload := map[string]any{}
	scopes := []string{"oracle.data_guard.transport_lag_seconds", "oracle.data_guard.apply_lag_seconds", "oracle.data_guard.archive_gap", "oracle.data_guard.protection_mode", "oracle.data_guard.destination_errors"}
	if !c.topologyEnabled(topology, "role", "standby", scopes...) {
		return payload
	}
	rows := c.queryRows(ctx, "oracle.data_guard.lag", `SELECT name AS "name", value AS "value", unit AS "unit", time_computed AS "time_computed", datum_time AS "datum_time" FROM v$dataguard_stats WHERE name IN ('transport lag','apply lag')`)
	payload["lag"] = rowsPayload(rows)
	for _, name := range []string{"transport", "apply"} {
		scope := "oracle.data_guard." + name + "_lag_seconds"
		valid := false
		for _, row := range rows {
			if row["name"] == name+" lag" {
				seconds, ok := lagSeconds(fmt.Sprint(row["value"]))
				if ok {
					payload[name+"_lag_seconds"] = seconds
					valid = true
				}
			}
		}
		if !valid {
			if evidence, ok := c.availability["db.data_guard.lag"].(map[string]any); ok && evidence["readable"] == false {
				c.availability[datasetPath(scope)] = evidence
			} else {
				c.markUnavailable(scope, "延迟数据缺失或无法解析", "请 DBA 在实际应用日志的 standby 实例查询 V$DATAGUARD_STATS，检查日志传输与应用进程后重新采集。")
			}
		}
	}
	payload["archive_gap"] = rowsPayload(c.queryRows(ctx, "oracle.data_guard.archive_gap", `SELECT thread# AS "thread", low_sequence# AS "low_sequence", high_sequence# AS "high_sequence" FROM v$archive_gap`))
	payload["protection_mode"] = c.queryString(ctx, "oracle.data_guard.protection_mode", `SELECT protection_mode FROM v$database`)
	if payload["protection_mode"] == "" {
		if evidence, ok := c.availability["db.data_guard.protection_mode"].(map[string]any); !ok || evidence["readable"] != false {
			c.markUnavailable("oracle.data_guard.protection_mode", "保护模式未返回", "请 DBA 查询 V$DATABASE 的 PROTECTION_MODE 并检查视图访问权限。")
		}
	}
	payload["destination_errors"] = rowsPayload(c.queryRows(ctx, "oracle.data_guard.destination_errors", archiveDestinationErrorsQuery))
	return payload
}

func (c *metricsCollector) collectASM(ctx context.Context, topology map[string]any) map[string]any {
	if !c.topologyEnabled(topology, "is_asm", true, "oracle.asm.diskgroups") {
		return map[string]any{}
	}
	rows := c.queryRows(ctx, "oracle.asm.diskgroups", `SELECT name AS "name", state AS "state", CASE WHEN state IN ('MOUNTED','CONNECTED') THEN 0 ELSE 1 END AS "unhealthy", type AS "redundancy", total_mb AS "total_mb", free_mb AS "free_mb", usable_file_mb AS "usable_file_mb", required_mirror_free_mb AS "required_mirror_free_mb", offline_disks AS "offline_disks", (total_mb-free_mb)/NULLIF(total_mb,0)*100 AS "used_pct" FROM v$asm_diskgroup_stat ORDER BY name`)
	if len(rows) == 0 {
		if evidence, ok := c.availability["db.asm.diskgroups"].(map[string]any); !ok || evidence["readable"] != false {
			c.markUnavailable("oracle.asm.diskgroups", "ASM 磁盘组信息为空", "请 DBA 确认数据库实例能访问 V$ASM_DISKGROUP_STAT，并在 ASM 实例检查磁盘组。")
		}
	}
	for _, row := range rows {
		if row["used_pct"] == nil {
			c.markUnavailable("oracle.asm.diskgroups.items[*].used_pct", "磁盘组总容量为零，无法计算使用率", "请 DBA 在 ASM 实例核对磁盘组挂载状态与 TOTAL_MB，修复后重新采集。")
		}
	}
	return map[string]any{"diskgroups": rowsPayload(rows)}
}

func (c *metricsCollector) collectRAC(ctx context.Context, topology map[string]any) map[string]any {
	if !c.topologyEnabled(topology, "is_rac", true, "oracle.rac.instances", "oracle.rac.parameters") {
		return map[string]any{}
	}
	return map[string]any{
		"instances":  rowsPayload(c.queryRows(ctx, "oracle.rac.instances", `SELECT inst_id AS "inst_id", instance_name AS "instance_name", host_name AS "host_name", status AS "status", database_status AS "database_status", active_state AS "active_state", CASE WHEN status='OPEN' AND database_status='ACTIVE' AND active_state='NORMAL' THEN 0 ELSE 1 END AS "unhealthy" FROM gv$instance ORDER BY inst_id`)),
		"parameters": rowsPayload(c.queryRows(ctx, "oracle.rac.parameters", `SELECT inst_id AS "inst_id", name AS "name", value AS "value", isdefault AS "isdefault" FROM gv$parameter ORDER BY inst_id, name`)),
	}
}

func (c *metricsCollector) collectHostChecks(ctx context.Context, topology map[string]any) map[string]any {
	payload := map[string]any{}
	remediation := "请客户 DBA 以 Oracle/Grid 软件属主运行 lsnrctl status；RAC 还需运行 crsctl check crs 和 crsctl status resource -t，检查离线节点和资源。提供 --os-host SSH 参数或在数据库主机使用 --local 后重新采集。"
	commands := map[string]string{"listener": "lsnrctl status"}
	if c.topologyEnabled(topology, "is_rac", true, "oracle.host_checks.clusterware") {
		commands["clusterware"] = "crsctl check crs && crsctl status resource -t"
	}
	var runner osinfo.CommandRunner
	if c.cfg.UseRemoteOS {
		var err error
		runner, err = osinfo.NewSSHCommandRunner(c.cfg)
		if err != nil {
			for name := range commands {
				c.markUnavailable("oracle.host_checks."+name, err.Error(), remediation)
			}
			return payload
		}
		defer runner.Close()
	}
	for name, command := range commands {
		var output string
		var err error
		if runner != nil {
			output, err = runner.Run(command)
		} else if c.cfg.Local {
			commandCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			bytes, runErr := exec.CommandContext(commandCtx, "sh", "-c", command).CombinedOutput()
			cancel()
			output, err = string(bytes), runErr
		} else {
			c.markUnavailable("oracle.host_checks."+name, "未提供数据库主机访问通道", remediation)
			continue
		}
		if err != nil || strings.TrimSpace(output) == "" {
			c.markUnavailable("oracle.host_checks."+name, fmt.Sprintf("主机命令未成功完成：%v", err), remediation)
			continue
		}
		upper := strings.ToUpper(output)
		payload[name] = map[string]any{"output": output, "unhealthy": strings.Contains(upper, "OFFLINE") || strings.Contains(upper, "TNS-") || strings.Contains(upper, "CRS-4535")}
	}
	return payload
}
