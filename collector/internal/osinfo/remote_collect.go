package osinfo

import (
	"context"
	"dbcheck/collector/internal/cli"
	"fmt"
	"strconv"
	"strings"
	"time"
)

type remoteProbe struct {
	runner remoteRunner
	snap   *snapshot
}

func (c Collector) collectRemote(_ context.Context, cfg cli.Config) (map[string]any, error) {
	runnerFactory := c.NewRemoteRunner
	if runnerFactory == nil {
		runnerFactory = newSSHRunner
	}
	runner, err := runnerFactory(cfg)
	if err != nil {
		return nil, err
	}
	defer runner.Close()
	return collectRemoteLinux(runner)
}

func collectRemoteLinux(runner remoteRunner) (map[string]any, error) {
	snap := &snapshot{timestamp: time.Now().Format(time.RFC3339), errors: []string{}}
	probe := remoteProbe{runner: runner, snap: snap}
	hostname, err := probe.required("system_info.hostname", remoteHostnameCommand)
	if err != nil {
		return nil, err
	}
	kernel, err := probe.required("system_info.os", remoteKernelCommand)
	if err != nil {
		return nil, err
	}
	if strings.ToLower(strings.TrimSpace(kernel)) != "linux" {
		return nil, fmt.Errorf("remote OS collection only supports Linux over SSH")
	}
	systemInfo := collectRemoteSystemInfo(probe, hostname)
	payload := map[string]any{
		"system_info": systemInfo,
		"cpu":         map[string]any{"samples": []map[string]any{collectRemoteCPUSample(probe)}},
		"memory":      map[string]any{"samples": []map[string]any{collectRemoteMemorySample(probe)}},
		"filesystem":  map[string]any{"samples": []map[string]any{collectRemoteFilesystemSample(probe)}},
		"disk_io":     map[string]any{"samples": []map[string]any{collectRemoteDiskIOSample(probe)}},
		"network":     map[string]any{"samples": []map[string]any{collectRemoteNetworkSample(probe)}},
		"process":     map[string]any{"samples": []map[string]any{collectRemoteProcessSample(probe)}},
	}
	if len(snap.errors) > 0 {
		payload["collect_errors"] = snap.errors
	}
	return payload, nil
}

func (p remoteProbe) required(scope string, command string) (string, error) {
	output, err := p.runner.Run(command)
	if err != nil {
		return "", fmt.Errorf("%s failed: %w", scope, err)
	}
	return strings.TrimSpace(output), nil
}

func (p remoteProbe) optional(scope string, command string) string {
	output, err := p.runner.Run(command)
	if err != nil {
		p.snap.addErr(scope, err)
		return ""
	}
	return strings.TrimSpace(output)
}

func collectRemoteSystemInfo(p remoteProbe, hostname string) map[string]any {
	arch := normalizeRemoteArch(p.optional("system_info.arch", remoteArchCommand))
	cpuCores, err := parseInt(p.optional("system_info.cpu_cores", remoteCPUCoreCommand))
	if err != nil {
		p.snap.addErr("system_info.cpu_cores", err)
	}
	fdUsage, err := parseFloat(p.optional("system_info.file_descriptor_usage_percent", remoteFDUsageCommand))
	if err != nil {
		p.snap.addErr("system_info.file_descriptor_usage_percent", err)
	}
	mysqlFDUsage, err := parseFloat(p.optional("system_info.mysql_fd_usage_percent", remoteMySQLFDUsageCommand))
	if err != nil {
		p.snap.addErr("system_info.mysql_fd_usage_percent", err)
	}
	return map[string]any{
		"hostname":                      hostname,
		"os":                            "linux",
		"arch":                          arch,
		"cpu_cores":                     cpuCores,
		"file_descriptor_usage_percent": fdUsage,
		"mysql_fd_usage_percent":        mysqlFDUsage,
		"oom_killer_detected":           false,
		"numa_imbalance_percent":        0.0,
		"transparent_hugepages":         parseTHPState(p.optional("system_info.transparent_hugepages", remoteTHPCommand)),
		"ntp_offset_seconds":            0.0,
	}
}

func collectRemoteCPUSample(p remoteProbe) map[string]any {
	usage, iowait, err := parseCPUStats(p.optional("cpu.sample", remoteCPUCommand))
	if err != nil {
		p.snap.addErr("cpu.sample", err)
	}
	return map[string]any{"timestamp": p.snap.timestamp, "usage_percent": usage, "iowait_percent": iowait}
}

func collectRemoteMemorySample(p remoteProbe) map[string]any {
	memInfo := parseMemInfo(p.optional("memory.meminfo", remoteMemInfoCommand))
	total := memInfo["MemTotal"]
	available := memInfo["MemAvailable"]
	if available == 0 && total >= memInfo["MemFree"]+memInfo["Buffers"]+memInfo["Cached"] {
		available = memInfo["MemFree"] + memInfo["Buffers"] + memInfo["Cached"]
	}
	used := uint64(0)
	if total > available {
		used = total - available
	}
	usagePercent := 0.0
	if total > 0 {
		usagePercent, _ = strconv.ParseFloat(fmt.Sprintf("%.6f", (float64(used)/float64(total))*100), 64)
	}
	swapUsed := uint64(0)
	if memInfo["SwapTotal"] > memInfo["SwapFree"] {
		swapUsed = memInfo["SwapTotal"] - memInfo["SwapFree"]
	}
	return map[string]any{
		"timestamp":       p.snap.timestamp,
		"total_bytes":     total,
		"used_bytes":      used,
		"available_bytes": available,
		"usage_percent":   usagePercent,
		"swap_used_bytes": swapUsed,
		"swap_in_per_sec": 0.0,
	}
}

func collectRemoteFilesystemSample(p remoteProbe) map[string]any {
	return map[string]any{"timestamp": p.snap.timestamp, "mountpoints": parseFilesystem(p.optional("filesystem.df", remoteFilesystemCommand))}
}

func collectRemoteDiskIOSample(p remoteProbe) map[string]any {
	uptime := parseUptime(p.optional("disk_io.uptime", remoteUptimeCommand))
	return map[string]any{"timestamp": p.snap.timestamp, "devices": parseDiskStats(p.optional("disk_io.diskstats", remoteDiskstatsCommand), uptime)}
}

func collectRemoteNetworkSample(p remoteProbe) map[string]any {
	return map[string]any{"timestamp": p.snap.timestamp, "interfaces": parseNetwork(p.optional("network.netdev", remoteNetDevCommand))}
}

func collectRemoteProcessSample(p remoteProbe) map[string]any {
	fields := strings.Fields(p.optional("process.loadavg", remoteLoadAvgCommand))
	loadAvg := 0.0
	if len(fields) > 0 {
		parsed, err := strconv.ParseFloat(fields[0], 64)
		if err != nil {
			p.snap.addErr("process.loadavg", err)
		} else {
			loadAvg = parsed
		}
	}
	return map[string]any{"timestamp": p.snap.timestamp, "load_avg_1": loadAvg, "go_routines": 0}
}

func parseTHPState(raw string) string {
	for _, token := range strings.Fields(raw) {
		if strings.HasPrefix(token, "[") && strings.HasSuffix(token, "]") {
			return strings.Trim(token, "[]")
		}
	}
	return strings.TrimSpace(raw)
}
