package osinfo

import (
	"os"
	"runtime"
	"strconv"
	"strings"

	"github.com/shirou/gopsutil/v3/process"
)

type fdLimit struct {
	Cur uint64
}

func collectSystemInfo(s *snapshot, hostname string) map[string]any {
	fdUsage, fdErr := processFDUsage()
	if fdErr != nil {
		s.addErr("system_info.file_descriptor_usage_percent", fdErr)
	}
	mysqlFDUsage, mysqlErr := mysqlFDUsage()
	if mysqlErr != nil {
		s.addErr("system_info.mysql_fd_usage_percent", mysqlErr)
	}
	return map[string]any{
		"hostname":                      hostname,
		"os":                            runtime.GOOS,
		"arch":                          runtime.GOARCH,
		"cpu_cores":                     runtime.NumCPU(),
		"file_descriptor_usage_percent": fdUsage,
		"mysql_fd_usage_percent":        mysqlFDUsage,
		"oom_killer_detected":           false,
		"numa_imbalance_percent":        0.0,
		"transparent_hugepages":         readTHPState(),
		"ntp_offset_seconds":            0.0,
	}
}

func processFDUsage() (float64, error) {
	proc, err := process.NewProcess(int32(os.Getpid()))
	if err != nil {
		return 0, err
	}
	fdCount, fdErr := proc.NumFDs()
	if fdErr != nil {
		return 0, fdErr
	}
	limit, limitErr := processFDLimit()
	if limitErr != nil {
		return 0, limitErr
	}
	if limit.Cur == 0 {
		return 0, nil
	}
	return (float64(fdCount) / float64(limit.Cur)) * 100, nil
}

func mysqlFDUsage() (float64, error) {
	procs, err := process.Processes()
	if err != nil {
		return 0, err
	}
	maxUsage := 0.0
	for _, proc := range procs {
		name, nameErr := proc.Name()
		if nameErr != nil || !strings.Contains(strings.ToLower(name), "mysql") {
			continue
		}
		fds, fdErr := proc.NumFDs()
		if fdErr != nil {
			continue
		}
		limits, limitErr := proc.RlimitUsage(true)
		if limitErr != nil {
			continue
		}
		usage := mysqlFDUsageFromLimits(fds, limits)
		if usage > maxUsage {
			maxUsage = usage
		}
	}
	return maxUsage, nil
}

func mysqlFDUsageFromLimits(fds int32, limits []process.RlimitStat) float64 {
	for _, item := range limits {
		if item.Resource != process.RLIMIT_NOFILE || item.Soft <= 0 {
			continue
		}
		return (float64(fds) / float64(item.Soft)) * 100
	}
	return 0
}

func readTHPState() string {
	if runtime.GOOS != "linux" {
		return "unsupported"
	}
	content, err := os.ReadFile("/sys/kernel/mm/transparent_hugepage/enabled")
	if err != nil {
		return "unknown"
	}
	parts := strings.Fields(string(content))
	for _, token := range parts {
		if strings.HasPrefix(token, "[") && strings.HasSuffix(token, "]") {
			return strings.Trim(token, "[]")
		}
	}
	if len(parts) > 0 {
		return strings.TrimSpace(parts[0])
	}
	return strconv.FormatBool(false)
}
