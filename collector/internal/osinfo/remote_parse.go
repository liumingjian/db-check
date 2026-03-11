package osinfo

import (
	"fmt"
	"strconv"
	"strings"
)

func parseFloat(raw string) (float64, error) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return 0, fmt.Errorf("empty float")
	}
	return strconv.ParseFloat(value, 64)
}

func parseInt(raw string) (int, error) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return 0, fmt.Errorf("empty int")
	}
	return strconv.Atoi(value)
}

func normalizeRemoteArch(raw string) string {
	switch strings.TrimSpace(raw) {
	case "x86_64":
		return "amd64"
	case "aarch64":
		return "arm64"
	default:
		return strings.TrimSpace(raw)
	}
}

func parseCPUStats(raw string) (float64, float64, error) {
	parts := strings.Split(strings.TrimSpace(raw), "\t")
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("invalid cpu sample")
	}
	usage, err := parseFloat(parts[0])
	if err != nil {
		return 0, 0, err
	}
	iowait, err := parseFloat(parts[1])
	if err != nil {
		return 0, 0, err
	}
	return usage, iowait, nil
}

func parseMemInfo(raw string) map[string]uint64 {
	values := map[string]uint64{}
	for _, line := range strings.Split(raw, "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		key := strings.TrimSuffix(fields[0], ":")
		parsed, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			continue
		}
		values[key] = parsed * 1024
	}
	return values
}

func parseFilesystem(raw string) []map[string]any {
	mountpoints := []map[string]any{}
	for _, line := range strings.Split(strings.TrimSpace(raw), "\n") {
		fields := strings.Split(line, "\t")
		if len(fields) != 6 {
			continue
		}
		total, err1 := strconv.ParseUint(fields[2], 10, 64)
		used, err2 := strconv.ParseUint(fields[3], 10, 64)
		usage, err3 := strconv.ParseFloat(strings.TrimSuffix(fields[4], "%"), 64)
		if err1 != nil || err2 != nil || err3 != nil {
			continue
		}
		free, _ := strconv.ParseUint(fields[3], 10, 64)
		free = 0
		if total > used {
			free = total - used
		}
		mountpoints = append(mountpoints, map[string]any{
			"device":        fields[0],
			"fstype":        fields[1],
			"total_bytes":   total,
			"used_bytes":    used,
			"free_bytes":    free,
			"usage_percent": usage,
			"mountpoint":    fields[5],
		})
	}
	return mountpoints
}

func parseUptime(raw string) float64 {
	fields := strings.Fields(strings.TrimSpace(raw))
	if len(fields) == 0 {
		return 0
	}
	value, err := strconv.ParseFloat(fields[0], 64)
	if err != nil {
		return 0
	}
	return value
}

func parseNetwork(raw string) []map[string]any {
	interfaces := []map[string]any{}
	for _, line := range strings.Split(raw, "\n") {
		if !strings.Contains(line, ":") {
			continue
		}
		parts := strings.SplitN(line, ":", 2)
		name := strings.TrimSpace(parts[0])
		fields := strings.Fields(parts[1])
		if name == "" || len(fields) < 11 {
			continue
		}
		bytesRecv, err1 := strconv.ParseUint(fields[0], 10, 64)
		errIn, err2 := strconv.ParseUint(fields[2], 10, 64)
		bytesSent, err3 := strconv.ParseUint(fields[8], 10, 64)
		errOut, err4 := strconv.ParseUint(fields[10], 10, 64)
		if err1 != nil || err2 != nil || err3 != nil || err4 != nil {
			continue
		}
		interfaces = append(interfaces, map[string]any{
			"name":       name,
			"errin":      errIn,
			"errout":     errOut,
			"bytes_recv": bytesRecv,
			"bytes_sent": bytesSent,
		})
	}
	return interfaces
}

func parseDiskStats(raw string, uptime float64) []map[string]any {
	devices := []map[string]any{}
	for _, line := range strings.Split(strings.TrimSpace(raw), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 14 || !includeBlockDevice(fields[2]) {
			continue
		}
		readCount, err1 := strconv.ParseUint(fields[3], 10, 64)
		writeCount, err2 := strconv.ParseUint(fields[7], 10, 64)
		readTime, err3 := strconv.ParseUint(fields[6], 10, 64)
		ioTime, err4 := strconv.ParseUint(fields[12], 10, 64)
		if err1 != nil || err2 != nil || err3 != nil || err4 != nil {
			continue
		}
		avgReadLatency := 0.0
		if readCount > 0 {
			avgReadLatency = float64(readTime) / float64(readCount)
		}
		ioUtil := 0.0
		if uptime > 0 {
			ioUtil = float64(ioTime) / (uptime * 10)
		}
		devices = append(devices, map[string]any{
			"name":                fields[2],
			"read_count":          readCount,
			"write_count":         writeCount,
			"avg_read_latency_ms": avgReadLatency,
			"io_util_percent":     clampPercent(ioUtil),
		})
	}
	return devices
}

func includeBlockDevice(name string) bool {
	prefixes := []string{"sd", "vd", "xvd", "nvme", "dm-"}
	for _, prefix := range prefixes {
		if strings.HasPrefix(name, prefix) {
			return true
		}
	}
	return false
}
