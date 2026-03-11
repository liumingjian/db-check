package osinfo

import (
	"fmt"
	"strings"

	"github.com/shirou/gopsutil/v3/disk"
	"github.com/shirou/gopsutil/v3/host"
	gnet "github.com/shirou/gopsutil/v3/net"
)

func collectFilesystemSample(s *snapshot) map[string]any {
	partitions, err := disk.Partitions(false)
	if err != nil {
		s.addErr("filesystem.partitions", err)
	}
	mountpoints := make([]map[string]any, 0, len(partitions))
	for _, partition := range partitions {
		usage, usageErr := disk.Usage(partition.Mountpoint)
		if usageErr != nil {
			s.addErr("filesystem.usage", usageErr)
			continue
		}
		mountpoints = append(mountpoints, map[string]any{
			"device":        partition.Device,
			"mountpoint":    partition.Mountpoint,
			"fstype":        partition.Fstype,
			"usage_percent": usage.UsedPercent,
			"total_bytes":   usage.Total,
			"used_bytes":    usage.Used,
			"free_bytes":    usage.Free,
		})
	}
	return map[string]any{"timestamp": s.timestamp, "mountpoints": mountpoints}
}

func collectDiskIOSample(s *snapshot) map[string]any {
	stats, err := disk.IOCounters()
	if err != nil {
		s.addErr("disk_io.counters", err)
	}
	uptimeSeconds := hostUptimeSeconds(s)
	devices := make([]map[string]any, 0, len(stats))
	for name, item := range stats {
		avgReadLatency := 0.0
		if item.ReadCount > 0 {
			avgReadLatency = float64(item.ReadTime) / float64(item.ReadCount)
		}
		ioUtil := 0.0
		if uptimeSeconds > 0 {
			ioUtil = float64(item.IoTime) / (uptimeSeconds * 10)
		}
		devices = append(devices, map[string]any{
			"name":                name,
			"read_count":          item.ReadCount,
			"write_count":         item.WriteCount,
			"avg_read_latency_ms": avgReadLatency,
			"io_util_percent":     clampPercent(ioUtil),
		})
	}
	return map[string]any{"timestamp": s.timestamp, "devices": devices}
}

func collectNetworkSample(s *snapshot) map[string]any {
	stats, err := safeNetworkCounters()
	if err != nil {
		s.addErr("network.counters", err)
	}
	interfaces := make([]map[string]any, 0, len(stats))
	for _, item := range stats {
		if strings.TrimSpace(item.Name) == "" {
			continue
		}
		interfaces = append(interfaces, map[string]any{
			"name":       item.Name,
			"errin":      item.Errin,
			"errout":     item.Errout,
			"bytes_recv": item.BytesRecv,
			"bytes_sent": item.BytesSent,
		})
	}
	return map[string]any{"timestamp": s.timestamp, "interfaces": interfaces}
}

func safeNetworkCounters() (_ []gnet.IOCountersStat, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("network counters panic: %v", recovered)
		}
	}()
	return gnet.IOCounters(true)
}

func hostUptimeSeconds(s *snapshot) float64 {
	uptime, err := host.Uptime()
	if err != nil {
		s.addErr("host.uptime", err)
		return 0
	}
	return float64(uptime)
}

func clampPercent(value float64) float64 {
	if value < 0 {
		return 0
	}
	if value > 100 {
		return 100
	}
	return value
}
