package osinfo

import (
	"fmt"
	"strings"

	"github.com/shirou/gopsutil/v3/disk"
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
			"device":               partition.Device,
			"mountpoint":           partition.Mountpoint,
			"fstype":               partition.Fstype,
			"usage_percent":        usage.UsedPercent,
			"total_bytes":          usage.Total,
			"used_bytes":           usage.Used,
			"free_bytes":           usage.Free,
			"inodes_total":         usage.InodesTotal,
			"inodes_used":          usage.InodesUsed,
			"inodes_free":          usage.InodesFree,
			"inodes_usage_percent": usage.InodesUsedPercent,
			"read_only":            partitionReadOnly(partition),
		})
	}
	return map[string]any{"timestamp": s.timestamp, "mountpoints": mountpoints}
}

func safeNetworkCounters() (_ []gnet.IOCountersStat, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("network counters panic: %v", recovered)
		}
	}()
	return gnet.IOCounters(true)
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

func partitionReadOnly(partition disk.PartitionStat) bool {
	for _, option := range partition.Opts {
		if strings.EqualFold(option, "ro") {
			return true
		}
	}
	return false
}
