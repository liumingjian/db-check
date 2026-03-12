package osinfo

import (
	"time"

	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/disk"
	gnet "github.com/shirou/gopsutil/v3/net"
)

const sampleWindow = time.Second

type timedSamples struct {
	cpu     map[string]any
	diskIO  map[string]any
	network map[string]any
	process map[string]any
}

type performanceSnapshot struct {
	at   time.Time
	cpu  cpu.TimesStat
	disk map[string]disk.IOCountersStat
	net  []gnet.IOCountersStat
}

func collectTimedSamples(s *snapshot) timedSamples {
	start := capturePerformanceSnapshot(s)
	time.Sleep(sampleWindow)
	end := capturePerformanceSnapshot(s)
	windowSeconds := end.at.Sub(start.at).Seconds()
	if windowSeconds <= 0 {
		windowSeconds = sampleWindow.Seconds()
	}
	return timedSamples{
		cpu:     collectCPUSample(s.timestamp, start.cpu, end.cpu),
		diskIO:  collectDiskIOSample(s.timestamp, start.disk, end.disk, windowSeconds),
		network: collectNetworkSample(s.timestamp, start.net, end.net, windowSeconds),
		process: collectProcessSample(s),
	}
}

func capturePerformanceSnapshot(s *snapshot) performanceSnapshot {
	return performanceSnapshot{
		at:   time.Now(),
		cpu:  readCPUTimes(s),
		disk: readDiskCounters(s),
		net:  readNetworkCounters(s),
	}
}

func readCPUTimes(s *snapshot) cpu.TimesStat {
	stats, err := cpu.Times(false)
	if err != nil {
		s.addErr("cpu.times", err)
		return cpu.TimesStat{}
	}
	if len(stats) == 0 {
		return cpu.TimesStat{}
	}
	return stats[0]
}

func readDiskCounters(s *snapshot) map[string]disk.IOCountersStat {
	stats, err := disk.IOCounters()
	if err != nil {
		s.addErr("disk_io.counters", err)
		return map[string]disk.IOCountersStat{}
	}
	return stats
}

func readNetworkCounters(s *snapshot) []gnet.IOCountersStat {
	stats, err := safeNetworkCounters()
	if err != nil {
		s.addErr("network.counters", err)
		return []gnet.IOCountersStat{}
	}
	return stats
}

func collectDiskIOSample(timestamp string, start map[string]disk.IOCountersStat, end map[string]disk.IOCountersStat, windowSeconds float64) map[string]any {
	devices := make([]map[string]any, 0, len(end))
	totalOps := 0.0
	totalBytes := 0.0
	totalLatencyMS := 0.0
	latencySamples := 0.0
	for name, current := range end {
		previous, ok := start[name]
		if !ok {
			continue
		}
		device, ops, bytes, latency := diskDeviceSample(current, previous, windowSeconds)
		if device == nil {
			continue
		}
		devices = append(devices, device)
		totalOps += ops
		totalBytes += bytes
		if latency > 0 {
			totalLatencyMS += latency
			latencySamples++
		}
	}
	return map[string]any{
		"timestamp":             timestamp,
		"devices":               devices,
		"total_iops":            totalOps,
		"total_throughput_kbps": totalBytes / 1024,
		"avg_latency_ms":        average(totalLatencyMS, latencySamples),
	}
}

func diskDeviceSample(current disk.IOCountersStat, previous disk.IOCountersStat, windowSeconds float64) (map[string]any, float64, float64, float64) {
	readCount := diffUint64(current.ReadCount, previous.ReadCount)
	writeCount := diffUint64(current.WriteCount, previous.WriteCount)
	readBytes := diffUint64(current.ReadBytes, previous.ReadBytes)
	writeBytes := diffUint64(current.WriteBytes, previous.WriteBytes)
	readTime := diffUint64(current.ReadTime, previous.ReadTime)
	writeTime := diffUint64(current.WriteTime, previous.WriteTime)
	ioTime := diffUint64(current.IoTime, previous.IoTime)
	ops := perSecond(readCount+writeCount, windowSeconds)
	bytes := perSecond(readBytes+writeBytes, windowSeconds)
	latency := 0.0
	if readCount+writeCount > 0 {
		latency = float64(readTime+writeTime) / float64(readCount+writeCount)
	}
	return map[string]any{
		"name":            current.Name,
		"read_count":      readCount,
		"write_count":     writeCount,
		"read_iops":       perSecond(readCount, windowSeconds),
		"write_iops":      perSecond(writeCount, windowSeconds),
		"throughput_kbps": bytes / 1024,
		"avg_latency_ms":  latency,
		"io_util_percent": clampPercent(float64(ioTime) / (windowSeconds * 10)),
	}, ops, bytes, latency
}

func collectNetworkSample(timestamp string, start []gnet.IOCountersStat, end []gnet.IOCountersStat, windowSeconds float64) map[string]any {
	previous := mapNetworkCounters(start)
	interfaces := make([]map[string]any, 0, len(end))
	totalRX := 0.0
	totalTX := 0.0
	totalErrDrop := 0.0
	for _, current := range end {
		if current.Name == "" {
			continue
		}
		item, rx, tx, errDrop := networkInterfaceSample(current, previous[current.Name], windowSeconds)
		interfaces = append(interfaces, item)
		totalRX += rx
		totalTX += tx
		totalErrDrop += errDrop
	}
	return map[string]any{
		"timestamp":                timestamp,
		"interfaces":               interfaces,
		"total_rx_bytes_per_sec":   totalRX,
		"total_tx_bytes_per_sec":   totalTX,
		"total_rate_bytes_per_sec": totalRX + totalTX,
		"error_drop_per_sec":       totalErrDrop,
	}
}

func mapNetworkCounters(stats []gnet.IOCountersStat) map[string]gnet.IOCountersStat {
	items := make(map[string]gnet.IOCountersStat, len(stats))
	for _, item := range stats {
		items[item.Name] = item
	}
	return items
}

func networkInterfaceSample(current gnet.IOCountersStat, previous gnet.IOCountersStat, windowSeconds float64) (map[string]any, float64, float64, float64) {
	rxBytes := perSecond(diffUint64(current.BytesRecv, previous.BytesRecv), windowSeconds)
	txBytes := perSecond(diffUint64(current.BytesSent, previous.BytesSent), windowSeconds)
	errDrop := perSecond(
		diffUint64(current.Errin, previous.Errin)+diffUint64(current.Errout, previous.Errout)+diffUint64(current.Dropin, previous.Dropin)+diffUint64(current.Dropout, previous.Dropout),
		windowSeconds,
	)
	return map[string]any{
		"name":               current.Name,
		"bytes_recv":         current.BytesRecv,
		"bytes_sent":         current.BytesSent,
		"errin":              current.Errin,
		"errout":             current.Errout,
		"dropin":             current.Dropin,
		"dropout":            current.Dropout,
		"rx_bytes_per_sec":   rxBytes,
		"tx_bytes_per_sec":   txBytes,
		"error_drop_per_sec": errDrop,
	}, rxBytes, txBytes, errDrop
}

func diffUint64(current uint64, previous uint64) uint64 {
	if current < previous {
		return 0
	}
	return current - previous
}

func perSecond(value uint64, windowSeconds float64) float64 {
	if windowSeconds <= 0 {
		return 0
	}
	return float64(value) / windowSeconds
}

func average(total float64, count float64) float64 {
	if count <= 0 {
		return 0
	}
	return total / count
}
