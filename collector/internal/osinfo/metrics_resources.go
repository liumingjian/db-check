package osinfo

import (
	"runtime"

	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/load"
	"github.com/shirou/gopsutil/v3/mem"
)

func collectCPUSample(s *snapshot) map[string]any {
	usagePercent := 0.0
	usage, err := cpu.Percent(0, false)
	if err != nil {
		s.addErr("cpu.percent", err)
	} else if len(usage) > 0 {
		usagePercent = usage[0]
	}
	iowaitPercent := 0.0
	times, timeErr := cpu.Times(false)
	if timeErr != nil {
		s.addErr("cpu.times", timeErr)
	} else if len(times) > 0 {
		total := cpuTimesTotal(times[0])
		if total > 0 {
			iowaitPercent = (times[0].Iowait / total) * 100
		}
	}
	return map[string]any{
		"timestamp":      s.timestamp,
		"usage_percent":  usagePercent,
		"iowait_percent": iowaitPercent,
	}
}

func cpuTimesTotal(stat cpu.TimesStat) float64 {
	return stat.User + stat.System + stat.Idle + stat.Nice + stat.Iowait + stat.Irq + stat.Softirq + stat.Steal
}

func collectMemorySample(s *snapshot) map[string]any {
	virtual, virtualErr := mem.VirtualMemory()
	if virtualErr != nil {
		s.addErr("memory.virtual", virtualErr)
	}
	swap, swapErr := mem.SwapMemory()
	if swapErr != nil {
		s.addErr("memory.swap", swapErr)
	}
	if virtual == nil {
		virtual = &mem.VirtualMemoryStat{}
	}
	if swap == nil {
		swap = &mem.SwapMemoryStat{}
	}
	return map[string]any{
		"timestamp":       s.timestamp,
		"total_bytes":     virtual.Total,
		"used_bytes":      virtual.Used,
		"available_bytes": virtual.Available,
		"usage_percent":   virtual.UsedPercent,
		"swap_used_bytes": swap.Used,
		"swap_in_per_sec": 0.0,
	}
}

func collectProcessSample(s *snapshot) map[string]any {
	avg, err := load.Avg()
	if err != nil {
		s.addErr("process.load_avg", err)
	}
	loadAvg := 0.0
	if avg != nil {
		loadAvg = avg.Load1
	}
	return map[string]any{
		"timestamp":   s.timestamp,
		"load_avg_1":  loadAvg,
		"go_routines": runtime.NumGoroutine(),
	}
}
