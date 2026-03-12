package osinfo

import (
	"runtime"

	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/load"
	"github.com/shirou/gopsutil/v3/mem"
)

func collectCPUSample(timestamp string, start cpu.TimesStat, end cpu.TimesStat) map[string]any {
	total := cpuTimesTotal(end) - cpuTimesTotal(start)
	if total <= 0 {
		return map[string]any{"timestamp": timestamp}
	}
	userDiff := end.User - start.User
	systemDiff := end.System - start.System
	idleDiff := end.Idle - start.Idle
	niceDiff := end.Nice - start.Nice
	iowaitDiff := end.Iowait - start.Iowait
	return map[string]any{
		"timestamp":      timestamp,
		"usage_percent":  diffPercent(total-idleDiff, total),
		"user_percent":   diffPercent(userDiff, total),
		"system_percent": diffPercent(systemDiff, total),
		"idle_percent":   diffPercent(idleDiff, total),
		"nice_percent":   diffPercent(niceDiff, total),
		"iowait_percent": diffPercent(iowaitDiff, total),
	}
}

func cpuTimesTotal(stat cpu.TimesStat) float64 {
	return stat.User + stat.System + stat.Idle + stat.Nice + stat.Iowait + stat.Irq + stat.Softirq + stat.Steal
}

func diffPercent(value float64, total float64) float64 {
	if total <= 0 || value <= 0 {
		return 0
	}
	return (value / total) * 100
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
		"timestamp":            s.timestamp,
		"total_bytes":          virtual.Total,
		"used_bytes":           virtual.Used,
		"available_bytes":      virtual.Available,
		"usage_percent":        virtual.UsedPercent,
		"swap_total_bytes":     swap.Total,
		"swap_used_bytes":      swap.Used,
		"swap_free_bytes":      swap.Free,
		"swap_usage_percent":   swap.UsedPercent,
		"swap_in_per_sec":      0.0,
		"meminfo":              buildMeminfo(virtual, swap),
		"transparent_hugepage": readTHPState(),
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
	misc, miscErr := load.Misc()
	if miscErr != nil {
		s.addErr("process.misc", miscErr)
	}
	return map[string]any{
		"timestamp":         s.timestamp,
		"load_avg_1":        loadAvg,
		"running_processes": miscValue(misc, func(item *load.MiscStat) int { return item.ProcsRunning }),
		"blocked_processes": miscValue(misc, func(item *load.MiscStat) int { return item.ProcsBlocked }),
		"total_processes":   miscValue(misc, func(item *load.MiscStat) int { return item.ProcsTotal }),
		"context_switches":  miscValue(misc, func(item *load.MiscStat) int { return item.Ctxt }),
		"go_routines":       runtime.NumGoroutine(),
	}
}

func miscValue(stat *load.MiscStat, picker func(*load.MiscStat) int) int {
	if stat == nil {
		return 0
	}
	return picker(stat)
}

func buildMeminfo(virtual *mem.VirtualMemoryStat, swap *mem.SwapMemoryStat) map[string]any {
	return map[string]any{
		"MemTotal":       virtual.Total,
		"MemFree":        virtual.Free,
		"MemAvailable":   virtual.Available,
		"Buffers":        virtual.Buffers,
		"Cached":         virtual.Cached,
		"SwapCached":     virtual.SwapCached,
		"Active":         virtual.Active,
		"Inactive":       virtual.Inactive,
		"Dirty":          virtual.Dirty,
		"Writeback":      virtual.WriteBack,
		"Slab":           virtual.Slab,
		"PageTables":     virtual.PageTables,
		"CommitLimit":    virtual.CommitLimit,
		"Committed_AS":   virtual.CommittedAS,
		"HugePagesTotal": virtual.HugePagesTotal,
		"HugePagesFree":  virtual.HugePagesFree,
		"HugePageSize":   virtual.HugePageSize,
		"SwapTotal":      swap.Total,
		"SwapFree":       swap.Free,
	}
}
