package osinfo

import (
	"testing"

	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/disk"
	"github.com/shirou/gopsutil/v3/mem"
	gnet "github.com/shirou/gopsutil/v3/net"
)

func TestCollectCPUSampleIncludesDetailedPercents(t *testing.T) {
	start := cpu.TimesStat{User: 10, System: 20, Idle: 70, Nice: 5, Iowait: 5}
	end := cpu.TimesStat{User: 20, System: 30, Idle: 90, Nice: 10, Iowait: 10}
	sample := collectCPUSample("2026-03-12T10:00:00Z", start, end)
	if sample["usage_percent"].(float64) <= 0 {
		t.Fatalf("expected usage percent > 0: %#v", sample)
	}
	if sample["user_percent"].(float64) <= 0 {
		t.Fatalf("expected user percent > 0: %#v", sample)
	}
	if sample["idle_percent"].(float64) <= 0 {
		t.Fatalf("expected idle percent > 0: %#v", sample)
	}
}

func TestDiskDeviceSampleProducesRates(t *testing.T) {
	device, ops, bytes, latency := diskDeviceSample(
		disk.IOCountersStat{Name: "sda", ReadCount: 20, WriteCount: 40, ReadBytes: 4096, WriteBytes: 8192, ReadTime: 50, WriteTime: 100, IoTime: 200},
		disk.IOCountersStat{Name: "sda", ReadCount: 10, WriteCount: 20, ReadBytes: 1024, WriteBytes: 2048, ReadTime: 20, WriteTime: 40, IoTime: 100},
		2,
	)
	if ops <= 0 || bytes <= 0 || latency <= 0 {
		t.Fatalf("expected positive disk sample metrics: ops=%v bytes=%v latency=%v", ops, bytes, latency)
	}
	if device["throughput_kbps"].(float64) <= 0 {
		t.Fatalf("expected throughput_kbps > 0: %#v", device)
	}
}

func TestNetworkInterfaceSampleProducesRates(t *testing.T) {
	item, rx, tx, errDrop := networkInterfaceSample(
		gnet.IOCountersStat{Name: "eth0", BytesRecv: 3000, BytesSent: 5000, Errin: 3, Errout: 2, Dropin: 1, Dropout: 1},
		gnet.IOCountersStat{Name: "eth0", BytesRecv: 1000, BytesSent: 2000, Errin: 1, Errout: 1, Dropin: 0, Dropout: 0},
		2,
	)
	if rx <= 0 || tx <= 0 || errDrop <= 0 {
		t.Fatalf("expected positive network sample metrics: rx=%v tx=%v errDrop=%v", rx, tx, errDrop)
	}
	if item["rx_bytes_per_sec"].(float64) <= 0 {
		t.Fatalf("expected rx_bytes_per_sec > 0: %#v", item)
	}
}

func TestBuildMeminfoIncludesKeyFields(t *testing.T) {
	meminfo := buildMeminfo(
		&mem.VirtualMemoryStat{Total: 1, Free: 1, Available: 1, Buffers: 1, Cached: 1, SwapCached: 1, Active: 1, Inactive: 1, Dirty: 1, WriteBack: 1, Slab: 1, PageTables: 1, CommitLimit: 1, CommittedAS: 1, HugePagesTotal: 1, HugePagesFree: 1, HugePageSize: 1},
		&mem.SwapMemoryStat{Total: 2, Free: 1},
	)
	if meminfo["MemTotal"] == nil || meminfo["SwapTotal"] == nil {
		t.Fatalf("expected meminfo to include MemTotal and SwapTotal: %#v", meminfo)
	}
}
