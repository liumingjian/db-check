"""Shared OS report sections for MySQL and Oracle reports."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from reporter.content.helpers import (
    first_item,
    format_bytes,
    format_number,
    format_percent,
    full_table,
    key_value_table,
)
from reporter.model.report_view import SectionBlock
MEMINFO_PRIORITY_KEYS = (
    "MemTotal",
    "MemFree",
    "MemAvailable",
    "Buffers",
    "Cached",
    "SwapCached",
    "Active",
    "Inactive",
    "Dirty",
    "Writeback",
    "Slab",
    "PageTables",
    "CommitLimit",
    "Committed_AS",
    "HugePagesTotal",
    "HugePagesFree",
    "HugePageSize",
    "SwapTotal",
    "SwapFree",
)


@dataclass(frozen=True)
class OSSectionOptions:
    section_prefix: str
    db_process_label: str
    include_db_process_fd: bool = True

def build_os_section(result: dict[str, Any], options: OSSectionOptions) -> SectionBlock:
    return SectionBlock(
        title=f"{options.section_prefix} 系统指标",
        tables=(full_table("系统指标", ("指标", "当前值", "说明"), _overview_rows(result, options)),),
        children=(
            _cpu_detail_section(result, options),
            _system_detail_section(result, options),
            _memory_detail_section(result, options),
            _filesystem_detail_section(result, options),
            _disk_io_detail_section(result, options),
            _network_detail_section(result, options),
        ),
    )

def _overview_rows(result: dict[str, Any], options: OSSectionOptions) -> tuple[tuple[str, str, str], ...]:
    cpu = _os_sample(result, "cpu")
    memory = _os_sample(result, "memory")
    process = _os_sample(result, "process")
    disk_io = _os_sample(result, "disk_io")
    network = _os_sample(result, "network")
    root_fs = _root_filesystem(result)
    system = _system_info(result)
    rows = [
        ("CPU 使用率", format_percent(cpu.get("usage_percent")), "采样窗口 CPU 总使用率"),
        ("CPU iowait", format_percent(cpu.get("iowait_percent")), "采样窗口磁盘等待占比"),
        ("内存使用率", format_percent(memory.get("usage_percent")), "采样时点内存占用"),
        ("Swap 使用率", format_percent(memory.get("swap_usage_percent")), "采样时点交换分区占用"),
        ("Load Average(1m)", format_number(process.get("load_avg_1")), "1 分钟系统负载"),
        ("运行队列", format_number(process.get("running_processes"), 0), "当前运行进程数"),
        ("阻塞队列", format_number(process.get("blocked_processes"), 0), "当前阻塞进程数"),
        ("磁盘使用率", format_percent(root_fs.get("usage_percent")), _root_mount_desc(root_fs)),
        ("inode 使用率", format_percent(root_fs.get("inodes_usage_percent")), "根文件系统 inode 占用"),
        ("总 IOPS", format_number(disk_io.get("total_iops")), "采样窗口总 IOPS"),
        ("IO 吞吐", _kbps_text(disk_io.get("total_throughput_kbps")), "采样窗口总吞吐"),
        ("IO 时延", _ms_text(disk_io.get("avg_latency_ms")), "采样窗口平均 I/O 时延"),
        ("网络总速率", _rate_text(network.get("total_rate_bytes_per_sec")), "所有网络接口总吞吐"),
        ("每秒错包/丢包", format_number(network.get("error_drop_per_sec")), "所有网络接口总错误/丢包速率"),
        ("文件描述符使用率", format_percent(system.get("file_descriptor_usage_percent")), "OS 级 fd 使用情况"),
    ]
    if options.include_db_process_fd:
        rows.append(
            (
                options.db_process_label,
                format_percent(system.get("mysql_fd_usage_percent")),
                "数据库进程 fd 使用情况",
            )
        )
    return tuple(rows)

def _cpu_detail_section(result: dict[str, Any], options: OSSectionOptions) -> SectionBlock:
    cpu = _os_sample(result, "cpu")
    process = _os_sample(result, "process")
    rows = (
        ("CPU 使用率", format_percent(cpu.get("usage_percent")), "CPU 总使用率"),
        ("CPU user", format_percent(cpu.get("user_percent")), "用户态 CPU 占比"),
        ("CPU system", format_percent(cpu.get("system_percent")), "内核态 CPU 占比"),
        ("CPU idle", format_percent(cpu.get("idle_percent")), "空闲 CPU 占比"),
        ("CPU iowait", format_percent(cpu.get("iowait_percent")), "磁盘等待占比"),
        ("CPU nice", format_percent(cpu.get("nice_percent")), "nice 时间占比"),
        ("Load Average(1m)", format_number(process.get("load_avg_1")), "1 分钟系统负载"),
        ("运行队列", format_number(process.get("running_processes"), 0), "当前运行进程数"),
        ("阻塞队列", format_number(process.get("blocked_processes"), 0), "当前阻塞进程数"),
    )
    return SectionBlock(
        title=f"{options.section_prefix}.1 CPU与调度",
        tables=(full_table("CPU与调度", ("指标", "当前值", "说明"), rows),),
    )

def _system_detail_section(result: dict[str, Any], options: OSSectionOptions) -> SectionBlock:
    system = _system_info(result)
    process = _os_sample(result, "process")
    rows = [
        ("主机名", str(system.get("hostname", ""))),
        ("操作系统", str(system.get("os", ""))),
        ("架构", str(system.get("arch", ""))),
        ("CPU 核数", format_number(system.get("cpu_cores"), 0)),
        ("总进程数", format_number(process.get("total_processes"), 0)),
        ("上下文切换", format_number(process.get("context_switches"), 0)),
        ("透明大页", str(system.get("transparent_hugepages", ""))),
        ("OOM Killer", _bool_text(system.get("oom_killer_detected"))),
        ("NUMA 失衡", format_percent(system.get("numa_imbalance_percent"))),
        ("NTP 偏移(秒)", format_number(system.get("ntp_offset_seconds"))),
        ("文件描述符使用率", format_percent(system.get("file_descriptor_usage_percent"))),
    ]
    if options.include_db_process_fd:
        rows.append((options.db_process_label, format_percent(system.get("mysql_fd_usage_percent"))))
    return SectionBlock(
        title=f"{options.section_prefix}.2 系统与进程",
        tables=(key_value_table("系统与进程", tuple(rows)),),
    )

def _memory_detail_section(result: dict[str, Any], options: OSSectionOptions) -> SectionBlock:
    memory = _os_sample(result, "memory")
    rows = [
        ("内存使用率", format_percent(memory.get("usage_percent"))),
        ("Swap 使用率", format_percent(memory.get("swap_usage_percent"))),
        ("内存总量", format_bytes(memory.get("total_bytes"))),
        ("内存已用", format_bytes(memory.get("used_bytes"))),
        ("内存可用", format_bytes(memory.get("available_bytes"))),
        ("Swap 总量", format_bytes(memory.get("swap_total_bytes"))),
        ("Swap 已用", format_bytes(memory.get("swap_used_bytes"))),
        ("Swap 剩余", format_bytes(memory.get("swap_free_bytes"))),
    ]
    rows.extend(_meminfo_rows(memory))
    return SectionBlock(
        title=f"{options.section_prefix}.3 内存明细",
        tables=(key_value_table("内存明细", tuple(rows)),),
    )

def _filesystem_detail_section(result: dict[str, Any], options: OSSectionOptions) -> SectionBlock:
    filesystem = _os_sample(result, "filesystem")
    mountpoints = filesystem.get("mountpoints", []) if isinstance(filesystem.get("mountpoints"), list) else []
    rows = tuple(
        (
            str(item.get("mountpoint", "")),
            str(item.get("device", "")),
            str(item.get("fstype", "")),
            format_bytes(item.get("total_bytes")),
            format_bytes(item.get("used_bytes")),
            format_bytes(item.get("free_bytes")),
            format_percent(item.get("usage_percent")),
            format_percent(item.get("inodes_usage_percent")),
            "是" if item.get("read_only") else "否",
        )
        for item in mountpoints
        if isinstance(item, dict)
    )
    return SectionBlock(
        title=f"{options.section_prefix}.4 文件系统明细",
        tables=(full_table("文件系统明细", ("挂载点", "设备", "文件系统", "总容量", "已用", "可用", "使用率", "inode使用率", "只读"), rows or (("待补充", "", "", "", "", "", "", "", ""),)),),
    )

def _disk_io_detail_section(result: dict[str, Any], options: OSSectionOptions) -> SectionBlock:
    disk_io = _os_sample(result, "disk_io")
    devices = disk_io.get("devices", []) if isinstance(disk_io.get("devices"), list) else []
    total_row = (
        "ALL",
        format_number(disk_io.get("total_iops")),
        _kbps_text(disk_io.get("total_throughput_kbps")),
        _ms_text(disk_io.get("avg_latency_ms")),
        "",
    )
    device_rows = tuple(
        (
            str(item.get("name", "")),
            format_number(_device_iops(item)),
            _kbps_text(item.get("throughput_kbps")),
            _ms_text(item.get("avg_latency_ms")),
            format_percent(item.get("io_util_percent")),
        )
        for item in devices
        if isinstance(item, dict)
    )
    rows = (total_row,) + device_rows if device_rows else (total_row,)
    return SectionBlock(
        title=f"{options.section_prefix}.5 磁盘IO明细",
        tables=(full_table("磁盘IO明细", ("设备", "IOPS", "总吞吐", "平均时延", "IO利用率"), rows),),
    )

def _network_detail_section(result: dict[str, Any], options: OSSectionOptions) -> SectionBlock:
    network = _os_sample(result, "network")
    interfaces = network.get("interfaces", []) if isinstance(network.get("interfaces"), list) else []
    total_row = (
        "ALL",
        _rate_text(network.get("total_rx_bytes_per_sec")),
        _rate_text(network.get("total_tx_bytes_per_sec")),
        format_number(network.get("error_drop_per_sec")),
        "",
        "",
    )
    interface_rows = tuple(
        (
            str(item.get("name", "")),
            _rate_text(item.get("rx_bytes_per_sec")),
            _rate_text(item.get("tx_bytes_per_sec")),
            format_number(item.get("error_drop_per_sec")),
            format_bytes(item.get("bytes_recv")),
            format_bytes(item.get("bytes_sent")),
        )
        for item in interfaces
        if isinstance(item, dict)
    )
    rows = (total_row,) + interface_rows if interface_rows else (total_row,)
    return SectionBlock(
        title=f"{options.section_prefix}.6 网络接口明细",
        tables=(full_table("网络接口明细", ("接口", "接收速率", "发送速率", "错包/丢包速率", "累计接收", "累计发送"), rows),),
    )

def _meminfo_rows(memory: dict[str, Any]) -> list[tuple[str, str]]:
    meminfo = memory.get("meminfo", {}) if isinstance(memory.get("meminfo"), dict) else {}
    ordered_keys = [key for key in MEMINFO_PRIORITY_KEYS if key in meminfo]
    extra_keys = sorted(str(key) for key in meminfo.keys() if key not in MEMINFO_PRIORITY_KEYS)
    rows: list[tuple[str, str]] = []
    for key in (*ordered_keys, *extra_keys):
        value = meminfo.get(key)
        text = format_number(value, 0) if "Pages" in key else format_bytes(value)
        rows.append((key, text))
    return rows

def _os_sample(result: dict[str, Any], key: str) -> dict[str, Any]:
    samples = result.get("os", {}).get(key, {}).get("samples", [])
    sample = first_item(samples, {}) or {}
    return sample if isinstance(sample, dict) else {}

def _system_info(result: dict[str, Any]) -> dict[str, Any]:
    system = result.get("os", {}).get("system_info", {})
    return system if isinstance(system, dict) else {}

def _root_filesystem(result: dict[str, Any]) -> dict[str, Any]:
    filesystem = _os_sample(result, "filesystem")
    mountpoints = filesystem.get("mountpoints", []) if isinstance(filesystem.get("mountpoints"), list) else []
    for item in mountpoints:
        if isinstance(item, dict) and item.get("mountpoint") == "/":
            return item
    sample = first_item(mountpoints, {}) or {}
    return sample if isinstance(sample, dict) else {}

def _root_mount_desc(sample: dict[str, Any]) -> str:
    mountpoint = str(sample.get("mountpoint", "")).strip()
    return mountpoint or "根文件系统使用率"

def _device_iops(item: dict[str, Any]) -> float:
    read_iops = item.get("read_iops") if isinstance(item.get("read_iops"), (int, float)) else 0.0
    write_iops = item.get("write_iops") if isinstance(item.get("write_iops"), (int, float)) else 0.0
    return read_iops + write_iops

def _rate_text(value: Any) -> str:
    if not isinstance(value, (int, float)):
        return ""
    return f"{format_bytes(value)}/s"

def _kbps_text(value: Any) -> str:
    if not isinstance(value, (int, float)):
        return ""
    return f"{format_number(value)} KB/s"

def _ms_text(value: Any) -> str:
    if not isinstance(value, (int, float)):
        return ""
    return f"{format_number(value)} ms"


def _bool_text(value: Any) -> str:
    return "是" if value else "否"
