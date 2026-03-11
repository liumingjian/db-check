"""Assemble chapter 2 detail sections."""

from __future__ import annotations

from typing import Any

from reporter.content.helpers import first_item, format_percent, full_table
from reporter.content.mysql_backup_details import build_backup_section
from reporter.content.mysql_basic_details import build_mysql_basic_info
from reporter.content.mysql_performance_details import build_mysql_performance
from reporter.model.report_view import SectionBlock


def build_detail_section(result: dict[str, Any], summary: dict[str, Any], meta: dict[str, Any]) -> SectionBlock:
    return SectionBlock(
        title="第二章 巡检明细",
        children=(
            _build_system_metrics(result),
            build_mysql_basic_info(result, summary, meta),
            build_mysql_performance(result),
            build_backup_section(result),
        ),
    )


def _build_system_metrics(result: dict[str, Any]) -> SectionBlock:
    table = full_table(
        "系统指标",
        ("指标", "当前值", "说明"),
        _system_metric_rows(result),
    )
    return SectionBlock(title="2.1 系统指标", tables=(table,))


def _system_metric_rows(result: dict[str, Any]) -> tuple[tuple[str, str, str], ...]:
    cpu = first_item(result.get("os", {}).get("cpu", {}).get("samples", []), {}) or {}
    memory = first_item(result.get("os", {}).get("memory", {}).get("samples", []), {}) or {}
    fs = _filesystem_sample(result)
    system = result.get("os", {}).get("system_info", {}) if isinstance(result.get("os", {}).get("system_info"), dict) else {}
    return (
        ("CPU 使用率", format_percent(cpu.get("usage_percent")), "采样时点 CPU 平均使用率"),
        ("CPU iowait", format_percent(cpu.get("iowait_percent")), "采样时点磁盘等待占比"),
        ("内存使用率", format_percent(memory.get("usage_percent")), "采样时点内存占用"),
        ("磁盘使用率", format_percent(fs.get("usage_percent")), _filesystem_desc(fs)),
        ("文件描述符使用率", format_percent(system.get("file_descriptor_usage_percent")), "OS 级 fd 使用情况"),
        ("MySQL fd 使用率", format_percent(system.get("mysql_fd_usage_percent")), "mysqld 进程 fd 使用情况"),
    )


def _filesystem_sample(result: dict[str, Any]) -> dict[str, Any]:
    filesystem = result.get("os", {}).get("filesystem", {})
    if not isinstance(filesystem, dict):
        return {}
    sample = first_item(filesystem.get("samples", []), {}) or {}
    mountpoints = sample.get("mountpoints", []) if isinstance(sample, dict) else []
    if not isinstance(mountpoints, list):
        return {}
    for item in mountpoints:
        if isinstance(item, dict) and item.get("mountpoint") == "/":
            return item
    return first_item(mountpoints, {}) or {}


def _filesystem_desc(sample: dict[str, Any]) -> str:
    mountpoint = sample.get("mountpoint", "")
    if mountpoint:
        return str(mountpoint)
    return "根文件系统使用率"
