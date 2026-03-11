"""Build MySQL backup detail sections."""

from __future__ import annotations

from typing import Any

from reporter.content.helpers import (
    format_duration_hours,
    format_number,
    format_time,
    full_table,
    key_value_table,
    row_value,
    unwrap_items,
)
from reporter.model.report_view import SectionBlock


def build_backup_section(result: dict[str, Any]) -> SectionBlock:
    return SectionBlock(title="2.4 数据库备份", children=(_backup_strategy(result), _backup_validation(result)))


def _backup_strategy(result: dict[str, Any]) -> SectionBlock:
    backup = _backup_payload(result)
    rows = (
        ("strategy_exists", format_number(backup.get("strategy_exists"))),
        ("binlog_format", str(backup.get("binlog_format", ""))),
        ("binlog_retention_policy", f"{format_number(backup.get('binlog_retention_policy'))} 天"),
        ("binlog_write_rate", format_number(backup.get("binlog_write_rate"))),
    )
    return SectionBlock(title="2.4.1 备份策略", tables=(key_value_table("备份策略", rows),))


def _backup_validation(result: dict[str, Any]) -> SectionBlock:
    backup = _backup_payload(result)
    rows = (
        ("last_backup_integrity", format_number(backup.get("last_backup_integrity"))),
        ("last_full_backup_age", format_duration_hours(backup.get("last_full_backup_age_hours"))),
        ("最近一次备份时间", _latest_backup_time(backup.get("backup_size_trend", {}))),
    )
    return SectionBlock(
        title="2.4.2 备份集可用性检查",
        tables=(key_value_table("备份集可用性检查", rows), _backup_history_table(backup)),
    )


def _backup_payload(result: dict[str, Any]) -> dict[str, Any]:
    backup = result.get("db", {}).get("backup")
    if isinstance(backup, dict):
        return backup
    return {}


def _backup_history_table(backup: dict[str, Any]):
    items = unwrap_items(backup.get("backup_size_trend"))
    rows = tuple(
        (
            format_time(str(row_value(item, "backup_time", "BACKUP_TIME"))),
            format_number(row_value(item, "backup_size_mb", "BACKUP_SIZE_MB")),
        )
        for item in items[:10]
    )
    table_rows = rows or (("无", "-"),)
    return full_table("最近备份记录", ("备份时间", "备份大小(MB)"), table_rows)


def _latest_backup_time(payload: Any) -> str:
    items = unwrap_items(payload)
    if not items:
        return ""
    return format_time(str(row_value(items[0], "backup_time", "BACKUP_TIME")))
