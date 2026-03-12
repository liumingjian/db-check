"""Oracle backup/recovery report sections."""

from __future__ import annotations

from reporter.content.helpers import format_number, format_percent, full_table, key_value_table, row_value, unwrap_items
from reporter.content.oracle_section_utils import db_payload, first_row
from reporter.model.report_view import SectionBlock


def build_backup_section(result: dict[str, object]) -> SectionBlock:
    backup = db_payload(result, "backup")
    jobs = unwrap_items(backup.get("jobs"))
    latest = jobs[0] if jobs else {}
    recovery_area = first_row(backup.get("recovery_area"))
    archive_summary = first_row(backup.get("archive_log_summary"))
    summary_rows = (
        ("归档模式", str(backup.get("archive_log_mode", ""))),
        ("RMAN 备份数", format_number(len(jobs), 0)),
        ("最近备份状态", str(row_value(latest, "status"))),
        ("最近备份开始时间", str(row_value(latest, "start_time"))),
        ("恢复区使用率", format_percent(row_value(recovery_area, "space_used_pct"))),
        ("归档日志数量", format_number(row_value(archive_summary, "archive_count"), 0)),
    )
    return SectionBlock(
        title="2.2.6 备份与可恢复性",
        tables=(
            key_value_table("备份摘要", summary_rows),
            _table(backup, "jobs", "最近备份记录", ("会话", "类型", "状态", "开始时间", "结束时间", "耗时"), ("session_key", "input_type", "status", "start_time", "end_time", "hours")),
            _table(backup, "archive_destinations", "归档目的地", ("目的地", "状态", "路径", "目标", "归档进程", "错误"), ("dest_name", "status", "destination", "target", "archiver", "error")),
            _table(backup, "archive_destination_errors", "归档目的地异常", ("目的地", "状态", "路径", "错误"), ("dest_name", "status", "destination", "error")),
            _table(backup, "archive_log_summary", "归档日志摘要", ("归档数", "大小(GB)", "最早时间", "最新时间"), ("archive_count", "archive_size_gb", "oldest_archive_time", "newest_archive_time")),
            _table(backup, "recovery_area", "恢复区使用情况", ("名称", "上限(GB)", "已用(GB)", "可回收(GB)", "使用率", "文件数"), ("name", "space_limit_gb", "space_used_gb", "space_reclaimable_gb", "space_used_pct", "number_of_files")),
        ),
    )


def _table(payload: dict[str, object], key: str, title: str, columns: tuple[str, ...], fields: tuple[str, ...]):
    rows = tuple(tuple("" if row_value(item, field) is None else str(row_value(item, field)) for field in fields) for item in unwrap_items(payload.get(key)))
    return full_table(title, columns, rows or (("待补充",) + ("",) * (len(columns) - 1),))
