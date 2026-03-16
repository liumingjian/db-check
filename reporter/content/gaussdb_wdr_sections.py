"""GaussDB WDR section for report view (requires enriched result.db.wdr)."""

from __future__ import annotations

from typing import Any

from reporter.content.helpers import format_number, format_percent, full_table
from reporter.model.report_view import SectionBlock, TableBlock


def build_wdr_section(result: dict[str, Any]) -> SectionBlock:
    wdr = _wdr_payload(result)
    tables: list[TableBlock] = [
        _database_stat_table(wdr),
        _load_profile_workload_table(wdr),
    ]
    response_table = _load_profile_response_time_table(wdr)
    if response_table is not None:
        tables.append(response_table)
    tables.extend(
        [
            _instance_efficiency_table(wdr),
            _io_profile_table(wdr),
            _sql_by_elapsed_table(wdr),
            _sql_by_cpu_table(wdr),
            *_appendix_tables(wdr),
        ]
    )
    return SectionBlock(title="2.2.6 WDR 分析", tables=tuple(tables))


def _wdr_payload(result: dict[str, Any]) -> dict[str, Any]:
    db = result.get("db")
    if not isinstance(db, dict):
        raise RuntimeError("result.db must be object")
    wdr = db.get("wdr")
    if not isinstance(wdr, dict):
        raise RuntimeError("result.db.wdr must be object")
    return wdr


def _items(payload: Any) -> list[dict[str, Any]]:
    if not isinstance(payload, dict):
        return []
    items = payload.get("items")
    if not isinstance(items, list):
        return []
    return [item for item in items if isinstance(item, dict)]


def _database_stat_table(wdr: dict[str, Any]) -> TableBlock:
    payload = wdr.get("database_stat")
    columns = ("Node Name", "DB Name", "Backends", "Xact Commit", "Xact Rollback", "Blks Read", "Blks Hit", "Deadlocks", "Stats Reset")
    rows = [
        (
            str(item.get("node_name") or ""),
            str(item.get("db_name") or ""),
            format_number(item.get("backends"), 0),
            format_number(item.get("xact_commit"), 0),
            format_number(item.get("xact_rollback"), 0),
            format_number(item.get("blks_read"), 0),
            format_number(item.get("blks_hit"), 0),
            format_number(item.get("deadlocks"), 0),
            str(item.get("stats_reset") or ""),
        )
        for item in _items(payload)
    ]
    return full_table("Database Stat", columns, rows)


def _load_profile_workload_table(wdr: dict[str, Any]) -> TableBlock:
    load_profile = wdr.get("load_profile") if isinstance(wdr.get("load_profile"), dict) else {}
    payload = load_profile.get("workload")
    columns = ("指标", "Per Second", "Per Transaction", "Per Exec")
    rows = [
        (
            str(item.get("metric") or ""),
            format_number(item.get("per_second")),
            format_number(item.get("per_transaction")),
            format_number(item.get("per_exec")),
        )
        for item in _items(payload)
    ]
    return full_table("Load Profile", columns, rows)


def _load_profile_response_time_table(wdr: dict[str, Any]) -> TableBlock | None:
    load_profile = wdr.get("load_profile") if isinstance(wdr.get("load_profile"), dict) else {}
    payload = load_profile.get("sql_response_time")
    if not isinstance(payload, dict):
        return None
    columns = ("指标", "Value(us)")
    rows = [(str(item.get("metric") or ""), format_number(item.get("value"), 0)) for item in _items(payload)]
    return full_table("SQL response time P80/P95", columns, rows)


def _instance_efficiency_table(wdr: dict[str, Any]) -> TableBlock:
    payload = wdr.get("instance_efficiency")
    columns = ("指标", "值(%)")
    rows = [(str(item.get("label") or ""), format_percent(item.get("value_pct"))) for item in _items(payload)]
    return full_table("Instance Efficiency Percentages", columns, rows)


def _io_profile_table(wdr: dict[str, Any]) -> TableBlock:
    payload = wdr.get("io_profile")
    columns = ("Metric", "Read+Write Per Sec", "Read Per Sec", "Write Per Sec")
    rows = [
        (
            str(item.get("metric") or ""),
            format_number(item.get("read_write_per_sec")),
            format_number(item.get("read_per_sec")),
            format_number(item.get("write_per_sec")),
        )
        for item in _items(payload)
    ]
    return full_table("IO Profile", columns, rows)


def _sql_by_elapsed_table(wdr: dict[str, Any]) -> TableBlock:
    sql = wdr.get("sql") if isinstance(wdr.get("sql"), dict) else {}
    payload = sql.get("by_elapsed_time")
    columns = ("Unique SQL Id", "DB Name", "Node Name", "User Name", "Calls", "Total Elapse Time(us)", "Avg Elapse Time(us)", "SQL Text")
    rows = [
        (
            str(item.get("unique_sql_id") or ""),
            str(item.get("db_name") or ""),
            str(item.get("node_name") or ""),
            str(item.get("user_name") or ""),
            format_number(item.get("calls"), 0),
            format_number(item.get("total_elapse_time_us"), 0),
            format_number(item.get("avg_elapse_time_us"), 0),
            str(item.get("sql_text") or ""),
        )
        for item in _items(payload)
    ]
    return full_table("SQL ordered by Elapsed Time", columns, rows)


def _sql_by_cpu_table(wdr: dict[str, Any]) -> TableBlock:
    sql = wdr.get("sql") if isinstance(wdr.get("sql"), dict) else {}
    payload = sql.get("by_cpu_time")
    columns = ("Unique SQL Id", "DB Name", "Node Name", "User Name", "Calls", "CPU Time(us)", "Avg Elapse Time(us)", "SQL Text")
    rows = [
        (
            str(item.get("unique_sql_id") or ""),
            str(item.get("db_name") or ""),
            str(item.get("node_name") or ""),
            str(item.get("user_name") or ""),
            format_number(item.get("calls"), 0),
            format_number(item.get("cpu_time_us"), 0),
            format_number(item.get("avg_elapse_time_us"), 0),
            str(item.get("sql_text") or ""),
        )
        for item in _items(payload)
    ]
    return full_table("SQL ordered by CPU Time", columns, rows)


def _appendix_tables(wdr: dict[str, Any]) -> tuple[TableBlock, ...]:
    appendix = wdr.get("appendix")
    if not isinstance(appendix, dict):
        return ()
    bad_lock = appendix.get("bad_lock_stats")
    if not isinstance(bad_lock, dict):
        return ()
    columns = ("Node Name", "DB Id", "Tablespace Id", "Relfilenode", "Fork Number", "Error Count", "First Time", "Last Time")
    rows = [
        (
            str(item.get("node_name") or ""),
            format_number(item.get("db_id"), 0),
            format_number(item.get("tablespace_id"), 0),
            format_number(item.get("relfilenode"), 0),
            format_number(item.get("fork_number"), 0),
            format_number(item.get("error_count"), 0),
            str(item.get("first_time") or ""),
            str(item.get("last_time") or ""),
        )
        for item in _items(bad_lock)
    ]
    if not rows:
        rows = [("-", "-", "-", "-", "-", "-", "无", "无")]
    return (full_table("Bad lock stats", columns, rows),)

