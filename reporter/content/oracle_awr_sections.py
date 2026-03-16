"""Oracle AWR section for report view (requires enriched result.db.awr)."""

from __future__ import annotations

from typing import Any

from reporter.content.helpers import format_number, format_percent, full_table
from reporter.model.report_view import SectionBlock, TableBlock


def build_awr_section(result: dict[str, Any]) -> SectionBlock:
    awr = _awr_payload(result)
    return SectionBlock(
        title="2.2.7 AWR 分析",
        tables=(
            _load_profile_table(awr),
            _instance_efficiency_table(awr),
            _top_foreground_events_table(awr),
            _wait_classes_table(awr),
            _sql_by_elapsed_table(awr),
            _sql_by_cpu_table(awr),
            *_appendix_tables(awr),
        ),
    )


def _awr_payload(result: dict[str, Any]) -> dict[str, Any]:
    db = result.get("db")
    if not isinstance(db, dict):
        raise RuntimeError("result.db must be object")
    awr = db.get("awr")
    if not isinstance(awr, dict):
        raise RuntimeError("result.db.awr must be object")
    return awr


def _items(payload: Any) -> list[dict[str, Any]]:
    if not isinstance(payload, dict):
        return []
    items = payload.get("items")
    if not isinstance(items, list):
        return []
    return [item for item in items if isinstance(item, dict)]


def _load_profile_table(awr: dict[str, Any]) -> TableBlock:
    payload = awr.get("load_profile")
    columns = ("指标", "Per Second", "Per Transaction", "Per Exec", "Per Call")
    rows = [
        (
            str(item.get("metric") or ""),
            format_number(item.get("per_second")),
            format_number(item.get("per_transaction")),
            format_number(item.get("per_exec")),
            format_number(item.get("per_call")),
        )
        for item in _items(payload)
    ]
    return full_table("Load Profile", columns, rows)


def _instance_efficiency_table(awr: dict[str, Any]) -> TableBlock:
    payload = awr.get("instance_efficiency")
    columns = ("指标", "值(%)")
    rows = [(str(item.get("label") or ""), format_percent(item.get("value_pct"))) for item in _items(payload)]
    return full_table("Instance Efficiency Percentages", columns, rows)


def _top_foreground_events_table(awr: dict[str, Any]) -> TableBlock:
    payload = awr.get("top_foreground_events")
    columns = ("Event", "Waits", "Total Wait Time (sec)", "Avg Wait (ms)", "% DB Time", "Wait Class")
    rows = [
        (
            str(item.get("event") or ""),
            format_number(item.get("waits"), 0),
            format_number(item.get("total_wait_time_sec")),
            format_number(item.get("avg_wait_ms")),
            format_percent(item.get("pct_db_time")),
            str(item.get("wait_class") or ""),
        )
        for item in _items(payload)
    ]
    return full_table("Top 10 Foreground Events by Total Wait Time", columns, rows)


def _wait_classes_table(awr: dict[str, Any]) -> TableBlock:
    payload = awr.get("wait_classes")
    columns = ("Wait Class", "Waits", "Total Wait Time (sec)", "Avg Wait Time (ms)", "% DB Time", "Avg Active Sessions")
    rows = [
        (
            str(item.get("wait_class") or ""),
            format_number(item.get("waits"), 0),
            format_number(item.get("total_wait_time_sec")),
            format_number(item.get("avg_wait_time_ms")),
            format_percent(item.get("pct_db_time")),
            format_number(item.get("avg_active_sessions")),
        )
        for item in _items(payload)
    ]
    return full_table("Wait Classes by Total Wait Time", columns, rows)


def _sql_by_elapsed_table(awr: dict[str, Any]) -> TableBlock:
    payload = awr.get("sql", {}).get("by_elapsed_time") if isinstance(awr.get("sql"), dict) else None
    columns = ("SQL Id", "Executions", "Elapsed Time (s)", "CPU Time (s)", "%Total", "%CPU", "%IO", "SQL Module", "SQL Text")
    rows = [
        (
            str(item.get("sql_id") or ""),
            format_number(item.get("executions"), 0),
            format_number(item.get("elapsed_time_sec")),
            format_number(item.get("cpu_time_sec")),
            format_percent(item.get("pct_total")),
            format_percent(item.get("pct_cpu")),
            format_percent(item.get("pct_io")),
            str(item.get("sql_module") or ""),
            str(item.get("sql_text") or ""),
        )
        for item in _items(payload)
    ]
    return full_table("SQL ordered by Elapsed Time", columns, rows)


def _sql_by_cpu_table(awr: dict[str, Any]) -> TableBlock:
    payload = awr.get("sql", {}).get("by_cpu_time") if isinstance(awr.get("sql"), dict) else None
    columns = ("SQL Id", "Executions", "CPU Time (s)", "Elapsed Time (s)", "%Total", "%CPU", "%IO", "SQL Module", "SQL Text")
    rows = [
        (
            str(item.get("sql_id") or ""),
            format_number(item.get("executions"), 0),
            format_number(item.get("cpu_time_sec")),
            format_number(item.get("elapsed_time_sec")),
            format_percent(item.get("pct_total")),
            format_percent(item.get("pct_cpu")),
            format_percent(item.get("pct_io")),
            str(item.get("sql_module") or ""),
            str(item.get("sql_text") or ""),
        )
        for item in _items(payload)
    ]
    return full_table("SQL ordered by CPU Time", columns, rows)


def _appendix_tables(awr: dict[str, Any]) -> tuple[TableBlock, ...]:
    appendix = awr.get("appendix")
    if not isinstance(appendix, dict):
        return ()
    out: list[TableBlock] = []
    if "memory_statistics" in appendix:
        out.append(_begin_end_table("Memory Statistics", appendix.get("memory_statistics")))
    if "shared_pool_statistics" in appendix:
        out.append(_begin_end_table("Shared Pool Statistics", appendix.get("shared_pool_statistics")))
    if "cache_sizes" in appendix:
        out.append(_begin_end_table("Cache Sizes", appendix.get("cache_sizes")))
    return tuple(out)


def _begin_end_table(title: str, payload: Any) -> TableBlock:
    columns = ("指标", "Begin", "End")
    rows = [
        (
            str(item.get("metric") or ""),
            _value_unit(item.get("begin_value"), item.get("begin_unit")),
            _value_unit(item.get("end_value"), item.get("end_unit")),
        )
        for item in _items(payload)
    ]
    return full_table(title, columns, rows)


def _value_unit(value: Any, unit: Any) -> str:
    text = format_number(value)
    unit_text = str(unit or "").strip()
    return f"{text}{unit_text}" if unit_text else text

