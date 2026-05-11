"""GaussDB WDR report section for SQL-first GaussDB reports."""

from __future__ import annotations

from typing import Any

from reporter.content.helpers import format_number, format_percent, full_table
from reporter.model.report_view import SectionBlock, TableBlock


def build_wdr_section(result: dict[str, Any]) -> SectionBlock | None:
    reports = _wdr_reports(result)
    if not reports:
        return None
    aggregate = _wdr_aggregate(result)
    tables = [
        _source_table(reports),
        _load_profile_table(reports),
        _instance_efficiency_table(aggregate),
        _io_profile_table(aggregate),
    ]
    wait_table = _wait_events_table(aggregate)
    if wait_table is not None:
        tables.append(wait_table)
    tables.extend((_sql_by_elapsed_table(aggregate), _sql_by_cpu_table(aggregate)))
    return SectionBlock(title="2.2.8 WDR 性能洞察", tables=tuple(tables))


def _wdr_reports(result: dict[str, Any]) -> list[dict[str, Any]]:
    db = result.get("db") if isinstance(result.get("db"), dict) else {}
    reports = db.get("wdr_reports") if isinstance(db, dict) else None
    if isinstance(reports, list):
        return [report for report in reports if isinstance(report, dict)]
    wdr = db.get("wdr") if isinstance(db, dict) else None
    return [wdr] if isinstance(wdr, dict) else []


def _wdr_aggregate(result: dict[str, Any]) -> dict[str, Any]:
    db = result.get("db") if isinstance(result.get("db"), dict) else {}
    wdr = db.get("wdr") if isinstance(db, dict) else None
    return wdr if isinstance(wdr, dict) else {}


def _metadata(report: dict[str, Any]) -> dict[str, Any]:
    meta = report.get("metadata")
    return meta if isinstance(meta, dict) else {}


def _items(payload: Any) -> list[dict[str, Any]]:
    if not isinstance(payload, dict):
        return []
    items = payload.get("items")
    if not isinstance(items, list):
        return []
    return [item for item in items if isinstance(item, dict)]


def _source_table(reports: list[dict[str, Any]]) -> TableBlock:
    rows = []
    for report in reports:
        meta = _metadata(report)
        rows.append(
            (
                str(meta.get("report_type") or ""),
                str(meta.get("report_scope") or ""),
                _report_node_label(meta),
                _snapshot_start(meta),
                _snapshot_end(meta),
                ", ".join(str(name) for name in meta.get("db_names", []) if str(name).strip()),
            )
        )
    return full_table("WDR 报告来源", ("类型", "范围", "节点", "开始时间", "结束时间", "数据库"), rows)


def _report_node_label(meta: dict[str, Any]) -> str:
    report_node = str(meta.get("report_node") or "").strip()
    if report_node:
        return report_node
    node_names = meta.get("node_names")
    if isinstance(node_names, list):
        names = [str(name) for name in node_names if str(name).strip()]
        if names:
            return ", ".join(names)
    return "-"


def _snapshot_start(meta: dict[str, Any]) -> str:
    snapshots = meta.get("snapshots")
    if not isinstance(snapshots, list) or not snapshots:
        return ""
    first = snapshots[0] if isinstance(snapshots[0], dict) else {}
    return str(first.get("start_time") or "")


def _snapshot_end(meta: dict[str, Any]) -> str:
    snapshots = meta.get("snapshots")
    if not isinstance(snapshots, list) or not snapshots:
        return ""
    last = snapshots[-1] if isinstance(snapshots[-1], dict) else {}
    return str(last.get("end_time") or "")


def _load_profile_table(reports: list[dict[str, Any]]) -> TableBlock:
    rows = []
    for report in reports:
        meta = _metadata(report)
        load_profile = report.get("load_profile") if isinstance(report.get("load_profile"), dict) else {}
        for item in _items(load_profile.get("workload")):
            metric = str(item.get("metric") or "")
            if metric not in {"DB Time(us)", "CPU Time(us)", "Logical read (blocks)", "Physical read (blocks)"}:
                continue
            rows.append(
                (
                    str(meta.get("report_scope") or ""),
                    _report_node_label(meta),
                    metric,
                    format_number(item.get("per_second"), 0),
                    format_number(item.get("per_transaction"), 0),
                    format_number(item.get("per_exec"), 0),
                )
            )
    return full_table("WDR 负载概要", ("范围", "节点", "指标", "每秒", "每事务", "每执行"), rows)


def _instance_efficiency_table(wdr: dict[str, Any]) -> TableBlock:
    payload = wdr.get("instance_efficiency")
    rows = [
        (
            str(item.get("source_scope") or ""),
            str(item.get("source_node") or "-"),
            str(item.get("label") or ""),
            format_percent(item.get("value_pct")),
        )
        for item in _items(payload)
    ]
    return full_table("WDR 实例效率", ("范围", "节点", "指标", "值"), rows)


def _io_profile_table(wdr: dict[str, Any]) -> TableBlock:
    payload = wdr.get("io_profile")
    rows = [
        (
            str(item.get("source_scope") or ""),
            str(item.get("source_node") or "-"),
            str(item.get("metric") or ""),
            format_number(item.get("read_write_per_sec")),
            format_number(item.get("read_per_sec")),
            format_number(item.get("write_per_sec")),
        )
        for item in _items(payload)
    ]
    return full_table("WDR IO 概要", ("范围", "节点", "指标", "读写/秒", "读/秒", "写/秒"), rows)


def _wait_events_table(wdr: dict[str, Any]) -> TableBlock | None:
    payload = wdr.get("wait_events")
    rows = [
        (
            str(item.get("source_scope") or ""),
            str(item.get("source_node") or "-"),
            str(item.get("event") or ""),
            format_number(item.get("waits"), 0),
            format_number(item.get("total_wait_time_us"), 0),
            format_number(item.get("avg_wait_time_us"), 0),
            str(item.get("type") or ""),
        )
        for item in _items(payload)
    ]
    if not rows:
        return None
    return full_table("WDR 等待事件Top10", ("范围", "节点", "等待事件", "等待次数", "总等待(us)", "平均等待(us)", "类型"), rows)


def _sql_by_elapsed_table(wdr: dict[str, Any]) -> TableBlock:
    sql = wdr.get("sql") if isinstance(wdr.get("sql"), dict) else {}
    return _sql_table("WDR Top SQL - Elapsed", sql.get("by_elapsed_time"), "total_elapse_time_us", "总耗时(us)")


def _sql_by_cpu_table(wdr: dict[str, Any]) -> TableBlock:
    sql = wdr.get("sql") if isinstance(wdr.get("sql"), dict) else {}
    return _sql_table("WDR Top SQL - CPU", sql.get("by_cpu_time"), "cpu_time_us", "CPU时间(us)")


def _sql_table(title: str, payload: Any, metric_key: str, metric_label: str) -> TableBlock:
    rows = [
        (
            str(item.get("source_scope") or ""),
            str(item.get("source_node") or item.get("node_name") or "-"),
            str(item.get("unique_sql_id") or ""),
            str(item.get("db_name") or ""),
            str(item.get("user_name") or ""),
            format_number(item.get("calls"), 0),
            format_number(item.get(metric_key), 0),
            format_number(item.get("avg_elapse_time_us"), 0),
            str(item.get("sql_text") or ""),
        )
        for item in _items(payload)[:20]
    ]
    return full_table(title, ("范围", "节点", "SQL ID", "数据库", "用户", "调用次数", metric_label, "平均耗时(us)", "SQL文本"), rows)
