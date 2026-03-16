"""Parse Oracle AWR HTML into structured, unit-safe metrics.

This parser is intentionally strict:
- Missing required (core) tables is an error.
- Numeric values are normalized into numbers + optional unit fields.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from reporter.awr.errors import AWRParseError
from reporter.awr.appendix_parser import parse_appendix
from reporter.awr.html_tables import (
    _Table,
    _TableCollector,
    _expect_table_header_and_first_row,
    _normalize_text,
    _normalized_header,
    _optional_table,
    _parse_int,
    _parse_number,
    _parse_number_with_unit,
    _require_int,
    _require_table,
    _row_dict,
)


@dataclass(frozen=True)
class AWRMetadata:
    db_name: str
    db_id: int


@dataclass(frozen=True)
class AWRPayload:
    metadata: AWRMetadata
    instance_efficiency: dict[str, Any]
    wait_classes: dict[str, Any]
    top_foreground_events: dict[str, Any]
    load_profile: dict[str, Any]
    sql: dict[str, Any]
    appendix: dict[str, Any]

    def to_result_payload(self) -> dict[str, Any]:
        return {
            "metadata": {"db_name": self.metadata.db_name, "db_id": self.metadata.db_id},
            "instance_efficiency": self.instance_efficiency,
            "wait_classes": self.wait_classes,
            "top_foreground_events": self.top_foreground_events,
            "load_profile": self.load_profile,
            "sql": self.sql,
            "appendix": self.appendix,
        }


def parse_awr_html(path: Path) -> AWRPayload:
    if not path.exists() or not path.is_file():
        raise AWRParseError(f"awr file not found: {path}")

    collector = _TableCollector()
    collector.feed(path.read_text(encoding="utf-8", errors="strict"))
    tables = collector.tables

    metadata = _parse_metadata(_require_metadata_table(tables))
    load_profile = _parse_load_profile(_require_table(tables, summary_contains="load profile"))
    instance_efficiency = _parse_instance_efficiency(
        _require_table(tables, summary_contains="instance efficiency percentages")
    )
    top_events = _parse_top_foreground_events(
        _require_table(tables, summary_contains="top 10 wait events by total wait time")
    )
    wait_classes = _parse_wait_classes(
        _require_table(tables, summary_contains="wait class statistics ordered by total wait time")
    )
    sql_elapsed = _parse_sql_table(_require_table(tables, summary_contains="top sql by elapsed time"))
    sql_cpu = _parse_sql_table(_require_table(tables, summary_contains="top sql by cpu time"))
    appendix = parse_appendix(tables)

    return AWRPayload(
        metadata=metadata,
        instance_efficiency=instance_efficiency,
        wait_classes=wait_classes,
        top_foreground_events=top_events,
        load_profile=load_profile,
        sql={"by_elapsed_time": sql_elapsed, "by_cpu_time": sql_cpu},
        appendix=appendix,
    )


def _require_metadata_table(tables: list[_Table]) -> _Table:
    candidates = [t for t in tables if "database instance information" in t.summary.lower()]
    for table in candidates:
        if not table.rows:
            continue
        header = [_normalize_text(cell).lower() for cell in table.rows[0]]
        if "db name" in header and "db id" in header:
            return table
    raise AWRParseError("missing required AWR table: database instance information (DB Name/DB Id)")


def _parse_metadata(table: _Table) -> AWRMetadata:
    header, row = _expect_table_header_and_first_row(table, "db metadata")
    values = {h: (row[idx] if idx < len(row) else "") for idx, h in enumerate(header)}
    db_name = _normalize_text(values.get("db name", ""))
    db_id_raw = values.get("db id", "")
    db_id = _require_int(db_id_raw, ctx="db id")
    if not db_name:
        raise AWRParseError("AWR DB Name is empty")
    return AWRMetadata(db_name=db_name, db_id=db_id)


def _parse_load_profile(table: _Table) -> dict[str, Any]:
    rows = []
    for item in table.rows:
        if len(item) < 2:
            continue
        metric = item[0].rstrip(":").strip()
        if not metric:
            continue
        rows.append(
            {
                "metric": metric,
                "per_second": _parse_number(item[1]),
                "per_transaction": _parse_number(item[2]) if len(item) > 2 else None,
                "per_exec": _parse_number(item[3]) if len(item) > 3 else None,
                "per_call": _parse_number(item[4]) if len(item) > 4 else None,
            }
        )
    if not rows:
        raise AWRParseError("Load Profile table is empty")
    return {"items": rows, "count": len(rows)}


def _parse_instance_efficiency(table: _Table) -> dict[str, Any]:
    items: list[dict[str, Any]] = []
    for row in table.rows:
        items.extend(_pairwise_metrics(row))
    by_key = {m["key"]: m["value_pct"] for m in items if isinstance(m.get("value_pct"), (int, float))}
    required = {
        "execute_to_parse_pct": by_key.get("execute_to_parse_pct"),
        "soft_parse_pct": by_key.get("soft_parse_pct"),
        "library_hit_pct": by_key.get("library_hit_pct"),
        "buffer_hit_pct": by_key.get("buffer_hit_pct"),
    }
    missing = [k for k, v in required.items() if v is None]
    if missing:
        raise AWRParseError(f"Instance Efficiency missing required metrics: {', '.join(missing)}")
    return {**required, "items": items, "count": len(items)}


def _pairwise_metrics(row: tuple[str, ...]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for idx in range(0, len(row) - 1, 2):
        label = _normalize_text(row[idx]).rstrip(":")
        value = _parse_number(row[idx + 1])
        if not label:
            continue
        key = _efficiency_key(label)
        out.append({"label": label, "key": key, "value_pct": value})
    return out


def _efficiency_key(label: str) -> str:
    normalized = _normalize_text(label).lower().replace(" ", "")
    mapping = {
        "executetoparse%": "execute_to_parse_pct",
        "softparse%": "soft_parse_pct",
        "libraryhit%": "library_hit_pct",
        "bufferhit%": "buffer_hit_pct",
    }
    return mapping.get(normalized, normalized)


def _parse_top_foreground_events(table: _Table) -> dict[str, Any]:
    header = _normalized_header(table)
    items = []
    for row in table.rows[1:]:
        if len(row) < len(header):
            continue
        record = _row_dict(header, row)
        event = str(record.get("event") or "").strip()
        if not event:
            continue
        items.append(
            {
                "event": event,
                "waits": _parse_int(record.get("waits")),
                "total_wait_time_sec": _parse_number(record.get("total wait time (sec)")),
                "avg_wait_ms": _parse_number_with_unit(record.get("avg wait")).get("value"),
                "pct_db_time": _parse_number(record.get("% db time")),
                "wait_class": str(record.get("wait class") or "").strip(),
            }
        )
    top = _top_non_cpu(items, key_name="event")
    return {**top, "items": items, "count": len(items)}


def _parse_wait_classes(table: _Table) -> dict[str, Any]:
    header = _normalized_header(table)
    items = []
    for row in table.rows[1:]:
        if len(row) < len(header):
            continue
        record = _row_dict(header, row)
        wait_class = str(record.get("wait class") or "").strip()
        if not wait_class:
            continue
        items.append(
            {
                "wait_class": wait_class,
                "waits": _parse_int(record.get("waits")),
                "total_wait_time_sec": _parse_number(record.get("total wait time (sec)")),
                "avg_wait_time_ms": _parse_number_with_unit(record.get("avg wait time")).get("value"),
                "pct_db_time": _parse_number(record.get("% db time")),
                "avg_active_sessions": _parse_number(record.get("avg active sessions")),
            }
        )
    top = _top_non_cpu(items, key_name="wait_class")
    return {**top, "items": items, "count": len(items)}


def _top_non_cpu(items: list[dict[str, Any]], *, key_name: str) -> dict[str, Any]:
    for row in items:
        if str(row.get(key_name) or "").strip().upper() == "DB CPU":
            continue
        pct = row.get("pct_db_time")
        if isinstance(pct, (int, float)):
            return {f"top_non_cpu_{key_name}": str(row.get(key_name)), "top_non_cpu_pct_db_time": pct}
    raise AWRParseError(f"cannot find non-DB CPU row for {key_name}")


def _parse_sql_table(table: _Table) -> dict[str, Any]:
    header = _normalized_header(table)
    items = []
    for row in table.rows[1:]:
        if len(row) < len(header):
            continue
        record = _row_dict(header, row)
        sql_id = str(record.get("sql id") or "").strip()
        sql_text = str(record.get("sql text") or "").strip()
        if not sql_id and not sql_text:
            continue
        items.append(
            {
                "sql_id": sql_id,
                "sql_text": sql_text,
                "executions": _parse_int(record.get("executions")),
                "elapsed_time_sec": _parse_number(record.get("elapsed  time (s)")),
                "cpu_time_sec": _parse_number(record.get("cpu    time (s)")),
                "pct_total": _parse_number(record.get("%total") or record.get("%total ")),
                "pct_cpu": _parse_number(record.get("%cpu")),
                "pct_io": _parse_number(record.get("%io")),
                "sql_module": str(record.get("sql module") or "").strip(),
            }
        )
    if not items:
        raise AWRParseError(f"SQL table is empty: heading={table.heading!r} summary={table.summary!r}")
    return {"items": items, "count": len(items)}
