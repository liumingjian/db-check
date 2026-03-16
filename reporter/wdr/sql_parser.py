"""SQL table parsing for WDR HTML."""

from __future__ import annotations

from typing import Any

from reporter.wdr.errors import WDRParseError
from reporter.wdr.html_tables import _Table, _normalized_header, _parse_int, _parse_number, _row_dict


def parse_sql_by_elapsed(table: _Table) -> dict[str, Any]:
    return _parse_sql_table(table, ctx="SQL ordered by Elapsed Time", mode="elapsed")


def parse_sql_by_cpu(table: _Table) -> dict[str, Any]:
    return _parse_sql_table(table, ctx="SQL ordered by CPU Time", mode="cpu")


def _parse_sql_table(table: _Table, *, ctx: str, mode: str) -> dict[str, Any]:
    header = _normalized_header(table)
    items = []
    for row in table.rows[1:]:
        if len(row) < len(header):
            continue
        record = _row_dict(header, row)
        sql_id = str(record.get("unique sql id") or "").strip()
        sql_text = str(record.get("sql text") or "").strip()
        if not sql_id and not sql_text:
            continue
        base = _sql_item(record, sql_id, sql_text, mode)
        items.append(base)
    if not items:
        raise WDRParseError(f"{ctx} table is empty")
    return {"items": items, "count": len(items)}


def _sql_item(record: dict[str, str], sql_id: str, sql_text: str, mode: str) -> dict[str, Any]:
    item: dict[str, Any] = {
        "unique_sql_id": sql_id,
        "db_name": str(record.get("db name") or "").strip(),
        "node_name": str(record.get("node name") or "").strip(),
        "user_name": str(record.get("user name") or "").strip(),
        "calls": _parse_int(record.get("calls")),
        "total_elapse_time_us": _parse_number(record.get("total elapse time(us)")),
        "avg_elapse_time_us": _parse_number(record.get("avg elapse time(us)")),
        "sql_text": sql_text,
    }
    if mode == "cpu":
        item["cpu_time_us"] = _parse_number(record.get("cpu time(us)"))
    return item

