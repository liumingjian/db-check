"""Optional appendix tables parsing for AWR HTML."""

from __future__ import annotations

from typing import Any

from reporter.awr.errors import AWRParseError
from reporter.awr.html_tables import _Table, _normalized_header, _optional_table, _parse_number_with_unit


def parse_appendix(tables: list[_Table]) -> dict[str, Any]:
    appendix: dict[str, Any] = {}
    memory = _optional_table(tables, summary_contains="memory statistics")
    if memory is not None:
        appendix["memory_statistics"] = _parse_begin_end_table(memory)
    shared = _optional_table(tables, summary_contains="shared pool statistics")
    if shared is not None:
        appendix["shared_pool_statistics"] = _parse_begin_end_table(shared)
    cache_sizes = _optional_table(tables, summary_contains="cache sizes and other statistics")
    if cache_sizes is not None:
        appendix["cache_sizes"] = _parse_cache_sizes(cache_sizes)
    return appendix


def _parse_begin_end_table(table: _Table) -> dict[str, Any]:
    header = _normalized_header(table)
    if "begin" not in header or "end" not in header:
        raise AWRParseError(f"invalid appendix table header: {table.summary}")
    items = []
    for row in table.rows[1:]:
        if len(row) < 3:
            continue
        metric = row[0].rstrip(":").strip()
        begin = _parse_number_with_unit(row[1])
        end = _parse_number_with_unit(row[2])
        if not metric:
            continue
        items.append(
            {
                "metric": metric,
                "begin_value": begin.get("value"),
                "begin_unit": begin.get("unit"),
                "end_value": end.get("value"),
                "end_unit": end.get("unit"),
            }
        )
    return {"items": items, "count": len(items)}


def _parse_cache_sizes(table: _Table) -> dict[str, Any]:
    items = []
    for row in table.rows[1:]:
        if len(row) < 3:
            continue
        metric = row[0].rstrip(":").strip()
        begin = _parse_number_with_unit(row[1])
        end = _parse_number_with_unit(row[2])
        if not metric:
            continue
        items.append(
            {
                "metric": metric,
                "begin_value": begin.get("value"),
                "begin_unit": begin.get("unit"),
                "end_value": end.get("value"),
                "end_unit": end.get("unit"),
            }
        )
    return {"items": items, "count": len(items)}

