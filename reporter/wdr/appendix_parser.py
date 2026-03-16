"""Optional appendix tables parsing for WDR HTML."""

from __future__ import annotations

from typing import Any

from reporter.wdr.html_tables import _Table, _normalized_header, _optional_tables, _parse_int, _row_dict


def parse_appendix(tables: list[_Table]) -> dict[str, Any]:
    appendix: dict[str, Any] = {}
    bad_lock = _optional_tables(tables, summary_contains="bad lock stats")
    if bad_lock:
        appendix["bad_lock_stats"] = _parse_bad_lock_stats(bad_lock[0])
    return appendix


def _parse_bad_lock_stats(table: _Table) -> dict[str, Any]:
    header = _normalized_header(table)
    items = []
    for row in table.rows[1:]:
        if len(row) < len(header):
            continue
        record = _row_dict(header, row)
        node_name = str(record.get("node name") or "").strip()
        if not node_name:
            continue
        items.append(
            {
                "node_name": node_name,
                "db_id": _parse_int(record.get("db id")),
                "tablespace_id": _parse_int(record.get("tablespace id")),
                "relfilenode": _parse_int(record.get("relfilenode")),
                "fork_number": _parse_int(record.get("fork number")),
                "error_count": _parse_int(record.get("error count")),
                "first_time": str(record.get("first time") or "").strip(),
                "last_time": str(record.get("last time") or "").strip(),
            }
        )
    return {"items": items, "count": len(items)}

