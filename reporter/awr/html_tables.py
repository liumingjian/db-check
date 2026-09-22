"""AWR-specific table selection and value validation."""

from __future__ import annotations

from typing import Any

from reporter.awr.errors import AWRParseError
from reporter.html_tables import HTMLTable as _Table
from reporter.html_tables import HTMLTableCollector as _TableCollector
from reporter.html_tables import normalize_text as _normalize_text
from reporter.html_tables import normalized_header as _shared_normalized_header
from reporter.html_tables import parse_number as _parse_number
from reporter.html_tables import row_dict as _shared_row_dict


def _normalized_header(table: _Table) -> list[str]:
    if not table.rows:
        raise AWRParseError(f"table has no rows: {table.summary}")
    return _shared_normalized_header(table)


def _row_dict(header: list[str], row: tuple[str, ...]) -> dict[str, str]:
    return _shared_row_dict(header, row)


def _require_table(tables: list[_Table], *, summary_contains: str) -> _Table:
    match = _optional_table(tables, summary_contains=summary_contains)
    if match is None:
        raise AWRParseError(f"missing required AWR table: summary contains {summary_contains!r}")
    return match


def _optional_table(tables: list[_Table], *, summary_contains: str) -> _Table | None:
    needle = summary_contains.lower().strip()
    for table in tables:
        if needle in table.summary.lower():
            return table
    return None


def _expect_table_header_and_first_row(table: _Table, ctx: str) -> tuple[list[str], tuple[str, ...]]:
    if len(table.rows) < 2:
        raise AWRParseError(f"{ctx}: expected header + 1 data row")
    return _normalized_header(table), table.rows[1]


def _require_int(value: str, *, ctx: str) -> int:
    parsed = _parse_int(value)
    if parsed is None:
        raise AWRParseError(f"{ctx}: expected integer, got {value!r}")
    return parsed


def _parse_int(value: Any) -> int | None:
    num = _parse_number(value)
    if num is None:
        return None
    try:
        return int(num)
    except Exception as exc:  # noqa: BLE001
        raise AWRParseError(f"invalid int: {value!r}: {exc}") from exc


def _parse_number_with_unit(value: Any) -> dict[str, Any]:
    if value is None:
        return {"value": None, "unit": ""}
    text = _normalize_text(str(value))
    if not text:
        return {"value": None, "unit": ""}
    normalized = text.replace(",", "")
    unit = ""
    for suffix in ("ms", "us", "ns", "s", "K", "M", "G", "k", "m", "g"):
        if normalized.endswith(suffix):
            unit = suffix
            normalized = normalized[: -len(suffix)]
            break
    if normalized.startswith("."):
        normalized = "0" + normalized
    try:
        return {"value": float(normalized), "unit": unit}
    except ValueError:
        return {"value": None, "unit": unit}
