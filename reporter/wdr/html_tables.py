"""WDR-specific table selection and value validation."""

from __future__ import annotations

from typing import Any

from reporter.html_tables import HTMLTable as _Table
from reporter.html_tables import HTMLTableCollector as _TableCollector
from reporter.html_tables import normalize_text as _normalize_text
from reporter.html_tables import normalized_header as _shared_normalized_header
from reporter.html_tables import parse_number as _parse_number
from reporter.html_tables import row_dict as _row_dict
from reporter.wdr.errors import WDRParseError


def _normalized_header(table: _Table) -> list[str]:
    if not table.rows:
        raise WDRParseError(f"table has no rows: {table.summary}")
    return _shared_normalized_header(table)


def _optional_tables(tables: list[_Table], *, summary_contains: str) -> list[_Table]:
    needle = summary_contains.lower().strip()
    return [table for table in tables if needle in table.summary.lower()]


def _require_table(tables: list[_Table], *, summary_contains: str) -> _Table:
    matches = _optional_tables(tables, summary_contains=summary_contains)
    if not matches:
        raise WDRParseError(f"missing required WDR table: summary contains {summary_contains!r}")
    return matches[0]


def _expect_table_header_and_first_row(table: _Table, ctx: str) -> tuple[list[str], tuple[str, ...]]:
    if len(table.rows) < 2:
        raise WDRParseError(f"{ctx}: expected header + 1 data row")
    return _normalized_header(table), table.rows[1]


def _parse_int(value: Any) -> int | None:
    num = _parse_number(value)
    if num is None:
        return None
    try:
        return int(num)
    except Exception as exc:  # noqa: BLE001
        raise WDRParseError(f"invalid int: {value!r}: {exc}") from exc
