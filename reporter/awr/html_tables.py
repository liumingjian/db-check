"""HTML table extraction and primitive value parsing for AWR HTML."""

from __future__ import annotations

from dataclasses import dataclass
from html.parser import HTMLParser
from typing import Any

from reporter.awr.errors import AWRParseError


@dataclass(frozen=True)
class _Table:
    summary: str
    heading: str
    rows: tuple[tuple[str, ...], ...]


class _TableCollector(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.tables: list[_Table] = []
        self._last_heading = ""
        self._in_h3 = False
        self._h3_parts: list[str] = []

        self._in_table = False
        self._table_summary = ""
        self._table_heading = ""
        self._rows: list[tuple[str, ...]] = []
        self._current_row: list[str] = []

        self._in_cell = False
        self._cell_parts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag == "h3":
            self._in_h3 = True
            self._h3_parts = []
            return
        if tag == "table":
            self._in_table = True
            self._rows = []
            self._current_row = []
            self._table_summary = _attr(attrs, "summary")
            self._table_heading = self._last_heading
            return
        if not self._in_table:
            return
        if tag == "tr":
            self._current_row = []
            return
        if tag in {"td", "th"}:
            self._in_cell = True
            self._cell_parts = []

    def handle_endtag(self, tag: str) -> None:
        if tag == "h3" and self._in_h3:
            self._in_h3 = False
            self._last_heading = _normalize_text("".join(self._h3_parts))
            return
        if not self._in_table:
            return
        if tag in {"td", "th"} and self._in_cell:
            self._in_cell = False
            self._current_row.append(_normalize_text("".join(self._cell_parts)))
            self._cell_parts = []
            return
        if tag == "tr":
            if self._current_row:
                self._rows.append(tuple(self._current_row))
            self._current_row = []
            return
        if tag == "table":
            self._in_table = False
            self.tables.append(_Table(summary=self._table_summary, heading=self._table_heading, rows=tuple(self._rows)))
            self._table_summary = ""
            self._table_heading = ""
            self._rows = []
            self._current_row = []

    def handle_data(self, data: str) -> None:
        if self._in_h3:
            self._h3_parts.append(data)
        if self._in_cell:
            self._cell_parts.append(data)


def _attr(attrs: list[tuple[str, str | None]], key: str) -> str:
    for k, v in attrs:
        if k == key:
            return "" if v is None else str(v)
    return ""


def _normalize_text(text: str) -> str:
    normalized = (text or "").replace("\xa0", " ")
    return " ".join(normalized.split()).strip()


def _normalized_header(table: _Table) -> list[str]:
    if not table.rows:
        raise AWRParseError(f"table has no rows: {table.summary}")
    return [_normalize_text(cell).lower() for cell in table.rows[0]]


def _row_dict(header: list[str], row: tuple[str, ...]) -> dict[str, str]:
    return {header[idx]: row[idx] for idx in range(min(len(header), len(row)))}


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
    header = [_normalize_text(cell).lower() for cell in table.rows[0]]
    return header, table.rows[1]


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


def _parse_number(value: Any) -> float | None:
    if value is None:
        return None
    text = _normalize_text(str(value))
    if not text:
        return None
    normalized = text.replace(",", "")
    if normalized.startswith("."):
        normalized = "0" + normalized
    multiplier = 1.0
    if normalized[-1:] in {"K", "k"}:
        multiplier = 1000.0
        normalized = normalized[:-1]
    try:
        return float(normalized) * multiplier
    except ValueError:
        return None


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
