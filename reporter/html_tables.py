"""Shared low-level extraction for report HTML tables."""

from __future__ import annotations

from dataclasses import dataclass
from html.parser import HTMLParser
from typing import Any


_KILO_MULTIPLIER = 1000.0


@dataclass(frozen=True)
class HTMLTable:
    summary: str
    heading: str
    rows: tuple[tuple[str, ...], ...]


class HTMLTableCollector(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.tables: list[HTMLTable] = []
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
            self._last_heading = normalize_text("".join(self._h3_parts))
            return
        if not self._in_table:
            return
        if tag in {"td", "th"} and self._in_cell:
            self._in_cell = False
            self._current_row.append(normalize_text("".join(self._cell_parts)))
            self._cell_parts = []
            return
        if tag == "tr":
            if self._current_row:
                self._rows.append(tuple(self._current_row))
            self._current_row = []
            return
        if tag == "table":
            self._in_table = False
            self.tables.append(
                HTMLTable(
                    summary=self._table_summary,
                    heading=self._table_heading,
                    rows=tuple(self._rows),
                )
            )
            self._table_summary = ""
            self._table_heading = ""
            self._rows = []
            self._current_row = []

    def handle_data(self, data: str) -> None:
        if self._in_h3:
            self._h3_parts.append(data)
        if self._in_cell:
            self._cell_parts.append(data)


def normalize_text(text: str) -> str:
    normalized = (text or "").replace("\xa0", " ")
    return " ".join(normalized.split()).strip()


def normalized_header(table: HTMLTable) -> list[str]:
    return [normalize_text(cell).lower() for cell in table.rows[0]]


def row_dict(header: list[str], row: tuple[str, ...]) -> dict[str, str]:
    return {header[idx]: row[idx] for idx in range(min(len(header), len(row)))}


def parse_number(value: Any) -> float | None:
    if value is None:
        return None
    text = normalize_text(str(value))
    if not text:
        return None
    normalized = text.replace(",", "")
    if normalized.startswith("."):
        normalized = "0" + normalized
    multiplier = 1.0
    if normalized[-1:] in {"K", "k"}:
        multiplier = _KILO_MULTIPLIER
        normalized = normalized[:-1]
    try:
        return float(normalized) * multiplier
    except ValueError:
        return None


def _attr(attrs: list[tuple[str, str | None]], key: str) -> str:
    for attr_key, value in attrs:
        if attr_key == key:
            return "" if value is None else str(value)
    return ""
