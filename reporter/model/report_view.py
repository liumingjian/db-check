"""Structured report view model shared by preview and future docx renderers."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass(frozen=True)
class TableBlock:
    title: str
    columns: tuple[str, ...]
    rows: tuple[tuple[str, ...], ...] = ()
    field_notes: tuple[tuple[str, str], ...] = ()
    status: str = "collected"
    note: str = ""
    sources: tuple[str, ...] = ()


@dataclass(frozen=True)
class SectionBlock:
    title: str
    status: str = "collected"
    paragraphs: tuple[str, ...] = ()
    tables: tuple[TableBlock, ...] = ()
    children: tuple["SectionBlock", ...] = ()
    note: str = ""


@dataclass(frozen=True)
class ReportView:
    title: str
    generated_at: str
    sections: tuple[SectionBlock, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
