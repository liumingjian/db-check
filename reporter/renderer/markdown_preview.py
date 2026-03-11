"""Render a ReportView into Markdown report output."""

from __future__ import annotations

from typing import Iterable

from reporter.content.helpers import STATUS_LABELS
from reporter.model.report_view import ReportView, SectionBlock, TableBlock


def render_markdown_preview(report: ReportView) -> str:
    lines = [f"# {report.title}", "", f"生成时间: {report.generated_at}", ""]
    for section in report.sections:
        lines.extend(_render_section(section, level=2))
    return "\n".join(lines).strip() + "\n"


def _render_section(section: SectionBlock, level: int) -> list[str]:
    lines = [f"{'#' * level} {section.title}", ""]
    if section.note:
        lines.extend(_status_note(section.status, section.note))
    for paragraph in section.paragraphs:
        lines.extend((paragraph, ""))
    for table in section.tables:
        lines.extend(_render_table(table))
    for child in section.children:
        lines.extend(_render_section(child, level + 1))
    return lines


def _render_table(table: TableBlock) -> list[str]:
    lines: list[str] = []
    if table.title:
        lines.extend((f"**{table.title}**", ""))
    lines.extend(_table_lines(table.columns, table.rows))
    if table.field_notes:
        lines.extend(("", "字段说明：", ""))
        lines.extend(_render_field_notes(table.field_notes))
    if table.note:
        lines.extend(("", *_status_note(table.status, table.note)))
    lines.append("")
    return lines


def _table_lines(columns: tuple[str, ...], rows: tuple[tuple[str, ...], ...]) -> Iterable[str]:
    yield "| " + " | ".join(columns) + " |"
    yield "| " + " | ".join("---" for _ in columns) + " |"
    if not rows:
        yield "| " + " | ".join("" for _ in columns) + " |"
        return
    for row in rows:
        yield "| " + " | ".join(_escape_cell(cell) for cell in row) + " |"


def _status_note(status: str, note: str) -> tuple[str, str, str]:
    return (f"> 状态: {STATUS_LABELS.get(status, status)}", f"> 说明: {note}", "")


def _render_field_notes(field_notes: tuple[tuple[str, str], ...]) -> list[str]:
    lines: list[str] = []
    for field_name, description in field_notes:
        lines.append(f"- `{field_name}`: {description}")
    lines.append("")
    return lines


def _escape_cell(value: str) -> str:
    return str(value).replace("\n", " ").replace("|", "\\|")
