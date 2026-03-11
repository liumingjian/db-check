"""Shared helpers for MySQL report content building."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Iterable, Sequence

from reporter.model.report_view import SectionBlock, TableBlock

STATUS_LABELS = {
    "collected": "已采集",
    "derived": "已推导",
    "external": "外部补充",
    "missing": "未采集",
    "na": "不适用",
}

LEVEL_LABELS = {
    "critical": "高风险",
    "warning": "中风险",
    "normal": "正常",
    "high": "高",
    "medium": "中",
    "low": "低",
}

LEVEL_ICONS = {
    "critical": "🔴",
    "warning": "🟡",
    "normal": "🟢",
    "high": "🔴",
    "medium": "🟡",
    "low": "🔵",
}


def nested_get(payload: dict[str, Any], path: Sequence[str], default: Any = "") -> Any:
    current: Any = payload
    for key in path:
        if not isinstance(current, dict) or key not in current:
            return default
        current = current[key]
    return current


def first_item(values: Any, default: Any = None) -> Any:
    if isinstance(values, list) and values:
        return values[0]
    return default


def unwrap_items(payload: Any) -> tuple[dict[str, Any], ...]:
    if isinstance(payload, dict) and isinstance(payload.get("items"), list):
        return tuple(item for item in payload["items"] if isinstance(item, dict))
    if isinstance(payload, list):
        return tuple(item for item in payload if isinstance(item, dict))
    return ()


def row_value(row: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in row:
            return row[key]
    lowered = {str(key).lower(): value for key, value in row.items()}
    for key in keys:
        lower_key = key.lower()
        if lower_key in lowered:
            return lowered[lower_key]
    return ""


def format_number(value: Any, digits: int = 2) -> str:
    if isinstance(value, bool):
        return "是" if value else "否"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return f"{value:.{digits}f}"
    if value is None or value == "":
        return ""
    return str(value)


def format_percent(value: Any, digits: int = 2) -> str:
    if value is None or value == "":
        return ""
    return f"{format_number(value, digits)}%"


def format_bytes(value: Any) -> str:
    if not isinstance(value, (int, float)):
        return ""
    units = ("B", "KB", "MB", "GB", "TB")
    size = float(value)
    unit_index = 0
    while size >= 1024 and unit_index < len(units) - 1:
        size /= 1024
        unit_index += 1
    return f"{size:.2f} {units[unit_index]}"


def format_duration_hours(hours: Any) -> str:
    if not isinstance(hours, (int, float)):
        return ""
    if hours < 24:
        return f"{format_number(hours)} 小时"
    return f"{format_number(hours / 24)} 天"


def format_uptime(seconds: Any) -> str:
    if not isinstance(seconds, (int, float)):
        return ""
    days = int(seconds // 86400)
    hours = int((seconds % 86400) // 3600)
    return f"{days} 天 {hours} 小时"


def format_time(value: str) -> str:
    if not value:
        return ""
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).strftime("%Y/%m/%d %H:%M:%S")
    except ValueError:
        return value


def ensure_rows(rows: Iterable[tuple[str, ...]], note: str, status: str) -> TableBlock:
    items = tuple(rows)
    return TableBlock(title="", columns=("说明",), rows=items, status=status, note=note)


def missing_table(title: str, note: str) -> TableBlock:
    return TableBlock(
        title=title,
        columns=("说明",),
        rows=((note,),),
        status="missing",
        note=note,
    )


def na_table(title: str, note: str) -> TableBlock:
    return TableBlock(
        title=title,
        columns=("说明",),
        rows=((note,),),
        status="na",
        note=note,
    )


def key_value_table(title: str, rows: Iterable[tuple[str, str]], status: str = "collected") -> TableBlock:
    return TableBlock(title=title, columns=("参数名称", "当前值"), rows=tuple(rows), status=status)


def full_table(
    title: str,
    columns: Sequence[str],
    rows: Iterable[tuple[str, ...]],
    status: str = "collected",
    note: str = "",
) -> TableBlock:
    return TableBlock(title=title, columns=tuple(columns), rows=tuple(rows), status=status, note=note)


def compact_table(
    title: str,
    columns: Sequence[str],
    rows: Iterable[tuple[str, ...]],
    field_notes: Sequence[tuple[str, str]],
    status: str = "collected",
    note: str = "",
) -> TableBlock:
    return TableBlock(
        title=title,
        columns=tuple(columns),
        rows=tuple(rows),
        field_notes=tuple(field_notes),
        status=status,
        note=note,
    )


def items_to_rows(items: Any, fields: Sequence[str], formatters: dict[int, Any] | None = None) -> tuple[tuple[str, ...], ...]:
    source_items = unwrap_items(items)
    formatter_map = formatters or {}
    rows: list[tuple[str, ...]] = []
    for item in source_items:
        row = tuple(format_cell(row_value(item, field), formatter_map.get(index)) for index, field in enumerate(fields))
        rows.append(row)
    return tuple(rows)


def format_cell(value: Any, formatter: Any) -> str:
    if callable(formatter):
        return formatter(value)
    if isinstance(value, (int, float, bool)):
        return format_number(value)
    if value is None or value == "":
        return ""
    return str(value)


def level_text(level: str) -> str:
    return LEVEL_LABELS.get(level, level)


def level_icon(level: str) -> str:
    return LEVEL_ICONS.get(level, "")


def section_with_table(title: str, table: TableBlock, note: str = "") -> SectionBlock:
    return SectionBlock(title=title, status=table.status, tables=(table,), note=note or table.note)
