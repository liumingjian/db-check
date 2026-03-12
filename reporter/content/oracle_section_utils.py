"""Shared helpers for Oracle report sections."""

from __future__ import annotations

from typing import Any

from reporter.content.helpers import format_number, unwrap_items


def db_payload(result: dict[str, Any], key: str) -> dict[str, Any]:
    payload = result.get("db", {}).get(key, {})
    return payload if isinstance(payload, dict) else {}


def first_row(value: Any) -> dict[str, Any]:
    items = unwrap_items(value)
    return items[0] if items else {}


def count_text(value: Any) -> str:
    if isinstance(value, dict) and isinstance(value.get("count"), int):
        return format_number(value["count"], 0)
    return format_number(len(unwrap_items(value)), 0)


def bytes_to_mb(value: Any) -> str:
    if value in (None, ""):
        return ""
    try:
        return format_number(float(value) / 1024 / 1024)
    except (TypeError, ValueError):
        return str(value)
