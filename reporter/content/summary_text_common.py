"""Shared narrative helpers for report summary sections."""

from __future__ import annotations

from collections import defaultdict
from typing import Any, Callable

from reporter.content.helpers import format_number, format_percent, row_value, unwrap_items

DisplayNameFn = Callable[[str], str]


def make_dimension_labels(
    business_dimensions: tuple[tuple[str, set[str]], ...],
) -> dict[str, str]:
    return {
        name: label
        for label, names in business_dimensions
        for name in names
    }


def group_abnormal_items(
    summary: dict[str, Any],
    display_dimension_name: DisplayNameFn,
) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    abnormal_items = summary.get("abnormal_items", [])
    if not isinstance(abnormal_items, list):
        return grouped
    for item in abnormal_items:
        if not isinstance(item, dict):
            continue
        dimension_name = display_dimension_name(str(item.get("dimension_name", "未分类")))
        grouped[dimension_name].append(item)
    return grouped


def focus_dimensions(
    abnormal_items: list[dict[str, Any]],
    display_dimension_name: DisplayNameFn,
) -> str:
    counts: dict[str, int] = defaultdict(int)
    for item in abnormal_items:
        counts[display_dimension_name(str(item.get("dimension_name", "未分类")))] += 1
    if not counts:
        return "未发现突出风险维度"
    ordered = sorted(counts.items(), key=lambda item: (-item[1], item[0]))[:3]
    return "、".join(name for name, _ in ordered)


def top_level(items: list[dict[str, Any]]) -> str:
    return "critical" if any(item.get("level") == "critical" for item in items) else "warning"


def risk_description(item: dict[str, Any]) -> str:
    name = str(item.get("name", "未命名检查项"))
    current = item.get("current_value")
    if isinstance(current, dict):
        max_value = current.get("max_value")
        if max_value not in (None, ""):
            return f"{name}（当前值: {format_number(max_value)}）"
        count = len(unwrap_items(current))
        return f"{name}（发现 {count} 条记录）"
    if isinstance(current, list):
        return f"{name}（记录数 {len(current)}）"
    if current is None or current == "":
        return name
    return f"{name}（当前值: {format_number(current)}）"


def impact_analysis(item: dict[str, Any]) -> str:
    level = str(item.get("level", "warning"))
    reason = str(item.get("reason", ""))
    if reason.startswith("critical threshold hit"):
        return "当前观测结果已触发高风险阈值，可能直接影响业务连续性、可用性或数据安全。"
    if reason.startswith("warning threshold hit"):
        return "当前观测结果已触发预设告警阈值，说明该项存在明确风险或持续性异常。"
    if reason.startswith("exists check matched"):
        return "本次采集已发现对应异常记录，说明该类问题在当前实例中真实存在。"
    if level == "critical":
        return "当前问题已达到高风险等级，建议优先处置。"
    return "当前问题已在本次巡检中命中，建议结合业务影响尽快处理。"


def metric_text(pairs: tuple[tuple[str, str], ...]) -> str:
    filtered = [f"{label} {emphasize(value)}" for label, value in pairs if value]
    return "；".join(filtered) if filtered else "待补充"


def level_label(level: str) -> str:
    return {
        "high": "高",
        "medium": "中",
        "low": "低",
        "critical": "高风险",
        "warning": "中风险",
        "normal": "正常",
    }.get(level, level)


def overall_risk_label(level: str) -> str:
    return {
        "high": "高风险",
        "medium": "中风险",
        "low": "低风险",
    }.get(level, level_label(level))


def emphasize(value: Any) -> str:
    text = "" if value is None else str(value)
    return f"**{text}**" if text else "待补充"


def risk_stat_text(summary: dict[str, Any]) -> str:
    counts = summary.get("counts", {}) if isinstance(summary.get("counts"), dict) else {}
    critical = format_number(counts.get("critical", 0), 0)
    warning = format_number(counts.get("warning", 0), 0)
    normal = format_number(counts.get("normal", 0), 0)
    unevaluated = format_number(counts.get("unevaluated", 0), 0)
    return (
        f"{emphasize(f'高风险 {critical} 项')}，"
        f"{emphasize(f'中风险 {warning} 项')}，"
        f"{emphasize(f'正常 {normal} 项')}，"
        f"{emphasize(f'未评估 {unevaluated} 项')}"
    )


def risk_icon(level: str) -> str:
    return {"critical": "🔴", "warning": "🟡", "normal": "🟢"}.get(level, level)


def join_unique(values: list[str]) -> str:
    ordered: list[str] = []
    seen: set[str] = set()
    for value in values:
        if not value or value in seen:
            continue
        seen.add(value)
        ordered.append(value)
    return "；".join(ordered)


def percent_pair(label: str, value: Any) -> tuple[str, str]:
    return (label, format_percent(value))


def numeric_pair(label: str, value: Any, digits: int = 0) -> tuple[str, str]:
    return (label, format_number(value, digits))


def row_count_pair(label: str, payload: Any) -> tuple[str, str]:
    return numeric_pair(label, len(unwrap_items(payload)))


def top_event_name(payload: Any) -> str:
    items = unwrap_items(payload)
    if not items:
        return ""
    return str(row_value(items[0], "event_name", "event", "EVENT_NAME"))
