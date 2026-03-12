"""Shared chapter-1 report builder."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from reporter.content.helpers import full_table, key_value_table
from reporter.model.report_view import SectionBlock

ALARM_DEFINITIONS = (
    ("高风险", "🔴", "可能影响业务连续性、可用性或数据安全，建议立即处理。", "24小时内"),
    ("中风险", "🟡", "存在明确风险或趋势性异常，建议尽快安排处理。", "1~2周内"),
    ("低风险", "🔵", "不符合最佳实践或存在优化空间，建议纳入优化计划。", "1~3个月内"),
    ("正常", "🟢", "未发现明显风险，当前运行状态总体可控。", "持续保持"),
)


@dataclass(frozen=True)
class SummaryStrategy:
    business_dimensions: Callable[[], tuple[tuple[str, set[str]], ...]]
    display_dimension_name: Callable[[str], str]
    group_abnormal_items: Callable[[dict[str, Any]], dict[str, list[dict[str, Any]]]]
    health_summary: Callable[[str, dict[str, Any], list[dict[str, Any]]], tuple[str, str]]
    risk_description: Callable[[dict[str, Any]], str]
    impact_analysis: Callable[[dict[str, Any]], str]
    conclusion_rows: Callable[[dict[str, Any], dict[str, Any]], tuple[tuple[str, str], ...]]
    conclusion_paragraphs: Callable[[dict[str, Any], dict[str, Any]], tuple[str, ...]]


def build_summary_section(
    result: dict[str, Any],
    summary: dict[str, Any],
    meta: dict[str, Any],
    strategy: SummaryStrategy,
) -> SectionBlock:
    return SectionBlock(
        title="第一章 巡检总结",
        children=(
            _build_alarm_definitions(),
            _build_scope_section(result, meta),
            _build_health_assessment(result, summary, strategy),
            _build_risk_findings(summary, strategy),
            _build_conclusion(result, summary, strategy),
        ),
    )


def _build_alarm_definitions() -> SectionBlock:
    table = full_table("巡检告警定义", ("风险等级", "风险标识", "定义", "建议响应时效"), ALARM_DEFINITIONS, status="derived")
    return SectionBlock(title="1.1 巡检告警定义", status="derived", tables=(table,))


def _build_scope_section(result: dict[str, Any], meta: dict[str, Any]) -> SectionBlock:
    scope = meta.get("scope", {}) if isinstance(meta.get("scope"), dict) else {}
    meta_info = result.get("meta", {}) if isinstance(result.get("meta"), dict) else {}
    rows = (
        ("巡检对象", str(scope.get("inspection_target", "待补充"))),
        ("实例清单", _join_instances(scope.get("instances"), meta_info)),
        ("数据库版本", str(scope.get("database_version", "待补充"))),
        ("架构角色", str(scope.get("architecture_role", "待补充"))),
        ("数据目录", str(scope.get("data_dir", "待补充"))),
    )
    return SectionBlock(title="1.2 巡检范围", status="external", tables=(key_value_table("巡检范围", rows, status="external"),))


def _join_instances(instances: Any, meta_info: dict[str, Any]) -> str:
    if isinstance(instances, list) and instances:
        return "；".join(str(item) for item in instances)
    host = str(meta_info.get("db_host", ""))
    port = meta_info.get("db_port", "")
    return f"{host}:{port}".strip(":")


def _build_health_assessment(
    result: dict[str, Any],
    summary: dict[str, Any],
    strategy: SummaryStrategy,
) -> SectionBlock:
    grouped = strategy.group_abnormal_items(summary)
    rows = tuple(_health_row(label, names, grouped, result, strategy) for label, names in strategy.business_dimensions())
    table = full_table("综合健康评估", ("检查维度", "风险标识", "关键发现"), rows, status="derived")
    return SectionBlock(title="1.3 综合健康评估", status="derived", tables=(table,))


def _health_row(
    label: str,
    dimension_names: set[str],
    grouped: dict[str, list[dict[str, Any]]],
    result: dict[str, Any],
    strategy: SummaryStrategy,
) -> tuple[str, str, str]:
    items: list[dict[str, Any]] = []
    for name in dimension_names:
        items.extend(grouped.get(strategy.display_dimension_name(name), []))
    level, finding = strategy.health_summary(label, result, items)
    return (label, _risk_icon(level), finding)


def _risk_icon(level: str) -> str:
    return {"critical": "🔴", "warning": "🟡", "normal": "🟢"}[level]


def _build_risk_findings(summary: dict[str, Any], strategy: SummaryStrategy) -> SectionBlock:
    rows = tuple(_risk_row(item, strategy) for item in _abnormal_items(summary))
    if not rows:
        rows = (("🟢", "-", "未发现异常项", "-", "无需整改"),)
    table = full_table("风险发现与整改建议", ("风险标识", "检查维度", "风险描述", "影响分析", "整改建议"), rows, status="derived")
    note = "仅列出存在风险的检查项，正常项不在此表展示。"
    return SectionBlock(title="1.4 风险发现与整改建议", status="derived", tables=(table,), note=note)


def _abnormal_items(summary: dict[str, Any]) -> list[dict[str, Any]]:
    items = summary.get("abnormal_items", [])
    if not isinstance(items, list):
        return []
    return [item for item in items if isinstance(item, dict)]


def _risk_row(item: dict[str, Any], strategy: SummaryStrategy) -> tuple[str, str, str, str, str]:
    level = str(item.get("level", "warning"))
    return (
        {"critical": "🔴", "warning": "🟡", "normal": "🟢"}.get(level, level),
        strategy.display_dimension_name(str(item.get("dimension_name", "-"))),
        strategy.risk_description(item),
        strategy.impact_analysis(item),
        str(item.get("advice", "-")),
    )


def _build_conclusion(
    result: dict[str, Any],
    summary: dict[str, Any],
    strategy: SummaryStrategy,
) -> SectionBlock:
    return SectionBlock(
        title="1.5 巡检结论",
        status="derived",
        tables=(key_value_table("巡检结论摘要", strategy.conclusion_rows(result, summary), status="derived"),),
        paragraphs=strategy.conclusion_paragraphs(result, summary),
    )
