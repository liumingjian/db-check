"""Build chapter 1 content aligned to template-mysql.md."""

from __future__ import annotations

from typing import Any

from reporter.content.helpers import full_table, key_value_table
from reporter.content.mysql_summary_text import (
    business_dimensions,
    conclusion_paragraphs,
    display_dimension_name,
    group_abnormal_items,
    health_summary,
    impact_analysis,
    risk_description,
)
from reporter.model.report_view import SectionBlock

ALARM_DEFINITIONS = (
    ("高风险", "🔴", "可能影响业务连续性或数据安全，建议立即处理。", "24小时内"),
    ("中风险", "🟡", "存在明确风险或趋势性异常，建议尽快安排处理。", "1~2周内"),
    ("低风险", "🔵", "不符合最佳实践或存在优化空间，建议纳入优化计划。", "1~3个月内"),
    ("正常", "🟢", "未发现明显风险，当前运行状态正常。", "持续保持"),
)


def build_summary_section(result: dict[str, Any], summary: dict[str, Any], meta: dict[str, Any]) -> SectionBlock:
    return SectionBlock(
        title="第一章 巡检总结",
        children=(
            _build_alarm_definitions(),
            _build_scope_section(result, meta),
            _build_health_assessment(result, summary),
            _build_risk_findings(summary),
            _build_conclusion(summary),
        ),
    )


def _build_alarm_definitions() -> SectionBlock:
    table = full_table("巡检告警定义", ("风险等级", "标识", "定义", "建议响应时效"), ALARM_DEFINITIONS, status="derived")
    return SectionBlock(title="1.1 巡检告警定义", status="derived", tables=(table,))


def _build_scope_section(result: dict[str, Any], meta: dict[str, Any]) -> SectionBlock:
    scope = meta.get("scope", {}) if isinstance(meta.get("scope"), dict) else {}
    meta_info = result.get("meta", {}) if isinstance(result.get("meta"), dict) else {}
    rows = (
        ("巡检对象", str(scope.get("inspection_target", "待补充"))),
        ("实例清单", _join_instances(scope.get("instances"), meta_info)),
        ("数据库版本", str(scope.get("database_version", "待补充"))),
        ("架构角色", str(scope.get("architecture_role", "待补充"))),
    )
    table = key_value_table("巡检范围", rows, status="external")
    return SectionBlock(title="1.2 巡检范围", status="external", tables=(table,))


def _join_instances(instances: Any, meta_info: dict[str, Any]) -> str:
    if isinstance(instances, list) and instances:
        return "；".join(str(item) for item in instances)
    host = meta_info.get("db_host", "")
    port = meta_info.get("db_port", "")
    return f"{host}:{port}".strip(":")


def _build_health_assessment(result: dict[str, Any], summary: dict[str, Any]) -> SectionBlock:
    grouped = group_abnormal_items(summary)
    rows = tuple(_health_row(label, names, grouped, result) for label, names in business_dimensions())
    table = full_table("综合健康评估", ("检查维度", "风险", "关键发现"), rows, status="derived")
    return SectionBlock(title="1.3 综合健康评估", status="derived", tables=(table,))


def _health_row(label: str, dimension_names: set[str], grouped: dict[str, list[dict[str, Any]]], result: dict[str, Any]) -> tuple[str, str, str]:
    items: list[dict[str, Any]] = []
    for name in dimension_names:
        items.extend(grouped.get(display_dimension_name(name), []))
    level, finding = health_summary(label, result, items)
    risk_text = {"critical": "🔴 高风险", "warning": "🟡 中风险", "normal": "🟢 正常"}[level]
    return (label, risk_text, finding)


def _build_risk_findings(summary: dict[str, Any]) -> SectionBlock:
    rows = tuple(_risk_row(item) for item in _abnormal_items(summary))
    if not rows:
        rows = (("🟢 正常", "-", "未发现异常项", "-", "无需整改"),)
    table = full_table("风险发现与整改建议", ("风险等级", "检查维度", "风险描述", "影响分析", "整改建议"), rows, status="derived")
    note = "仅列出存在风险的检查项，正常项不在此表展示。"
    return SectionBlock(title="1.4 风险发现与整改建议", status="derived", tables=(table,), note=note)


def _abnormal_items(summary: dict[str, Any]) -> list[dict[str, Any]]:
    items = summary.get("abnormal_items", [])
    if not isinstance(items, list):
        return []
    return [item for item in items if isinstance(item, dict)]


def _risk_row(item: dict[str, Any]) -> tuple[str, str, str, str, str]:
    level = str(item.get("level", "warning"))
    return (
        {"critical": "🔴 高风险", "warning": "🟡 中风险", "normal": "🟢 正常"}.get(level, level),
        display_dimension_name(str(item.get("dimension_name", "-"))),
        risk_description(item),
        impact_analysis(item),
        str(item.get("advice", "-")),
    )


def _build_conclusion(summary: dict[str, Any]) -> SectionBlock:
    return SectionBlock(title="1.5 巡检结论", status="derived", paragraphs=conclusion_paragraphs(summary))
