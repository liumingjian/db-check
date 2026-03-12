"""Narrative helpers for GaussDB summary sections."""

from __future__ import annotations

from typing import Any

from reporter.content.helpers import first_item, format_percent, nested_get
from reporter.content.summary_text_common import (
    emphasize,
    focus_dimensions as common_focus_dimensions,
    group_abnormal_items as common_group_abnormal_items,
    impact_analysis as common_impact_analysis,
    join_unique,
    make_dimension_labels,
    metric_text,
    overall_risk_label,
    percent_pair,
    risk_description as common_risk_description,
    risk_stat_text,
    top_level,
)

BUSINESS_DIMENSIONS = (
    ("操作系统资源", {"操作系统资源"}),
    ("数据库可用性", {"基础连通性", "集群同步与复制"}),
    ("参数配置", {"参数与配置一致性"}),
    ("连接与会话", {"连接与会话"}),
    ("容量规划", {"存储与容量"}),
    ("事务与锁", {"事务与锁"}),
    ("数据库性能", {"运行性能"}),
)

DIMENSION_LABELS = make_dimension_labels(BUSINESS_DIMENSIONS)


def group_abnormal_items(summary: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    return common_group_abnormal_items(summary, display_dimension_name)


def business_dimensions() -> tuple[tuple[str, set[str]], ...]:
    return BUSINESS_DIMENSIONS


def display_dimension_name(name: str) -> str:
    return DIMENSION_LABELS.get(name, name)


def health_summary(label: str, result: dict[str, Any], items: list[dict[str, Any]]) -> tuple[str, str]:
    if items:
        return top_level(items), join_unique([risk_description(item) for item in items[:3]])
    return "normal", _default_summary(label, result)


def risk_description(item: dict[str, Any]) -> str:
    return common_risk_description(item)


def impact_analysis(item: dict[str, Any]) -> str:
    return common_impact_analysis(item)


def conclusion_rows(result: dict[str, Any], summary: dict[str, Any]) -> tuple[tuple[str, str], ...]:
    abnormal_items = [item for item in summary.get("abnormal_items", []) if isinstance(item, dict)]
    focus = focus_dimensions(abnormal_items)
    return (
        ("综合风险等级", emphasize(overall_risk_label(str(summary.get("overall_risk", "low"))))),
        ("风险项统计", risk_stat_text(summary)),
        ("重点关注维度", emphasize(focus)),
        ("关键指标摘要", _key_metrics_text(result)),
        ("结论建议", _recommendation(summary, focus)),
    )


def conclusion_paragraphs(result: dict[str, Any], summary: dict[str, Any]) -> tuple[str, str, str]:
    counts = summary.get("counts", {}) if isinstance(summary.get("counts"), dict) else {}
    abnormal_items = [item for item in summary.get("abnormal_items", []) if isinstance(item, dict)]
    focus = focus_dimensions(abnormal_items)
    paragraph_1 = (
        f"本次巡检共检查 {emphasize(f'{counts.get('total_checks', 0)} 个检查项')}，"
        f"其中 {emphasize(f'高风险 {counts.get('critical', 0)} 项')}、"
        f"{emphasize(f'中风险 {counts.get('warning', 0)} 项')}、"
        f"{emphasize(f'正常 {counts.get('normal', 0)} 项')}。"
    )
    paragraph_2 = _conclusion_sentence(str(summary.get("overall_risk", "low")), focus)
    paragraph_3 = (
        f"关键指标方面，{_key_metrics_text(result)}。"
        "本结论基于本次真实采集窗口内的 GaussDB 与系统观测结果生成。"
    )
    return paragraph_1, paragraph_2, paragraph_3


def focus_dimensions(abnormal_items: list[dict[str, Any]]) -> str:
    return common_focus_dimensions(abnormal_items, display_dimension_name)


def _default_summary(label: str, result: dict[str, Any]) -> str:
    cpu = first_item(nested_get(result, ("os", "cpu", "samples"), []), {}) or {}
    memory = first_item(nested_get(result, ("os", "memory", "samples"), []), {}) or {}
    db = result.get("db", {}) if isinstance(result.get("db"), dict) else {}
    defaults = {
        "操作系统资源": (
            f"CPU 使用率 {format_percent(cpu.get('usage_percent'))}，"
            f"内存使用率 {format_percent(memory.get('usage_percent'))}，当前未发现明显资源瓶颈。"
        ),
        "数据库可用性": f"基础与集群检查当前未发现明显异常，数据库版本 {db.get('basic_info', {}).get('summary', {}).get('version', '待补充')}。",
        "参数配置": f"参数类检查异常 {db.get('config_check', {}).get('summary', {}).get('abnormal_count', 0)} 项。",
        "连接与会话": f"连接与会话类检查异常 {db.get('connection', {}).get('summary', {}).get('abnormal_count', 0)} 项。",
        "容量规划": f"存储与容量类检查异常 {db.get('storage', {}).get('summary', {}).get('abnormal_count', 0)} 项。",
        "事务与锁": f"事务与锁类检查异常 {db.get('transactions', {}).get('summary', {}).get('abnormal_count', 0)} 项。",
        "数据库性能": f"运行性能类检查异常 {db.get('performance', {}).get('summary', {}).get('abnormal_count', 0)} 项。",
    }
    return defaults.get(label, "未发现明显风险。")


def _conclusion_sentence(overall: str, focus: str) -> str:
    label = overall_risk_label(overall)
    if overall == "high":
        return f"综合风险等级为 {emphasize(label)}，当前风险主要集中在 {emphasize(focus)}，建议优先处置连接可用性、容量和锁等待问题。"
    if overall == "medium":
        return f"综合风险等级为 {emphasize(label)}，当前薄弱点主要集中在 {emphasize(focus)}，建议尽快完成参数、会话和性能优化。"
    return f"综合风险等级为 {emphasize(label)}，整体运行状态可控，建议持续跟踪 {emphasize(focus)} 相关指标。"


def _key_metrics_text(result: dict[str, Any]) -> str:
    cpu = first_item(nested_get(result, ("os", "cpu", "samples"), []), {}) or {}
    memory = first_item(nested_get(result, ("os", "memory", "samples"), []), {}) or {}
    db = result.get("db", {}) if isinstance(result.get("db"), dict) else {}
    return metric_text(
        (
            percent_pair("CPU 使用率", cpu.get("usage_percent")),
            percent_pair("内存使用率", memory.get("usage_percent")),
            ("基础异常项", str(db.get("basic_info", {}).get("summary", {}).get("abnormal_count", 0))),
            ("参数异常项", str(db.get("config_check", {}).get("summary", {}).get("abnormal_count", 0))),
            ("连接异常项", str(db.get("connection", {}).get("summary", {}).get("abnormal_count", 0))),
            ("存储异常项", str(db.get("storage", {}).get("summary", {}).get("abnormal_count", 0))),
            ("事务异常项", str(db.get("transactions", {}).get("summary", {}).get("abnormal_count", 0))),
        )
    )


def _recommendation(summary: dict[str, Any], focus: str) -> str:
    overall = str(summary.get("overall_risk", "low"))
    if overall == "high":
        return f"建议围绕 {emphasize(focus)} 立即开展高风险项整改，并优先保障数据库可用性和业务连续性。"
    if overall == "medium":
        return f"建议优先围绕 {emphasize(focus)} 制定分阶段优化计划，并持续跟踪整改效果。"
    return f"建议保持现有基线配置，同时持续跟踪 {emphasize(focus)} 相关指标变化。"
