"""Narrative helpers for MySQL summary sections."""

from __future__ import annotations

from typing import Any

from reporter.content.helpers import (
    first_item,
    format_number,
    format_percent,
    format_uptime,
    level_text,
    nested_get,
    row_value,
    unwrap_items,
)
from reporter.content.summary_text_common import (
    emphasize,
    focus_dimensions as common_focus_dimensions,
    group_abnormal_items as common_group_abnormal_items,
    impact_analysis as common_impact_analysis,
    join_unique,
    make_dimension_labels,
    metric_text,
    numeric_pair,
    overall_risk_label,
    percent_pair,
    risk_description as common_risk_description,
    risk_stat_text,
    row_count_pair,
    top_event_name,
    top_level,
)

BUSINESS_DIMENSIONS = (
    ("操作系统资源", {"操作系统资源"}),
    ("数据库可用性", {"高可用与复制", "数据库可用性"}),
    ("数据库性能", {"数据库性能", "锁与并发"}),
    ("安全配置", {"安全与审计"}),
    ("备份与恢复", {"备份与恢复"}),
    ("参数配置", {"参数配置"}),
    ("对象与索引", {"对象与索引"}),
    ("容量规划", {"存储与空间管理", "容量规划"}),
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
        return (top_level(items), _item_summary(label, items))
    return ("normal", _default_summary(label, result))


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
    total_text = emphasize(f"{counts.get('total_checks', 0)} 个检查项")
    critical_text = emphasize(f"高风险 {counts.get('critical', 0)} 项")
    warning_text = emphasize(f"中风险 {counts.get('warning', 0)} 项")
    normal_text = emphasize(f"正常 {counts.get('normal', 0)} 项")
    paragraph_1 = (
        f"本次巡检共检查 {total_text}，"
        f"其中 {critical_text}、{warning_text}、{normal_text}。"
    )
    paragraph_2 = _conclusion_sentence(str(summary.get("overall_risk", "low")), focus)
    paragraph_3 = (
        f"关键指标方面，{_key_metrics_text(result)}。"
        "本结论基于本次真实采集窗口内的数据库与系统观测结果生成。"
    )
    return (paragraph_1, paragraph_2, paragraph_3)


def focus_dimensions(abnormal_items: list[dict[str, Any]]) -> str:
    return common_focus_dimensions(abnormal_items, display_dimension_name)


def _item_summary(label: str, items: list[dict[str, Any]]) -> str:
    limits = {
        "操作系统资源": 2,
        "数据库可用性": 2,
        "数据库性能": 3,
        "安全配置": 2,
        "参数配置": 2,
        "对象与索引": 2,
        "容量规划": 2,
    }
    size = limits.get(label, 2)
    return join_unique([risk_description(item) for item in items[:size]])


def _default_summary(label: str, result: dict[str, Any]) -> str:
    cpu = first_item(nested_get(result, ("os", "cpu", "samples"), []), {}) or {}
    memory = first_item(nested_get(result, ("os", "memory", "samples"), []), {}) or {}
    perf = nested_get(result, ("db", "performance"), {})
    replication = nested_get(result, ("db", "replication"), {})
    config = nested_get(result, ("db", "config_check"), {})
    storage = nested_get(result, ("db", "storage"), {})
    security = nested_get(result, ("db", "security"), {})
    backup = nested_get(result, ("db", "backup_recovery"), {})
    sql_analysis = nested_get(result, ("db", "sql_analysis"), {})
    defaults = {
        "操作系统资源": (
            f"CPU 使用率 {format_percent(cpu.get('usage_percent'))}，"
            f"内存使用率 {format_percent(memory.get('usage_percent'))}，当前未发现明显资源瓶颈。"
        ),
        "数据库可用性": (
            f"实例已运行 {format_uptime(nested_get(result, ('db', 'basic_info', 'instance_uptime_seconds')))}，"
            f"复制延迟 {format_number(replication.get('seconds_behind_master'), 0)} 秒。"
        ),
        "数据库性能": (
            f"QPS {format_number(perf.get('qps'))}，Threads Running {format_number(perf.get('threads_running'), 0)}，"
            f"Top 等待事件 {top_event_name(perf.get('top_wait_events')) or '未发现显著等待热点'}。"
        ),
        "安全配置": (
            f"高权限账号 {format_number(len(unwrap_items(security.get('high_privilege_accounts'))), 0)} 个，"
            f"密码策略启用={format_number(nested_get(security, ('password_policy', 'enabled')))}。"
        ),
        "备份与恢复": (
            f"最近备份记录 {format_number(len(unwrap_items(backup.get('latest_backup_jobs'))), 0)} 条，"
            f"恢复演练记录 {format_number(len(unwrap_items(backup.get('restore_drill'))), 0)} 条。"
        ),
        "参数配置": (
            f"slow_query_log={format_number(config.get('slow_query_log'))}，"
            f"max_connections={format_number(config.get('max_connections'))}。"
        ),
        "对象与索引": (
            f"热点表 {format_number(len(unwrap_items(storage.get('table_size_top10'))), 0)} 个，"
            f"热点索引 {format_number(len(unwrap_items(storage.get('hot_indexes_top10'))), 0)} 个。"
        ),
        "容量规划": (
            f"总容量 {format_number(storage.get('total_size_gb'))} GB，"
            f"Top 表数量 {format_number(len(unwrap_items(storage.get('top_tables'))), 0)} 个。"
        ),
    }
    return defaults.get(label, "未发现明显风险。")


def _conclusion_sentence(overall: str, focus: str) -> str:
    label = overall_risk_label(overall)
    if overall == "high":
        return f"综合风险等级为 {emphasize(label)}，风险主要集中在 {emphasize(focus)}，建议优先处理高风险项并建立整改闭环。"
    if overall == "medium":
        return f"综合风险等级为 {emphasize(label)}，当前薄弱点主要集中在 {emphasize(focus)}，建议尽快安排优化与整改。"
    return f"综合风险等级为 {emphasize(label)}，整体运行状态较稳定，当前建议持续关注 {emphasize(focus)} 相关指标。"


def _key_metrics_text(result: dict[str, Any]) -> str:
    cpu = first_item(nested_get(result, ("os", "cpu", "samples"), []), {}) or {}
    memory = first_item(nested_get(result, ("os", "memory", "samples"), []), {}) or {}
    perf = nested_get(result, ("db", "performance"), {})
    replication = nested_get(result, ("db", "replication"), {})
    backup = nested_get(result, ("db", "backup_recovery"), {})
    sql_analysis = nested_get(result, ("db", "sql_analysis"), {})
    return metric_text(
        (
            percent_pair("CPU 使用率", cpu.get("usage_percent")),
            percent_pair("内存使用率", memory.get("usage_percent")),
            numeric_pair("QPS", perf.get("qps"), 2),
            numeric_pair("Threads Running", perf.get("threads_running"), 0),
            row_count_pair("慢 SQL 条数", sql_analysis.get("slow_sql_top")),
            numeric_pair("复制延迟", replication.get("seconds_behind_master"), 0),
            row_count_pair("最近备份记录", backup.get("latest_backup_jobs")),
        )
    )


def _recommendation(summary: dict[str, Any], focus: str) -> str:
    overall = str(summary.get("overall_risk", "low"))
    if overall == "high":
        return f"建议围绕 {emphasize(focus)} 立即开展高风险项整改，并优先保障业务连续性与数据安全。"
    if overall == "medium":
        return f"建议优先围绕 {emphasize(focus)} 制定分阶段优化计划，并跟踪整改效果。"
    return f"建议保持现有基线配置，同时持续跟踪 {emphasize(focus)} 相关指标变化。"
