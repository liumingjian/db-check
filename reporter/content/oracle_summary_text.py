"""Narrative helpers for Oracle summary sections."""

from __future__ import annotations

from typing import Any

from reporter.content.helpers import (
    first_item,
    format_number,
    format_percent,
    level_text,
    nested_get,
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
    top_level,
)

BUSINESS_DIMENSIONS = (
    ("操作系统资源", {"操作系统资源"}),
    ("数据库可用性", {"实例与架构基础"}),
    ("数据库性能", {"性能与 SQL"}),
    ("安全配置", {"安全与权限"}),
    ("备份与恢复", {"备份与可恢复性"}),
    ("参数配置", set()),
    ("对象与索引", set()),
    ("容量规划", {"存储与表空间"}),
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
        return (top_level(items), join_unique([risk_description(item) for item in items[:3]]))
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
        "本结论基于本次真实采集窗口内的 Oracle 数据库与系统观测结果生成。"
    )
    return (paragraph_1, paragraph_2, paragraph_3)


def focus_dimensions(abnormal_items: list[dict[str, Any]]) -> str:
    return common_focus_dimensions(abnormal_items, display_dimension_name)


def _default_summary(label: str, result: dict[str, Any]) -> str:
    cpu = first_item(nested_get(result, ("os", "cpu", "samples"), []), {}) or {}
    memory = first_item(nested_get(result, ("os", "memory", "samples"), []), {}) or {}
    basic = nested_get(result, ("db", "basic_info"), {})
    config = nested_get(result, ("db", "config_check"), {})
    storage = nested_get(result, ("db", "storage"), {})
    perf = nested_get(result, ("db", "performance"), {})
    backup = nested_get(result, ("db", "backup"), {})
    security = nested_get(result, ("db", "security"), {})
    defaults = {
        "操作系统资源": (
            f"CPU 使用率 {format_percent(cpu.get('usage_percent'))}，"
            f"内存使用率 {format_percent(memory.get('usage_percent'))}，当前未发现明显资源瓶颈。"
        ),
        "数据库可用性": (
            f"实例 {basic.get('instance_name', '')} 运行正常，"
            f"日志模式 {basic.get('log_mode', '')}，RAC={'是' if basic.get('is_rac') else '否'}。"
        ),
        "数据库性能": (
            f"活跃会话 {format_number(_sum_active_sessions(perf), 0)} 个，"
            f"Redo Nowait {format_percent(perf.get('redo_nowait_pct'))}。"
        ),
        "安全配置": (
            f"高权限账号 {format_number(len(unwrap_items(security.get('dba_role_users'))), 0)} 个，"
            f"过期账号 {format_number(len(unwrap_items(security.get('expired_users'))), 0)} 个。"
        ),
        "备份与恢复": (
            f"归档模式 {backup.get('archive_log_mode', '')}，"
            f"恢复区使用率 {format_percent(_first_recovery_area_pct(backup))}。"
        ),
        "参数配置": (
            f"SPFILE={config.get('spfile', '')}，"
            f"SGA Target={format_number(config.get('sga_target_mb'))} MB。"
        ),
        "对象与索引": (
            f"无效对象 {format_number(len(unwrap_items(storage.get('invalid_objects'))), 0)} 个，"
            f"不可用索引 {format_number(len(unwrap_items(storage.get('invalid_indexes'))), 0)} 个。"
        ),
        "容量规划": (
            f"表空间最高使用率 {format_percent(_max_tablespace_pct(storage))}，"
            f"UNDO 使用率 {format_percent(_max_undo_pct(perf))}。"
        ),
    }
    return defaults.get(label, "未发现明显风险。")


def _conclusion_sentence(overall: str, focus: str) -> str:
    label = overall_risk_label(overall)
    if overall == "high":
        return f"综合风险等级为 {emphasize(label)}，当前风险主要集中在 {emphasize(focus)}，建议优先处理可恢复性、阻塞与容量相关高风险项。"
    if overall == "medium":
        return f"综合风险等级为 {emphasize(label)}，当前薄弱点主要集中在 {emphasize(focus)}，建议尽快开展性能与配置优化。"
    return f"综合风险等级为 {emphasize(label)}，实例整体运行可控，建议持续跟踪 {emphasize(focus)} 相关指标。"


def _key_metrics_text(result: dict[str, Any]) -> str:
    cpu = first_item(nested_get(result, ("os", "cpu", "samples"), []), {}) or {}
    memory = first_item(nested_get(result, ("os", "memory", "samples"), []), {}) or {}
    perf = nested_get(result, ("db", "performance"), {})
    backup = nested_get(result, ("db", "backup"), {})
    sql = nested_get(result, ("db", "sql_analysis"), {})
    storage = nested_get(result, ("db", "storage"), {})
    return metric_text(
        (
            percent_pair("CPU 使用率", cpu.get("usage_percent")),
            percent_pair("内存使用率", memory.get("usage_percent")),
            numeric_pair("活跃会话数", _sum_active_sessions(perf), 0),
            percent_pair("表空间最高使用率", _max_tablespace_pct(storage)),
            percent_pair("恢复区使用率", _first_recovery_area_pct(backup)),
            percent_pair("Redo Nowait", perf.get("redo_nowait_pct")),
            row_count_pair("高解析SQL", sql.get("high_parse_count_sql")),
        )
    )


def _recommendation(summary: dict[str, Any], focus: str) -> str:
    overall = str(summary.get("overall_risk", "low"))
    if overall == "high":
        return f"建议围绕 {emphasize(focus)} 立即开展高风险项整改，并优先保障数据库可恢复性与业务连续性。"
    if overall == "medium":
        return f"建议优先围绕 {emphasize(focus)} 制定分阶段优化计划，并关注性能和容量趋势变化。"
    return f"建议保持现有基线配置，同时持续跟踪 {emphasize(focus)} 相关指标变化。"


def _sum_active_sessions(perf: dict[str, Any]) -> int:
    return sum(int(item.get("active_sessions", 0)) for item in unwrap_items(perf.get("active_sessions")))


def _max_tablespace_pct(storage: dict[str, Any]) -> Any:
    values = [item.get("real_percent") for item in unwrap_items(storage.get("tablespace_usage")) if item.get("real_percent") not in (None, "")]
    return max(values) if values else ""


def _max_undo_pct(perf: dict[str, Any]) -> Any:
    values = [item.get("usage_percent") for item in unwrap_items(perf.get("undo_tablespace_usage")) if item.get("usage_percent") not in (None, "")]
    return max(values) if values else ""


def _first_recovery_area_pct(backup: dict[str, Any]) -> Any:
    items = unwrap_items(backup.get("recovery_area"))
    if not items:
        return ""
    return items[0].get("space_used_pct")
