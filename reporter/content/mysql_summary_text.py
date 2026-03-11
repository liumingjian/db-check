"""Narrative helpers for MySQL markdown reports."""

from __future__ import annotations

from collections import defaultdict
from typing import Any

from reporter.content.helpers import (
    first_item,
    format_duration_hours,
    format_number,
    format_percent,
    format_uptime,
    level_icon,
    level_text,
    nested_get,
    row_value,
    unwrap_items,
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

DIMENSION_LABELS = {
    name: label
    for label, names in BUSINESS_DIMENSIONS
    for name in names
}


def group_abnormal_items(summary: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
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


def business_dimensions() -> tuple[tuple[str, set[str]], ...]:
    return BUSINESS_DIMENSIONS


def display_dimension_name(name: str) -> str:
    return DIMENSION_LABELS.get(name, name)


def health_summary(label: str, result: dict[str, Any], items: list[dict[str, Any]]) -> tuple[str, str]:
    if items:
        return (_top_level(items), _item_summary(label, items))
    return ("normal", _default_summary(label, result))


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
        return "当前观测结果已触发高风险阈值，可能直接影响业务连续性或安全性。"
    if reason.startswith("warning threshold hit"):
        return "当前观测结果已触发预设告警阈值，说明该项存在明确风险或持续性异常。"
    if reason.startswith("exists check matched"):
        return "本次采集已发现对应异常记录，说明该类问题在当前实例中真实存在。"
    if level == "critical":
        return "当前问题已达到高风险等级，建议优先处置。"
    return "当前问题已在本次巡检中命中，建议结合业务影响尽快处理。"


def conclusion_paragraphs(summary: dict[str, Any]) -> tuple[str, str, str]:
    counts = summary.get("counts", {}) if isinstance(summary.get("counts"), dict) else {}
    abnormal_items = [item for item in summary.get("abnormal_items", []) if isinstance(item, dict)]
    total = counts.get("total_checks", 0)
    overall = str(summary.get("overall_risk", "low"))
    focus = focus_dimensions(abnormal_items)
    paragraph_1 = (
        f"本次巡检共检查 {total} 个检查项，其中正常 {counts.get('normal', 0)} 项，"
        f"中风险 {counts.get('warning', 0)} 项，高风险 {counts.get('critical', 0)} 项，"
        f"不适用 {counts.get('not_applicable', 0)} 项。"
    )
    paragraph_2 = _conclusion_sentence(overall, focus)
    paragraph_3 = f"未评估项 {counts.get('unevaluated', 0)}，当前报告基于真实采集结果与汇总结论生成。"
    return (paragraph_1, paragraph_2, paragraph_3)


def focus_dimensions(abnormal_items: list[dict[str, Any]]) -> str:
    counts: dict[str, int] = defaultdict(int)
    for item in abnormal_items:
        counts[display_dimension_name(str(item.get("dimension_name", "未分类")))] += 1
    if not counts:
        return "未发现突出风险维度"
    ordered = sorted(counts.items(), key=lambda item: (-item[1], item[0]))[:3]
    return "、".join(name for name, _ in ordered)


def _item_summary(label: str, items: list[dict[str, Any]]) -> str:
    if label == "操作系统资源":
        return _resource_summary(items)
    if label == "数据库可用性":
        return _availability_summary(items)
    if label == "数据库性能":
        return _performance_summary(items)
    if label == "安全配置":
        return _security_summary(items)
    if label == "参数配置":
        return _parameter_summary(items)
    if label == "对象与索引":
        return _object_summary(items)
    if label == "容量规划":
        return _capacity_summary(items)
    return "；".join(risk_description(item) for item in items[:2])


def _resource_summary(items: list[dict[str, Any]]) -> str:
    return _join_unique(risk_description(item) for item in items[:2])


def _availability_summary(items: list[dict[str, Any]]) -> str:
    return _join_unique(risk_description(item) for item in items[:2])


def _performance_summary(items: list[dict[str, Any]]) -> str:
    return _join_unique(risk_description(item) for item in items[:3])


def _security_summary(items: list[dict[str, Any]]) -> str:
    return _join_unique(risk_description(item) for item in items[:2])


def _parameter_summary(items: list[dict[str, Any]]) -> str:
    return _join_unique(risk_description(item) for item in items[:2])


def _object_summary(items: list[dict[str, Any]]) -> str:
    return _join_unique(risk_description(item) for item in items[:2])


def _capacity_summary(items: list[dict[str, Any]]) -> str:
    return _join_unique(risk_description(item) for item in items[:2])


def _default_summary(label: str, result: dict[str, Any]) -> str:
    cpu = first_item(nested_get(result, ("os", "cpu", "samples"), []), {}) or {}
    memory = first_item(nested_get(result, ("os", "memory", "samples"), []), {}) or {}
    perf = nested_get(result, ("db", "performance"), {})
    backup = nested_get(result, ("db", "backup"), {})
    storage = nested_get(result, ("db", "storage"), {})
    security = nested_get(result, ("db", "security"), {})
    no_index_count = len(unwrap_items(nested_get(result, ("db", "sql_analysis", "no_index_sqls"), {})))
    top_wait = first_item(unwrap_items(perf.get("top_wait_events")), {}) or {}
    backup_records = len(unwrap_items(backup.get("backup_size_trend")))
    password_policy = security.get("password_policy", {}) if isinstance(security.get("password_policy"), dict) else {}
    defaults = {
        "操作系统资源": f"CPU 使用率 {format_percent(cpu.get('usage_percent'))}，内存使用率 {format_percent(memory.get('usage_percent'))}，当前未发现明显资源瓶颈。",
        "数据库可用性": f"实例已运行 {format_uptime(nested_get(result, ('db', 'basic_info', 'uptime_seconds')))}，连接状态正常。",
        "数据库性能": _performance_default_summary(perf, no_index_count, top_wait),
        "安全配置": _security_default_summary(security, password_policy),
        "备份与恢复": f"备份策略已配置={format_number(backup.get('strategy_exists'))}，最近全备年龄 {format_duration_hours(backup.get('last_full_backup_age_hours'))}，备份记录 {backup_records} 条。",
        "参数配置": f"slow_query_log={format_number(nested_get(result, ('db', 'config_check', 'slow_query_log')))}，max_connections={format_number(nested_get(result, ('db', 'config_check', 'max_connections')))}。",
        "对象与索引": f"无主键表 {format_number(len(unwrap_items(storage.get('tables_without_pk'))))} 个，未使用索引 {format_number(len(unwrap_items(storage.get('unused_indexes'))))} 个。",
        "容量规划": f"数据库数量 {format_number(len(unwrap_items(storage.get('database_sizes'))))}，binlog 占用 {format_number(storage.get('binlog_disk_usage_bytes'))} 字节。",
    }
    return defaults.get(label, "未发现明显风险。")


def _top_level(items: list[dict[str, Any]]) -> str:
    return "critical" if any(item.get("level") == "critical" for item in items) else "warning"


def _conclusion_sentence(overall: str, focus: str) -> str:
    label = level_text(overall)
    if overall == "high":
        return f"综合风险等级为 {label}，报告已发现高风险项，风险主要集中在 {focus}，建议优先处理高风险和持续性异常。"
    if overall == "medium":
        return f"综合风险等级为 {label}，当前风险主要集中在 {focus}，建议尽快安排优化与整改。"
    return f"综合风险等级为 {label}，整体运行状态较稳定，当前关注点主要在 {focus}。"


def _join_unique(values: Any) -> str:
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        text = str(value)
        if text in seen:
            continue
        seen.add(text)
        ordered.append(text)
    return "；".join(ordered)


def _performance_default_summary(perf: dict[str, Any], no_index_count: int, top_wait: dict[str, Any]) -> str:
    event_name = row_value(top_wait, "EVENT_NAME", "event_name")
    event_part = f"Top 等待事件 {event_name}" if event_name else "未发现显著等待热点"
    return (
        f"慢查询数 {format_number(perf.get('slow_queries_count'))}，"
        f"线程缓存命中率 {format_percent(perf.get('thread_cache_hit_ratio'))}，"
        f"无索引 SQL {format_number(no_index_count)} 条，{event_part}。"
    )


def _security_default_summary(security: dict[str, Any], password_policy: dict[str, Any]) -> str:
    anonymous_count = len(unwrap_items(security.get("anonymous_users")))
    return (
        f"SSL 启用={format_number(security.get('ssl_enabled'))}，"
        f"root 远程登录={format_number(security.get('root_remote_login'))}，"
        f"密码策略启用={format_number(password_policy.get('enabled'))}，"
        f"匿名用户数 {format_number(anonymous_count)}。"
    )
