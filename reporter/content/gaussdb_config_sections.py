"""GaussDB configuration section builders."""

from __future__ import annotations

from typing import Any

from reporter.content.gaussdb_section_utils import details_dict, domain_summary, domain_table, find_item, visible_items
from reporter.content.helpers import full_table, key_value_table
from reporter.model.report_view import SectionBlock, TableBlock


def build_config_section(result: dict[str, Any]) -> SectionBlock | None:
    tables: list[TableBlock] = []
    guc_value_item = find_item(result, "CheckGUCValue", "config_check")
    if guc_value_item is not None:
        tables.extend(_build_guc_value_tables(guc_value_item))
    summary = domain_summary(result, "config_check")
    guc_details = summary.get("checkgucconsistent_details")
    if isinstance(guc_details, dict) and guc_details:
        tables.extend(build_guc_consistency_tables(guc_details))
    items = visible_items(result, "config_check")
    if items:
        tables.append(domain_table("参数与配置结论", items))
    if not tables:
        return None
    return SectionBlock(
        title="2.2.2 参数与配置",
        paragraphs=("正文仅展示关键参数校验结果、关键参数分组和差异项，完整 GUC 快照保留在 result.json 与 gs_check 原始输出中。",),
        tables=tuple(tables),
    )


def _build_guc_value_tables(item: dict[str, Any]) -> list[TableBlock]:
    details = details_dict(item)
    rows = [("检查结论", str(item.get("summary") or ""))]
    mapping = (
        ("max_connections", "最大连接数"),
        ("max_prepared_transactions", "最大预备事务数"),
        ("max_locks_per_transaction", "每事务最大锁数"),
        ("computed_value", "锁资源预算值"),
    )
    for key, label in mapping:
        value = details.get(key)
        if value not in ("", None):
            rows.append((label, str(value)))
    if details.get("configuration_reasonable") is True:
        rows.append(("分析建议", "guc 参数配置合理"))
    return [key_value_table("参数值检查", tuple(rows))]


def build_guc_consistency_tables(details: dict[str, Any]) -> list[TableBlock]:
    tables = [_summary_table(details)]
    groups = details.get("key_groups")
    if isinstance(groups, list):
        for group in groups:
            if isinstance(group, dict):
                table = _group_table(group)
                if table is not None:
                    tables.append(table)
    differences = details.get("key_inconsistencies")
    if isinstance(differences, list) and differences:
        tables.append(_difference_table(differences))
    return tables


def _summary_table(details: dict[str, Any]) -> TableBlock:
    inconsistent = int(details.get("key_inconsistent_parameter_count") or 0)
    conclusion = "关键参数一致性正常" if inconsistent == 0 else f"发现 {inconsistent} 个关键参数存在差异"
    rows = (
        ("实例数量", str(details.get("instance_count") or 0)),
        ("参数总数", str(details.get("parameter_count") or 0)),
        ("关键参数分组数", str(details.get("key_parameter_group_count") or 0)),
        ("关键差异参数数", str(inconsistent)),
        ("结论", conclusion),
    )
    return key_value_table("参数一致性摘要", rows)


def _group_table(group: dict[str, Any]) -> TableBlock | None:
    parameters = group.get("parameters")
    if not isinstance(parameters, list) or not parameters:
        return None
    rows = []
    for parameter in parameters:
        if not isinstance(parameter, dict):
            continue
        rows.append(
            (
                str(parameter.get("label") or parameter.get("parameter") or ""),
                str(parameter.get("representative_value") or ""),
                _consistency_label(parameter.get("consistent")),
                _instance_values_text(parameter.get("instance_values")),
            )
        )
    if not rows:
        return None
    return full_table(str(group.get("title") or "关键参数"), ("参数", "当前值", "一致性", "实例值"), tuple(rows))


def _difference_table(differences: list[Any]) -> TableBlock:
    rows = []
    for item in differences:
        if not isinstance(item, dict):
            continue
        rows.append(
            (
                str(item.get("label") or item.get("parameter") or ""),
                _instance_values_text(item.get("instance_values")),
                str(item.get("distinct_value_count") or ""),
            )
        )
    return full_table("参数差异明细", ("参数", "差异实例值", "差异值数"), tuple(rows))


def _instance_values_text(values: Any) -> str:
    if not isinstance(values, list):
        return ""
    parts = []
    for item in values:
        if not isinstance(item, dict):
            continue
        instance = str(item.get("instance") or "")
        value = str(item.get("value") or "")
        if instance and value:
            parts.append(f"{instance}={value}")
    return "\n".join(parts)


def _consistency_label(value: Any) -> str:
    if value is True:
        return "一致"
    if value is False:
        return "存在差异"
    return ""
