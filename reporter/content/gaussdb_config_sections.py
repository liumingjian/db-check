"""GaussDB configuration section builders."""

from __future__ import annotations

from typing import Any

from reporter.content.gaussdb_section_utils import details_dict, domain_table, find_item, visible_items
from reporter.content.helpers import full_table, key_value_table
from reporter.model.report_view import SectionBlock, TableBlock

IMPORTANT_PARAMS = {
    "archive_mode",
    "maintenance_work_mem",
    "max_connections",
    "max_locks_per_transaction",
    "max_prepared_transactions",
    "shared_buffers",
    "statement_timeout",
    "synchronous_commit",
    "work_mem",
}


def build_config_section(result: dict[str, Any]) -> SectionBlock | None:
    tables = _config_tables(result)
    if not tables:
        return None
    return SectionBlock(
        title="2.2.2 参数配置",
        paragraphs=("正文展示 SQL-first 关键参数校验结果；完整 pg_settings 快照和 SQL 原文保留在 run_dir/sql/ 目录。",),
        tables=tuple(tables),
    )


def _config_tables(result: dict[str, Any]) -> list[TableBlock]:
    tables: list[TableBlock] = []
    guc_value_item = find_item(result, "CheckGUCValue", "config_check")
    if guc_value_item is not None:
        tables.append(_guc_value_table(guc_value_item))
    db_params_item = find_item(result, "CheckDBParams", "config_check")
    if db_params_item is not None:
        tables.append(_important_params_table(db_params_item))
    items = visible_items(result, "config_check")
    if items:
        tables.append(domain_table("参数与配置结论", items))
    return tables


def _guc_value_table(item: dict[str, Any]) -> TableBlock:
    details = details_dict(item)
    rows = [("检查结论", str(item.get("summary") or ""))]
    for key, label in _guc_labels():
        value = details.get(key)
        if value not in ("", None):
            rows.append((label, str(value)))
    return key_value_table("参数值检查", tuple(rows))


def _guc_labels() -> tuple[tuple[str, str], ...]:
    return (
        ("max_connections", "最大连接数"),
        ("max_prepared_transactions", "最大预备事务数"),
        ("max_locks_per_transaction", "每事务最大锁数"),
        ("computed_value", "锁资源预算值"),
    )


def _important_params_table(item: dict[str, Any]) -> TableBlock:
    rows = tuple(_param_row(row) for row in _setting_rows(item) if _is_important_param(row))
    if not rows:
        rows = (("采集结果", str(item.get("summary") or "")),)
    return full_table("核心参数快照", ("参数", "当前值"), rows)


def _setting_rows(item: dict[str, Any]) -> tuple[dict[str, Any], ...]:
    rows = details_dict(item).get("rows")
    if not isinstance(rows, list):
        return ()
    return tuple(row for row in rows if isinstance(row, dict))


def _is_important_param(row: dict[str, Any]) -> bool:
    return str(row.get("name") or "") in IMPORTANT_PARAMS


def _param_row(row: dict[str, Any]) -> tuple[str, str]:
    return (str(row.get("name") or ""), str(row.get("setting") or ""))
