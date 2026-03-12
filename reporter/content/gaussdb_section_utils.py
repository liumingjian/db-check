"""Helpers for GaussDB report content."""

from __future__ import annotations

from typing import Any

from reporter.content.helpers import full_table, nested_get
from reporter.model.report_view import TableBlock


def domain_payload(result: dict[str, Any], key: str) -> dict[str, Any]:
    payload = result.get("db", {}).get(key, {})
    return payload if isinstance(payload, dict) else {}


def domain_summary(result: dict[str, Any], key: str) -> dict[str, Any]:
    payload = domain_payload(result, key).get("summary", {})
    return payload if isinstance(payload, dict) else {}


def visible_items(result: dict[str, Any], key: str) -> tuple[dict[str, Any], ...]:
    summary = domain_summary(result, key)
    items = summary.get("visible_items", [])
    if isinstance(items, list):
        return tuple(item for item in items if isinstance(item, dict))
    payload_items = domain_payload(result, key).get("items", [])
    if isinstance(payload_items, list):
        return tuple(item for item in payload_items if isinstance(item, dict) and item.get("normalized_status") != "not_applicable")
    return ()


def find_item(result: dict[str, Any], item_name: str, *domains: str) -> dict[str, Any] | None:
    search_domains = domains or tuple(
        key
        for key in ("basic_info", "cluster", "config_check", "connection", "storage", "performance", "transactions", "sql_analysis", "security")
    )
    for domain in search_domains:
        for item in visible_items(result, domain):
            if str(item.get("item") or "") == item_name:
                return item
    return None


def details_dict(item: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(item, dict):
        return {}
    details = item.get("details", {})
    return details if isinstance(details, dict) else {}


def rows_payload(result: dict[str, Any], domain: str, key: str) -> dict[str, Any]:
    payload = domain_payload(result, domain).get(key, {})
    return payload if isinstance(payload, dict) else {}


def status_label(status: str) -> str:
    return {
        "normal": "正常",
        "abnormal": "异常",
        "not_applicable": "不适用",
        "unknown": "未知",
    }.get(status, status or "未知")


def item_rows(items: tuple[dict[str, Any], ...]) -> tuple[tuple[str, str, str], ...]:
    rows = []
    for item in items:
        rows.append(
            (
                str(item.get("label") or item.get("item") or ""),
                status_label(str(item.get("normalized_status") or "")),
                str(item.get("summary") or ""),
            )
        )
    return tuple(rows)


def domain_table(title: str, items: tuple[dict[str, Any], ...], note: str = "") -> TableBlock:
    return full_table(title, ("检查项", "状态", "摘要"), item_rows(items), note=note)


def basic_info_rows(result: dict[str, Any]) -> tuple[tuple[str, str], ...]:
    summary = domain_summary(result, "basic_info")
    meta = result.get("meta", {}) if isinstance(result.get("meta"), dict) else {}
    rows = (
        ("数据库类型", str(meta.get("db_type", ""))),
        ("数据库地址", f"{meta.get('db_host', '')}:{meta.get('db_port', '')}".strip(":")),
        ("数据库名", str(meta.get("db_name", ""))),
        ("GaussDB 版本", str(summary.get("version") or summary.get("gaussdb_version") or "")),
        ("gsql 版本", str(summary.get("gsql_version", ""))),
        ("gs_check 版本", str(summary.get("gs_check_version", ""))),
        ("执行用户", str(summary.get("gauss_user", ""))),
        ("环境文件", str(summary.get("gauss_env_file", ""))),
        ("GAUSSHOME", str(summary.get("gausshome", ""))),
        ("GAUSSLOG", str(summary.get("gausslog", ""))),
        ("PGUSER", str(summary.get("pguser", ""))),
        ("PGHOST", str(summary.get("pghost", ""))),
        ("集群标识", str(summary.get("gs_cluster_name", ""))),
    )
    return tuple((label, value) for label, value in rows if value)


def system_hostname(result: dict[str, Any]) -> str:
    return str(nested_get(result, ("os", "system_info", "hostname"), ""))
