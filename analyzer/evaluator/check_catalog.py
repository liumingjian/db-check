"""Rule check catalog helpers."""

from __future__ import annotations

from typing import Any


def parse_check_id(value: str) -> tuple[int, int]:
    try:
        major, minor = value.split(".", 1)
        return int(major), int(minor)
    except Exception:  # noqa: BLE001
        return 9999, 9999


def module_for_check_id(check_id: str) -> str:
    major, _ = parse_check_id(check_id)
    if major == 1:
        return "os"
    if major == 2:
        return "db_replication"
    if major in {3, 7}:
        return "db_basic"
    if major == 4:
        return "db_perf"
    if major == 5:
        return "db_security"
    if major == 6:
        return "db_backup"
    if major in {8, 9, 10}:
        return "db_storage"
    if major == 11:
        return "db_lock"
    return "unknown"


def collect_checks(rule: dict[str, Any]) -> list[dict[str, Any]]:
    checks: list[dict[str, Any]] = []
    for dimension in rule.get("dimensions", []):
        dimension_id = str(dimension.get("dimension_id") or dimension.get("id") or "")
        dimension_name = dimension.get("name", "")
        for check in dimension.get("checks", []):
            check_id = str(check.get("check_id") or check.get("id") or "")
            if not check_id:
                continue
            checks.append(
                {
                    "check_id": check_id,
                    "name": check.get("name", ""),
                    "dimension_id": dimension_id,
                    "dimension_name": dimension_name,
                    "extract": check.get("extract", {}) if isinstance(check.get("extract"), dict) else {},
                    "thresholds": check.get("thresholds", {}) if isinstance(check.get("thresholds"), dict) else {},
                    "evaluation": check.get("evaluation", {}) if isinstance(check.get("evaluation"), dict) else {},
                    "source_module": str(check.get("source_module") or "").strip(),
                    "optimization_advice": check.get("optimization_advice", ""),
                }
            )
    checks.sort(key=lambda item: parse_check_id(item["check_id"]))
    return checks
