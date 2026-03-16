"""Rule merge helpers (base rule + extension rule)."""

from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any, cast


class RuleMergeError(ValueError):
    pass


EFFECTIVE_RULE_NAME = "rule.effective.json"


def write_effective_rule(*, run_dir: Path, base_rule: Path, extension_rule: Path) -> Path:
    merged = merge_rules(_load_rule(base_rule, "base"), _load_rule(extension_rule, "extension"))
    out = run_dir / EFFECTIVE_RULE_NAME
    out.write_text(json.dumps(merged, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return out


def merge_rules(base_rule: dict[str, Any], extension_rule: dict[str, Any]) -> dict[str, Any]:
    base_dimensions = _dimensions(base_rule, ctx="base")
    ext_dimensions = _dimensions(extension_rule, ctx="extension")
    base_ids = _collect_check_ids(base_dimensions)
    ext_ids = _collect_check_ids(ext_dimensions)

    dup = sorted(base_ids & ext_ids)
    if dup:
        raise RuleMergeError(f"duplicate check_id found when merging rules: {', '.join(dup)}")

    merged = copy.deepcopy(base_rule)
    merged_dimensions = _dimensions(merged, ctx="merged")
    for ext_dim in ext_dimensions:
        dim_id = str(ext_dim.get("dimension_id") or "")
        if not dim_id:
            raise RuleMergeError("extension dimension missing dimension_id")
        target = _find_dimension(merged_dimensions, dim_id)
        if target is None:
            raise RuleMergeError(f"base rule missing target dimension_id={dim_id}")
        ext_checks = _checks(ext_dim, ctx=f"extension dimension {dim_id}")
        target_checks = _checks(target, ctx=f"merged dimension {dim_id}")
        target_checks.extend(copy.deepcopy(ext_checks))

    return merged


def _load_rule(path: Path, label: str) -> dict[str, Any]:
    if not path.exists() or not path.is_file():
        raise RuleMergeError(f"{label} rule file not found: {path}")
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:  # noqa: BLE001
        raise RuleMergeError(f"{label} rule parse failed: {exc}") from exc
    if not isinstance(obj, dict):
        raise RuleMergeError(f"{label} rule root must be object")
    return obj


def _dimensions(rule: dict[str, Any], *, ctx: str) -> list[dict[str, Any]]:
    dims = rule.get("dimensions")
    if not isinstance(dims, list):
        raise RuleMergeError(f"{ctx} rule dimensions must be array")
    out: list[dict[str, Any]] = []
    for idx, item in enumerate(dims):
        if not isinstance(item, dict):
            raise RuleMergeError(f"{ctx} rule dimensions[{idx}] must be object")
        out.append(item)
    return out


def _checks(dimension: dict[str, Any], *, ctx: str) -> list[dict[str, Any]]:
    checks = dimension.get("checks")
    if not isinstance(checks, list):
        raise RuleMergeError(f"{ctx} checks must be array")
    for idx, item in enumerate(checks):
        if not isinstance(item, dict):
            raise RuleMergeError(f"{ctx} checks[{idx}] must be object")
        if not str(item.get("check_id") or ""):
            raise RuleMergeError(f"{ctx} checks[{idx}] missing check_id")
    return cast(list[dict[str, Any]], checks)


def _collect_check_ids(dimensions: list[dict[str, Any]]) -> set[str]:
    out: set[str] = set()
    for dim in dimensions:
        for check in _checks(dim, ctx=f"dimension {dim.get('dimension_id')}"):
            out.add(str(check.get("check_id") or ""))
    return out


def _find_dimension(dimensions: list[dict[str, Any]], dim_id: str) -> dict[str, Any] | None:
    for dim in dimensions:
        if str(dim.get("dimension_id") or "") == dim_id:
            return dim
    return None
