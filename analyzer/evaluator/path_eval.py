"""Path extraction and threshold evaluation helpers."""

from __future__ import annotations

import math
from typing import Any


def _coerce_number(value: Any) -> float | None:
    if isinstance(value, dict):
        for key in ("max_value", "count", "value"):
            if key in value:
                return _coerce_number(value[key])
        return None
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def compare_values(operator: str, observed: Any, expected: Any) -> bool | None:
    op = operator.strip()
    observed_num = _coerce_number(observed)
    expected_num = _coerce_number(expected)
    if observed_num is not None and expected_num is not None:
        if op == ">":
            return observed_num > expected_num
        if op == ">=":
            return observed_num >= expected_num
        if op == "<":
            return observed_num < expected_num
        if op == "<=":
            return observed_num <= expected_num
        if op == "==":
            return observed_num == expected_num
        if op == "!=":
            return observed_num != expected_num
    if op == "==":
        return observed == expected
    if op == "!=":
        return observed != expected
    return None


def _parse_segment(segment: str) -> tuple[str, bool]:
    if segment.endswith("[*]"):
        return segment[:-3], True
    return segment, False


def extract_values(data: Any, json_path: str) -> list[Any]:
    current = [data]
    for segment in json_path.split("."):
        key, wildcard = _parse_segment(segment)
        next_values: list[Any] = []
        for node in current:
            if not isinstance(node, dict) or key not in node:
                continue
            value = node[key]
            if wildcard and isinstance(value, list):
                next_values.extend(value)
            if not wildcard:
                next_values.append(value)
        current = next_values
        if not current:
            break
    flattened: list[Any] = []
    for item in current:
        if isinstance(item, list):
            flattened.extend(item)
        else:
            flattened.append(item)
    return flattened


def aggregate(values: list[Any], mode: str) -> Any:
    if not values:
        return None
    normalized_mode = (mode or "raw").lower()
    if normalized_mode == "raw":
        return values[0] if len(values) == 1 else values
    nums = [_coerce_number(v) for v in values]
    numeric_values = [n for n in nums if n is not None]
    if not numeric_values:
        return None
    if normalized_mode == "avg":
        return sum(numeric_values) / len(numeric_values)
    if normalized_mode == "max":
        return max(numeric_values)
    if normalized_mode == "min":
        return min(numeric_values)
    if normalized_mode == "last":
        return numeric_values[-1]
    if normalized_mode == "p95":
        sorted_values = sorted(numeric_values)
        idx = max(0, min(len(sorted_values) - 1, int(math.ceil(0.95 * len(sorted_values))) - 1))
        return sorted_values[idx]
    if normalized_mode == "count":
        return len(values)
    if normalized_mode == "sum":
        return sum(numeric_values)
    return values[0] if len(values) == 1 else values


def _default_observed_level(observed: Any, row_count: int | None, reason: str) -> tuple[str, str]:
    if row_count is not None:
        return "normal", reason
    if observed is not None:
        return "normal", reason
    return "unevaluated", "threshold expression not auto-evaluable"


def _threshold_rule(thresholds: dict[str, Any], key: str) -> dict[str, Any] | None:
    rule = thresholds.get(key)
    if not isinstance(rule, dict):
        return None
    operator = str(rule.get("operator", "")).strip()
    if not operator:
        return None
    if "value" not in rule:
        return None
    return rule


def _apply_threshold(label: str, rule: dict[str, Any] | None, observed: Any) -> tuple[bool, bool, str]:
    if rule is None:
        return False, False, ""
    result = compare_values(str(rule.get("operator", "")), observed, rule.get("value"))
    if result is True:
        reason = f"{label} threshold hit: {rule.get('operator', '')} {rule.get('value')}"
        return True, True, reason
    if result is False:
        return False, True, ""
    return False, True, ""


def evaluate_thresholds(thresholds: dict[str, Any], observed: Any, row_count: int | None) -> tuple[str, str]:
    if not thresholds:
        return _default_observed_level(observed, row_count, "informational metric without machine threshold")
    critical_rule = _threshold_rule(thresholds, "critical")
    warning_rule = _threshold_rule(thresholds, "warning")
    normal_rule = _threshold_rule(thresholds, "normal")

    critical_hit, critical_defined, critical_reason = _apply_threshold("critical", critical_rule, observed)
    if critical_hit:
        return "critical", critical_reason
    warning_hit, warning_defined, warning_reason = _apply_threshold("warning", warning_rule, observed)
    if warning_hit:
        return "warning", warning_reason
    normal_hit, normal_defined, normal_reason = _apply_threshold("normal", normal_rule, observed)
    if normal_hit:
        return "normal", normal_reason
    if normal_defined and not critical_defined and not warning_defined:
        return "warning", "normal threshold not satisfied"
    if critical_defined or warning_defined:
        return "normal", "abnormal thresholds not matched"
    return _default_observed_level(observed, row_count, "informational metric without machine threshold")


def evaluate_check(check: dict[str, Any], observed: Any, row_count: int | None) -> tuple[str, str]:
    evaluation = check.get("evaluation", {}) if isinstance(check.get("evaluation"), dict) else {}
    method = str(evaluation.get("method") or "").strip().lower()
    thresholds = evaluation.get("thresholds") if isinstance(evaluation.get("thresholds"), dict) else None
    if not thresholds:
        thresholds = check.get("thresholds") if isinstance(check.get("thresholds"), dict) else {}
    if method in {"", "threshold"}:
        return evaluate_thresholds(thresholds, observed, row_count)
    if method == "exists":
        exists = (row_count or 0) > 0 if row_count is not None else observed is not None
        return ("warning", "exists check matched") if exists else ("normal", "exists check not matched")
    if method == "row_count":
        actual_rows = row_count if row_count is not None else (0 if observed is None else 1)
        if thresholds:
            return evaluate_thresholds(thresholds, actual_rows, actual_rows)
        return ("warning", "rows found") if actual_rows > 0 else ("normal", "no rows found")
    if method == "info":
        return "normal", "informational metric"
    if method == "gate":
        return "normal", "gate condition satisfied"
    if method == "custom":
        return "unevaluated", "custom evaluation is not supported in analyzer"
    return "unevaluated", f"unsupported evaluation method: {method}"
