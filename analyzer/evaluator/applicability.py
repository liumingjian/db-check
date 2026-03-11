"""Applicability and gate helpers for summary generation."""

from __future__ import annotations

from typing import Any

from analyzer.evaluator.path_eval import aggregate, compare_values, extract_values


def gate_na_map(checks: list[dict[str, Any]], result: dict[str, Any]) -> dict[str, str]:
    gated: dict[str, str] = {}
    for check in checks:
        reason = gate_failure_reason(check, result)
        if reason is None:
            continue
        for target in _gate_targets(checks, check):
            gated[target] = reason
    return gated


def direct_na_reason(check: dict[str, Any], result: dict[str, Any]) -> str | None:
    sample_reason = sample_requirement_reason(check, result)
    if sample_reason is not None:
        return sample_reason
    condition = evaluation_meta(check).get("na_when")
    return condition_reason(result, condition)


def gate_failure_reason(check: dict[str, Any], result: dict[str, Any]) -> str | None:
    meta = evaluation_meta(check)
    if str(meta.get("method") or "").strip().lower() != "gate":
        return None
    condition = meta.get("gate")
    if not isinstance(condition, dict):
        return "gate definition is missing"
    return None if condition_matches(result, condition) else _reason_text(condition, "gate condition not met")


def sample_requirement_reason(check: dict[str, Any], result: dict[str, Any]) -> str | None:
    config = result.get("collect_config") if isinstance(result.get("collect_config"), dict) else {}
    sample_mode = str(config.get("sample_mode") or "single")
    expected_samples = _int_value(config.get("expected_samples"), 1)
    meta = evaluation_meta(check)
    required_mode = str(meta.get("sample_mode_required") or "").strip()
    if required_mode and required_mode != sample_mode:
        return f"requires sample_mode={required_mode}, actual={sample_mode}"
    minimum = _int_value(meta.get("min_expected_samples"), 0)
    if minimum > 0 and expected_samples < minimum:
        return f"requires expected_samples>={minimum}, actual={expected_samples}"
    return None


def evaluation_meta(check: dict[str, Any]) -> dict[str, Any]:
    return check.get("evaluation", {}) if isinstance(check.get("evaluation"), dict) else {}


def condition_reason(result: dict[str, Any], condition: Any) -> str | None:
    if not isinstance(condition, dict):
        return None
    return _reason_text(condition, "not applicable condition matched") if condition_matches(result, condition) else None


def condition_matches(result: dict[str, Any], condition: dict[str, Any]) -> bool:
    observed = _condition_observed(result, condition)
    if observed is _MISSING:
        return False
    matched = compare_values(str(condition.get("operator") or "").strip(), observed, condition.get("value"))
    return matched is True


def _condition_observed(result: dict[str, Any], condition: dict[str, Any]) -> Any:
    json_path = str(condition.get("json_path") or "").strip()
    if not json_path:
        return _MISSING
    extracted = extract_values(result, json_path)
    if not extracted:
        return _MISSING
    return aggregate(extracted, str(condition.get("aggregation") or "raw"))


def _gate_targets(checks: list[dict[str, Any]], check: dict[str, Any]) -> list[str]:
    meta = evaluation_meta(check)
    if isinstance(meta.get("na_check_ids"), list):
        return [str(item) for item in meta["na_check_ids"]]
    dimension_id = str(meta.get("na_dimension") or check.get("dimension_id") or "")
    if not dimension_id:
        return [check["check_id"]]
    return [item["check_id"] for item in checks if str(item.get("dimension_id") or "") == dimension_id]


def _reason_text(condition: dict[str, Any], fallback: str) -> str:
    text = str(condition.get("reason") or "").strip()
    return text or fallback


def _int_value(value: Any, default: int) -> int:
    try:
        return int(value)
    except Exception:  # noqa: BLE001
        return default


_MISSING = object()
