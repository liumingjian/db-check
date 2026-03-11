"""Summary generation orchestrator."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from analyzer.evaluator.applicability import direct_na_reason, gate_na_map
from analyzer.evaluator.check_catalog import collect_checks, module_for_check_id
from analyzer.evaluator.path_eval import aggregate, evaluate_check, extract_values


def _failure_meta(exit_code: int) -> tuple[str, str]:
    if exit_code == 20:
        return "collector_failed", "collector exited with 20: collection failed"
    return "precheck_failed", "collector exited with 30: argument/connectivity/permission precheck failed"


def _new_counts(total_checks: int) -> dict[str, int]:
    return {
        "total_checks": total_checks,
        "normal": 0,
        "warning": 0,
        "critical": 0,
        "unevaluated": 0,
        "not_applicable": 0,
    }


def generate_summary(
    manifest: dict[str, Any],
    result: dict[str, Any],
    rule: dict[str, Any],
    na_check_ids: set[str] | None = None,
) -> dict[str, Any]:
    checks = collect_checks(rule)
    counts = _new_counts(len(checks))
    summary_base = _summary_base(manifest, rule)
    gated_na = gate_na_map(checks, result)
    exit_code = manifest.get("exit_code")
    if isinstance(exit_code, int) and exit_code in {20, 30}:
        return _failure_summary(summary_base, checks, counts, exit_code)

    module_stats = manifest.get("module_stats", {}) if isinstance(manifest.get("module_stats"), dict) else {}
    abnormal_items, unevaluated_items, na_items = _evaluate_checks(
        checks,
        result,
        module_stats,
        na_check_ids or set(),
        gated_na,
        counts,
    )
    _finalize_counts(counts, unevaluated_items, na_items)

    summary = dict(summary_base)
    summary["overall_risk"] = _risk_from_counts(counts)
    summary["counts"] = counts
    summary["abnormal_items"] = abnormal_items
    summary["unevaluated_items"] = unevaluated_items
    summary["na_items"] = na_items
    return summary


def _summary_base(manifest: dict[str, Any], rule: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": "1.0",
        "run_id": manifest.get("run_id", ""),
        "rule_version": str(rule.get("rule_meta", {}).get("rule_version", "1.0")),
        "generated_at": datetime.now().astimezone().isoformat(),
    }


def _failure_summary(
    summary_base: dict[str, Any],
    checks: list[dict[str, Any]],
    counts: dict[str, int],
    exit_code: int,
) -> dict[str, Any]:
    reason_type, message = _failure_meta(exit_code)
    unevaluated_items = [
        {
            "check_id": check["check_id"],
            "reason_type": "failed",
            "reason": message,
            "source_module": check["source_module"] or module_for_check_id(check["check_id"]),
        }
        for check in checks
    ]
    counts["unevaluated"] = len(unevaluated_items)
    counts["total_checks"] = len(unevaluated_items)
    summary = dict(summary_base)
    summary["overall_risk"] = "high"
    summary["counts"] = counts
    summary["abnormal_items"] = []
    summary["unevaluated_items"] = unevaluated_items
    summary["na_items"] = []
    summary["failure"] = {"exit_code": exit_code, "reason_type": reason_type, "message": message}
    return summary


def _evaluate_checks(
    checks: list[dict[str, Any]],
    result: dict[str, Any],
    module_stats: dict[str, Any],
    na_ids: set[str],
    gated_na: dict[str, str],
    counts: dict[str, int],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    abnormal_items: list[dict[str, Any]] = []
    unevaluated_items: list[dict[str, Any]] = []
    na_items: list[dict[str, Any]] = []
    for check in checks:
        _evaluate_single_check(
            check,
            result,
            module_stats,
            na_ids,
            gated_na,
            counts,
            abnormal_items,
            unevaluated_items,
            na_items,
        )
    return abnormal_items, unevaluated_items, na_items


def _evaluate_single_check(
    check: dict[str, Any],
    result: dict[str, Any],
    module_stats: dict[str, Any],
    na_ids: set[str],
    gated_na: dict[str, str],
    counts: dict[str, int],
    abnormal_items: list[dict[str, Any]],
    unevaluated_items: list[dict[str, Any]],
    na_items: list[dict[str, Any]],
) -> None:
    check_id = check["check_id"]
    module = check["source_module"] or module_for_check_id(check_id)
    module_status = module_stats.get(module, {}) if isinstance(module_stats.get(module), dict) else {}
    if check_id in gated_na:
        na_items.append({"check_id": check_id, "reason_type": "not_applicable", "reason": gated_na[check_id]})
        return
    if check_id in na_ids:
        na_items.append({"check_id": check_id, "reason_type": "not_applicable", "reason": f"marked by na checks ({module})"})
        return
    reason = direct_na_reason(check, result)
    if reason is not None:
        na_items.append({"check_id": check_id, "reason_type": "not_applicable", "reason": reason})
        return
    if _append_module_unavailable(check_id, module_status, module, unevaluated_items):
        return
    observed, reason = _extract_observed_value(check, result)
    if reason is not None:
        unevaluated_items.append({"check_id": check_id, "reason_type": "failed", "reason": reason, "source_module": module})
        return
    row_count = _derive_row_count(observed)
    level, reason = evaluate_check(check, observed, row_count)
    _append_evaluation(check, module, observed, level, reason, counts, abnormal_items, unevaluated_items)


def _derive_row_count(observed: Any) -> int | None:
    if isinstance(observed, list):
        return len(observed)
    if not isinstance(observed, dict):
        return None
    items = observed.get("items")
    if isinstance(items, list):
        return len(items)
    return None


def _append_module_unavailable(
    check_id: str,
    module_status: dict[str, Any],
    module: str,
    unevaluated_items: list[dict[str, Any]],
) -> bool:
    if module_status.get("status") not in {"failed", "skipped"}:
        return False
    reason_type = "failed" if module_status.get("status") == "failed" else "skipped"
    reason = str(module_status.get("error") or "module unavailable")
    unevaluated_items.append({"check_id": check_id, "reason_type": reason_type, "reason": reason, "source_module": module})
    return True


def _extract_observed_value(check: dict[str, Any], result: dict[str, Any]) -> tuple[Any, str | None]:
    json_path = str(check["extract"].get("json_path") or "").strip()
    if not json_path:
        return None, "missing extract.json_path"
    extracted = extract_values(result, json_path)
    if not extracted:
        return None, f"no data extracted from path: {json_path}"
    observed = aggregate(extracted, str(check["extract"].get("aggregation") or "raw"))
    return observed, None


def _append_evaluation(
    check: dict[str, Any],
    module: str,
    observed: Any,
    level: str,
    reason: str,
    counts: dict[str, int],
    abnormal_items: list[dict[str, Any]],
    unevaluated_items: list[dict[str, Any]],
) -> None:
    if level in {"critical", "warning"}:
        abnormal_items.append(
            {
                "check_id": check["check_id"],
                "name": check["name"],
                "dimension_id": check["dimension_id"],
                "dimension_name": check["dimension_name"],
                "level": level,
                "current_value": observed,
                "reason": reason,
                "advice": check.get("optimization_advice", ""),
            }
        )
    if level == "critical":
        counts["critical"] += 1
    elif level == "warning":
        counts["warning"] += 1
    elif level == "normal":
        counts["normal"] += 1
    else:
        unevaluated_items.append({"check_id": check["check_id"], "reason_type": "failed", "reason": reason, "source_module": module})


def _finalize_counts(counts: dict[str, int], unevaluated_items: list[dict[str, Any]], na_items: list[dict[str, Any]]) -> None:
    counts["unevaluated"] = len(unevaluated_items)
    counts["not_applicable"] = len(na_items)
    counts["total_checks"] = counts["normal"] + counts["warning"] + counts["critical"] + counts["unevaluated"] + counts["not_applicable"]


def _risk_from_counts(counts: dict[str, int]) -> str:
    if counts["critical"] > 0:
        return "high"
    if counts["warning"] > 0:
        return "medium"
    return "low"
