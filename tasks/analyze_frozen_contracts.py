#!/usr/bin/env python3
"""MVP analyzer for frozen contracts with strict missing-semantics mapping."""

from __future__ import annotations

import argparse
import json
import math
from datetime import datetime
from pathlib import Path
from typing import Any


def _load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _coerce_number(v: Any) -> float | None:
    if isinstance(v, (int, float)) and not isinstance(v, bool):
        return float(v)
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return None
        try:
            return float(s)
        except ValueError:
            # extract leading numeric for loose compatibility
            buf = []
            seen_digit = False
            dot_used = False
            sign_used = False
            for ch in s:
                if ch.isdigit():
                    buf.append(ch)
                    seen_digit = True
                elif ch == "." and seen_digit and not dot_used:
                    buf.append(ch)
                    dot_used = True
                elif ch in "+-" and not sign_used and not buf:
                    buf.append(ch)
                    sign_used = True
                elif ch == " ":
                    if buf:
                        break
                else:
                    if buf:
                        break
            try:
                return float("".join(buf)) if buf else None
            except ValueError:
                return None
    return None


def _compare(op: str, observed: Any, expected: Any) -> bool | None:
    op = (op or "").strip()
    if not op:
        return None
    o_num = _coerce_number(observed)
    e_num = _coerce_number(expected)
    if o_num is not None and e_num is not None:
        if op == ">":
            return o_num > e_num
        if op == ">=":
            return o_num >= e_num
        if op == "<":
            return o_num < e_num
        if op == "<=":
            return o_num <= e_num
        if op == "==":
            return o_num == e_num
        if op == "!=":
            return o_num != e_num
    if op == "==":
        return observed == expected
    if op == "!=":
        return observed != expected
    return None


def _parse_segment(seg: str) -> tuple[str, bool]:
    if seg.endswith("[*]"):
        return seg[:-3], True
    return seg, False


def extract_values(data: Any, json_path: str) -> list[Any]:
    current = [data]
    for raw_seg in json_path.split("."):
        key, wildcard = _parse_segment(raw_seg)
        nxt: list[Any] = []
        for node in current:
            if not isinstance(node, dict) or key not in node:
                continue
            value = node[key]
            if wildcard:
                if isinstance(value, list):
                    nxt.extend(value)
            else:
                nxt.append(value)
        current = nxt
        if not current:
            break
    out: list[Any] = []
    for item in current:
        if isinstance(item, list):
            out.extend(item)
        else:
            out.append(item)
    return out


def aggregate(values: list[Any], mode: str) -> Any:
    if not values:
        return None
    mode = (mode or "raw").lower()
    if mode == "raw":
        return values[0] if len(values) == 1 else values
    nums = [_coerce_number(v) for v in values]
    nums = [n for n in nums if n is not None]
    if not nums:
        return None
    if mode == "avg":
        return sum(nums) / len(nums)
    if mode == "max":
        return max(nums)
    if mode == "min":
        return min(nums)
    if mode == "last":
        return nums[-1]
    if mode == "p95":
        s = sorted(nums)
        idx = int(math.ceil(0.95 * len(s))) - 1
        idx = max(0, min(idx, len(s) - 1))
        return s[idx]
    return values[0] if len(values) == 1 else values


def evaluate_thresholds(thresholds: dict[str, Any], observed: Any, row_count: int | None = None) -> tuple[str, str]:
    if not thresholds:
        return "unevaluated", "missing thresholds"

    c = thresholds.get("critical")
    if c and _compare(str(c.get("operator", "")), observed, c.get("value")) is True:
        return "critical", f"critical threshold hit: {c.get('operator', '')} {c.get('value')}"

    w = thresholds.get("warning")
    if w and _compare(str(w.get("operator", "")), observed, w.get("value")) is True:
        return "warning", f"warning threshold hit: {w.get('operator', '')} {w.get('value')}"

    n = thresholds.get("normal")
    if n:
        n_cmp = _compare(str(n.get("operator", "")), observed, n.get("value"))
        if n_cmp is True:
            return "normal", f"normal threshold matched: {n.get('operator', '')} {n.get('value')}"
        if n_cmp is False and not c and not w:
            return "warning", "normal threshold not satisfied"

    # fallback for row-based checks
    if row_count is not None:
        return ("warning", "rows found") if row_count > 0 else ("normal", "no rows found")

    return "unevaluated", "threshold expression not auto-evaluable"


def evaluate_check(check: dict[str, Any], observed: Any, row_count: int | None = None) -> tuple[str, str]:
    evaluation = check.get("evaluation", {})
    method = ""
    if isinstance(evaluation, dict):
        method = str(evaluation.get("method") or "").strip().lower()

    eval_thresholds = evaluation.get("thresholds") if isinstance(evaluation, dict) else None
    if isinstance(eval_thresholds, dict) and eval_thresholds:
        thresholds = eval_thresholds
    else:
        raw_thresholds = check.get("thresholds", {})
        thresholds = raw_thresholds if isinstance(raw_thresholds, dict) else {}

    if method in {"", "threshold"}:
        return evaluate_thresholds(thresholds, observed, row_count=row_count)

    if method == "exists":
        exists = (row_count or 0) > 0 if row_count is not None else observed is not None
        return ("warning", "exists check matched") if exists else ("normal", "exists check not matched")

    if method == "row_count":
        if row_count is None:
            row_count = 0 if observed is None else 1
        if thresholds:
            return evaluate_thresholds(thresholds, row_count, row_count=row_count)
        return ("warning", "rows found") if row_count > 0 else ("normal", "no rows found")

    if method == "custom":
        return "unevaluated", "custom evaluation is not supported in MVP analyzer"

    return "unevaluated", f"unsupported evaluation method: {method}"


def parse_check_id(value: str) -> tuple[int, int]:
    try:
        major, minor = value.split(".", 1)
        return int(major), int(minor)
    except Exception:
        return (9999, 9999)


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


def load_na_ids(path: Path | None) -> set[str]:
    if path is None:
        return set()
    data = _load_json(path)
    if not isinstance(data, list):
        raise ValueError("--na-checks must be a JSON array")
    out: set[str] = set()
    for item in data:
        if isinstance(item, str) and item.strip():
            out.add(item.strip())
    return out


def collect_checks(rule: dict[str, Any]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for dim in rule.get("dimensions", []):
        did = str(dim.get("dimension_id") or dim.get("id") or "")
        dname = dim.get("name", "")
        for chk in dim.get("checks", []):
            cid = str(chk.get("check_id") or chk.get("id") or "")
            if not cid:
                continue
            out.append(
                {
                    "check_id": cid,
                    "name": chk.get("name", ""),
                    "dimension_id": did,
                    "dimension_name": dname,
                    "extract": chk.get("extract", {}) if isinstance(chk.get("extract"), dict) else {},
                    "thresholds": chk.get("thresholds", {}) if isinstance(chk.get("thresholds"), dict) else {},
                    "evaluation": chk.get("evaluation", {}) if isinstance(chk.get("evaluation"), dict) else {},
                    "source_module": str(chk.get("source_module") or "").strip(),
                    "optimization_advice": chk.get("optimization_advice", ""),
                }
            )
    out.sort(key=lambda x: parse_check_id(x["check_id"]))
    return out


def failure_meta(exit_code: int) -> tuple[str, str]:
    if exit_code == 20:
        return ("collector_failed", "collector exited with 20: collection failed")
    return ("precheck_failed", "collector exited with 30: argument/connectivity/permission precheck failed")


def main() -> int:
    parser = argparse.ArgumentParser(description="Analyze result with frozen missing-semantics mapping")
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--result", type=Path, required=True)
    parser.add_argument("--rule", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument(
        "--na-checks",
        type=Path,
        default=None,
        help="optional JSON array of check_id explicitly marked as not_applicable",
    )
    args = parser.parse_args()

    manifest = _load_json(args.manifest)
    result = _load_json(args.result)
    rule = _load_json(args.rule)

    if not isinstance(manifest, dict) or not isinstance(result, dict) or not isinstance(rule, dict):
        raise SystemExit("manifest/result/rule must be JSON objects")

    na_ids = load_na_ids(args.na_checks)
    checks = collect_checks(rule)

    module_stats = manifest.get("module_stats", {})
    if not isinstance(module_stats, dict):
        module_stats = {}

    abnormal_items: list[dict[str, Any]] = []
    unevaluated_items: list[dict[str, Any]] = []
    na_items: list[dict[str, Any]] = []
    counts = {
        "total_checks": len(checks),
        "normal": 0,
        "warning": 0,
        "critical": 0,
        "unevaluated": 0,
        "not_applicable": 0,
    }
    exit_code = manifest.get("exit_code")
    failure: dict[str, Any] | None = None

    if isinstance(exit_code, int) and exit_code in {20, 30}:
        reason_type, message = failure_meta(exit_code)
        failure = {
            "exit_code": exit_code,
            "reason_type": reason_type,
            "message": message,
        }
        for chk in checks:
            cid = chk["check_id"]
            module = chk["source_module"] or module_for_check_id(cid)
            unevaluated_items.append(
                {
                    "check_id": cid,
                    "reason_type": "failed",
                    "reason": message,
                    "source_module": module,
                }
            )
        counts["unevaluated"] = len(unevaluated_items)
        counts["total_checks"] = len(unevaluated_items)
        out = {
            "schema_version": "1.0",
            "run_id": manifest.get("run_id", ""),
            "rule_version": str(rule.get("rule_meta", {}).get("rule_version", "1.0")),
            "generated_at": datetime.now().astimezone().isoformat(),
            "overall_risk": "high",
            "counts": counts,
            "abnormal_items": [],
            "unevaluated_items": unevaluated_items,
            "na_items": [],
            "failure": failure,
        }
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"generated: {args.out}")
        return 0

    for chk in checks:
        cid = chk["check_id"]
        name = chk["name"]
        module = chk["source_module"] or module_for_check_id(cid)
        mod_stat = module_stats.get(module, {"status": "success", "error": None})
        if not isinstance(mod_stat, dict):
            mod_stat = {"status": "success", "error": None}
        mod_status = mod_stat.get("status", "success")
        mod_error = mod_stat.get("error")

        if cid in na_ids:
            counts["not_applicable"] += 1
            na_items.append(
                {
                    "check_id": cid,
                    "reason_type": "not_applicable",
                    "reason": f"marked not_applicable by input list ({module})",
                }
            )
            continue

        if mod_status in {"failed", "skipped"}:
            reason_type = "failed" if mod_status == "failed" else "skipped"
            counts["unevaluated"] += 1
            unevaluated_items.append(
                {
                    "check_id": cid,
                    "reason_type": reason_type,
                    "reason": str(mod_error or f"module {module} status={mod_status}"),
                    "source_module": module,
                }
            )
            continue

        json_path = str(chk["extract"].get("json_path") or "").strip()
        method = str(chk.get("evaluation", {}).get("method") or "").strip().lower()
        if method == "custom":
            counts["unevaluated"] += 1
            unevaluated_items.append(
                {
                    "check_id": cid,
                    "reason_type": "failed",
                    "reason": "custom evaluation is not supported in MVP analyzer",
                    "source_module": module,
                }
            )
            continue
        agg = str(chk["extract"].get("aggregation") or "raw")
        observed: Any = None
        row_count: int | None = None
        if json_path:
            vals = extract_values(result, json_path)
            if not vals:
                if mod_status == "partial":
                    counts["unevaluated"] += 1
                    unevaluated_items.append(
                        {
                            "check_id": cid,
                            "reason_type": "failed",
                            "reason": str(mod_error or f"module {module} partial and data missing"),
                            "source_module": module,
                        }
                    )
                    continue
                # treat non-partial missing as unevaluated to avoid silent false normal
                counts["unevaluated"] += 1
                unevaluated_items.append(
                    {
                        "check_id": cid,
                        "reason_type": "failed",
                        "reason": f"no data extracted from path: {json_path}",
                        "source_module": module,
                    }
                )
                continue
            observed = aggregate(vals, agg)
            if isinstance(observed, list):
                row_count = len(observed)
        else:
            counts["unevaluated"] += 1
            unevaluated_items.append(
                {
                    "check_id": cid,
                    "reason_type": "failed",
                    "reason": "missing extract.json_path in rule",
                    "source_module": module,
                }
            )
            continue

        level, reason = evaluate_check(chk, observed, row_count=row_count)
        if level == "critical":
            counts["critical"] += 1
            abnormal_items.append(
                {
                    "check_id": cid,
                    "name": name,
                    "dimension_id": chk["dimension_id"],
                    "dimension_name": chk["dimension_name"],
                    "level": "critical",
                    "current_value": observed,
                    "reason": reason,
                    "advice": chk.get("optimization_advice", ""),
                }
            )
        elif level == "warning":
            counts["warning"] += 1
            abnormal_items.append(
                {
                    "check_id": cid,
                    "name": name,
                    "dimension_id": chk["dimension_id"],
                    "dimension_name": chk["dimension_name"],
                    "level": "warning",
                    "current_value": observed,
                    "reason": reason,
                    "advice": chk.get("optimization_advice", ""),
                }
            )
        elif level == "unevaluated":
            counts["unevaluated"] += 1
            unevaluated_items.append(
                {
                    "check_id": cid,
                    "reason_type": "failed",
                    "reason": reason,
                    "source_module": module,
                }
            )
        else:
            counts["normal"] += 1

    # hard count sync
    counts["unevaluated"] = len(unevaluated_items)
    counts["not_applicable"] = len(na_items)
    counts["total_checks"] = (
        counts["normal"] + counts["warning"] + counts["critical"] + counts["unevaluated"] + counts["not_applicable"]
    )

    overall_risk = "high" if counts["critical"] > 0 else ("medium" if counts["warning"] > 0 else "low")
    out = {
        "schema_version": "1.0",
        "run_id": manifest.get("run_id", ""),
        "rule_version": str(rule.get("rule_meta", {}).get("rule_version", "1.0")),
        "generated_at": datetime.now().astimezone().isoformat(),
        "overall_risk": overall_risk,
        "counts": counts,
        "abnormal_items": abnormal_items,
        "unevaluated_items": unevaluated_items,
        "na_items": na_items,
    }
    if failure is not None:
        out["failure"] = failure

    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"generated: {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
