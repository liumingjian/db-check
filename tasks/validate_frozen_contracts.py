#!/usr/bin/env python3
"""Validate frozen contracts with built-in checks and optional JSON Schema validation."""

from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any


RFC3339_Z_RE = re.compile(r"Z$")
UNIT_STRING_RE = re.compile(
    r"^\s*[-+]?\d+(?:\.\d+)?\s*(ms|s|us|ns|bytes|kib|mib|gib|kb|mb|gb|tb|seconds?)\s*$",
    re.IGNORECASE,
)
SCHEMA_DIR = Path(__file__).resolve().parent.parent / "contracts" / "schemas"


def _parse_dt(value: str) -> datetime | None:
    if not isinstance(value, str):
        return None
    normalized = RFC3339_Z_RE.sub("+00:00", value)
    try:
        return datetime.fromisoformat(normalized)
    except ValueError:
        return None


def _has_tz(dt: datetime) -> bool:
    return dt.tzinfo is not None and dt.utcoffset() is not None


def _iter_timestamp_nodes(node: Any, path: str) -> list[tuple[str, Any]]:
    out: list[tuple[str, Any]] = []
    if isinstance(node, dict):
        for key, val in node.items():
            cur = f"{path}.{key}" if path else key
            if key == "timestamp":
                out.append((cur, val))
            out.extend(_iter_timestamp_nodes(val, cur))
    elif isinstance(node, list):
        for idx, val in enumerate(node):
            cur = f"{path}[{idx}]"
            out.extend(_iter_timestamp_nodes(val, cur))
    return out


class Validator:
    def __init__(self, strict_schema: bool = False) -> None:
        self.errors: list[str] = []
        self.warnings: list[str] = []
        self.strict_schema = strict_schema

    def err(self, msg: str) -> None:
        self.errors.append(msg)

    def warn(self, msg: str) -> None:
        self.warnings.append(msg)

    def _expect(self, cond: bool, msg: str) -> None:
        if not cond:
            self.err(msg)

    def _require_keys(self, obj: dict[str, Any], keys: list[str], ctx: str) -> None:
        for key in keys:
            if key not in obj:
                self.err(f"{ctx}: missing required key '{key}'")

    def _as_dict(self, obj: Any, ctx: str) -> dict[str, Any] | None:
        if not isinstance(obj, dict):
            self.err(f"{ctx}: must be object")
            return None
        return obj

    def _as_list(self, obj: Any, ctx: str) -> list[Any] | None:
        if not isinstance(obj, list):
            self.err(f"{ctx}: must be array")
            return None
        return obj

    def validate_with_schema(self, name: str, obj: dict[str, Any], schema_path: Path) -> None:
        if not schema_path.exists():
            self.err(f"{name}: schema file not found: {schema_path}")
            return
        try:
            schema = _load_json(schema_path)
        except Exception as exc:  # noqa: BLE001
            self.err(f"{name}: failed to parse schema {schema_path}: {exc}")
            return

        try:
            import jsonschema  # type: ignore[import-not-found]
        except Exception:
            msg = (
                f"{name}: python package 'jsonschema' not installed, skipped JSON Schema validation "
                f"for {schema_path.name}"
            )
            if self.strict_schema:
                self.err(msg)
            else:
                self.warn(msg)
            return

        try:
            validator = jsonschema.Draft202012Validator(schema)
        except Exception as exc:  # noqa: BLE001
            self.err(f"{name}: failed to build schema validator for {schema_path.name}: {exc}")
            return

        errors = sorted(validator.iter_errors(obj), key=lambda e: list(e.absolute_path))
        for item in errors:
            location = ".".join(str(p) for p in item.absolute_path)
            where = f"{name}.{location}" if location else name
            self.err(f"{where}: schema validation failed: {item.message}")

    def validate_result_sample_timestamps(self, result: dict[str, Any], ws: datetime, we: datetime) -> None:
        sample_roots = [("result.os", result.get("os")), ("result.db", result.get("db"))]
        found = 0
        for root_name, root_value in sample_roots:
            for path, value in _iter_timestamp_nodes(root_value, root_name):
                found += 1
                dt = _parse_dt(value)
                if dt is None:
                    self.err(f"{path} must be RFC3339 date-time")
                    continue
                if not _has_tz(dt):
                    self.err(f"{path} must contain timezone offset")
                    continue
                if dt < ws or dt > we:
                    self.err(
                        f"{path} out of collect_window range: {dt.isoformat()} not in "
                        f"[{ws.isoformat()}, {we.isoformat()}]"
                    )
        if found == 0:
            self.warn("result has no sample timestamp fields under result.os/result.db; skipped window sample check")

    def validate_manifest(self, manifest: dict[str, Any]) -> None:
        ctx = "manifest"
        self._require_keys(
            manifest,
            [
                "schema_version",
                "run_id",
                "db_type",
                "start_time",
                "end_time",
                "exit_code",
                "overall_status",
                "module_stats",
                "artifacts",
            ],
            ctx,
        )
        if manifest.get("schema_version") != "1.0":
            self.err("manifest.schema_version must be '1.0'")
        if manifest.get("db_type") not in {"mysql", "oracle"}:
            self.err("manifest.db_type must be one of: mysql, oracle")
        if manifest.get("exit_code") not in {0, 10, 20, 30}:
            self.err("manifest.exit_code must be one of: 0, 10, 20, 30")
        if manifest.get("overall_status") not in {"success", "partial_success", "failed"}:
            self.err("manifest.overall_status must be one of: success, partial_success, failed")

        st = _parse_dt(manifest.get("start_time"))
        et = _parse_dt(manifest.get("end_time"))
        if not st:
            self.err("manifest.start_time must be RFC3339 date-time")
        elif not _has_tz(st):
            self.err("manifest.start_time must contain timezone offset")
        if not et:
            self.err("manifest.end_time must be RFC3339 date-time")
        elif not _has_tz(et):
            self.err("manifest.end_time must contain timezone offset")
        if st and et and et < st:
            self.err("manifest.end_time must be >= start_time")

        module_stats = self._as_dict(manifest.get("module_stats"), "manifest.module_stats")
        if module_stats is not None:
            if len(module_stats) == 0:
                self.err("manifest.module_stats must not be empty")
            valid_status = {"success", "partial", "failed", "skipped"}
            for mod, item in module_stats.items():
                if not isinstance(item, dict):
                    self.err(f"manifest.module_stats.{mod} must be object")
                    continue
                self._require_keys(item, ["status", "duration_ms", "error"], f"manifest.module_stats.{mod}")
                if item.get("status") not in valid_status:
                    self.err(
                        f"manifest.module_stats.{mod}.status must be one of: "
                        "success, partial, failed, skipped"
                    )
                if not isinstance(item.get("duration_ms"), int) or item.get("duration_ms") < 0:
                    self.err(f"manifest.module_stats.{mod}.duration_ms must be integer >= 0")
                if not (item.get("error") is None or isinstance(item.get("error"), str)):
                    self.err(f"manifest.module_stats.{mod}.error must be string or null")

            non_success = [m for m, v in module_stats.items() if isinstance(v, dict) and v.get("status") != "success"]
            code = manifest.get("exit_code")
            if code == 0 and non_success:
                self.err(f"manifest.exit_code=0 but non-success modules found: {', '.join(non_success)}")
            if code == 10 and not non_success:
                self.warn("manifest.exit_code=10 but all module status are success")

        artifacts = self._as_dict(manifest.get("artifacts"), "manifest.artifacts")
        if artifacts is not None:
            self._require_keys(artifacts, ["log", "result"], "manifest.artifacts")
            if not isinstance(artifacts.get("log"), str) or not artifacts.get("log"):
                self.err("manifest.artifacts.log must be non-empty string")
            for k in ["result", "summary", "report"]:
                if k in artifacts and artifacts[k] is not None and not isinstance(artifacts[k], str):
                    self.err(f"manifest.artifacts.{k} must be string or null")

    def _scan_for_unit_string(self, node: Any, path: str = "result") -> None:
        if isinstance(node, dict):
            for key, val in node.items():
                self._scan_for_unit_string(val, f"{path}.{key}")
            return
        if isinstance(node, list):
            for i, val in enumerate(node):
                self._scan_for_unit_string(val, f"{path}[{i}]")
            return
        if isinstance(node, str) and UNIT_STRING_RE.match(node):
            self.err(f"{path}: unit-bearing string values are forbidden in fact layer: '{node}'")

    def validate_result(self, result: dict[str, Any]) -> None:
        ctx = "result"
        self._require_keys(result, ["meta", "collect_config", "collect_window", "os", "db"], ctx)
        for forbidden in ["exit_code", "overall_status", "module_stats"]:
            if forbidden in result:
                self.err(f"result.{forbidden} is forbidden in fact layer")

        meta = self._as_dict(result.get("meta"), "result.meta")
        if meta is not None:
            self._require_keys(
                meta,
                [
                    "schema_version",
                    "collector_version",
                    "db_type",
                    "db_host",
                    "db_port",
                    "timezone",
                    "collect_time",
                ],
                "result.meta",
            )
            if meta.get("schema_version") != "2.0":
                self.err("result.meta.schema_version must be '2.0'")
            if meta.get("db_type") not in {"mysql", "oracle"}:
                self.err("result.meta.db_type must be mysql or oracle")
            if not isinstance(meta.get("db_port"), int) or meta.get("db_port") <= 0:
                self.err("result.meta.db_port must be integer > 0")
            collect_time = _parse_dt(meta.get("collect_time"))
            if collect_time is None:
                self.err("result.meta.collect_time must be RFC3339 date-time")
            elif not _has_tz(collect_time):
                self.err("result.meta.collect_time must contain timezone offset")

        cfg = self._as_dict(result.get("collect_config"), "result.collect_config")
        if cfg is not None:
            self._require_keys(
                cfg,
                ["sample_mode", "sample_interval_seconds", "sample_period_seconds", "expected_samples"],
                "result.collect_config",
            )
            mode = cfg.get("sample_mode")
            if mode not in {"single", "periodic"}:
                self.err("result.collect_config.sample_mode must be single or periodic")
            if not isinstance(cfg.get("expected_samples"), int) or cfg.get("expected_samples") < 1:
                self.err("result.collect_config.expected_samples must be integer >= 1")
            interval = cfg.get("sample_interval_seconds")
            period = cfg.get("sample_period_seconds")
            if mode == "single":
                if interval is not None or period is not None:
                    self.err("single mode requires sample_interval_seconds=null and sample_period_seconds=null")
            elif mode == "periodic":
                if not isinstance(interval, int) or interval < 1:
                    self.err("periodic mode requires sample_interval_seconds integer >= 1")
                if not isinstance(period, int) or period < 1:
                    self.err("periodic mode requires sample_period_seconds integer >= 1")

        window = self._as_dict(result.get("collect_window"), "result.collect_window")
        if window is not None:
            self._require_keys(window, ["window_start", "window_end", "duration_seconds"], "result.collect_window")
            ws = _parse_dt(window.get("window_start"))
            we = _parse_dt(window.get("window_end"))
            if ws is None:
                self.err("result.collect_window.window_start must be RFC3339 date-time")
            elif not _has_tz(ws):
                self.err("result.collect_window.window_start must contain timezone offset")
            if we is None:
                self.err("result.collect_window.window_end must be RFC3339 date-time")
            elif not _has_tz(we):
                self.err("result.collect_window.window_end must contain timezone offset")
            if ws and we and we < ws:
                self.err("result.collect_window.window_end must be >= window_start")
            if not isinstance(window.get("duration_seconds"), int) or window.get("duration_seconds") < 0:
                self.err("result.collect_window.duration_seconds must be integer >= 0")
            if ws and we and _has_tz(ws) and _has_tz(we):
                self.validate_result_sample_timestamps(result, ws, we)

        if not isinstance(result.get("os"), dict):
            self.err("result.os must be object")
        if not isinstance(result.get("db"), dict):
            self.err("result.db must be object")

        self._scan_for_unit_string(result)

    def validate_summary(self, summary: dict[str, Any]) -> None:
        ctx = "summary"
        self._require_keys(
            summary,
            [
                "schema_version",
                "run_id",
                "rule_version",
                "generated_at",
                "overall_risk",
                "counts",
                "abnormal_items",
                "unevaluated_items",
                "na_items",
            ],
            ctx,
        )
        if summary.get("schema_version") != "1.0":
            self.err("summary.schema_version must be '1.0'")
        if summary.get("overall_risk") not in {"low", "medium", "high"}:
            self.err("summary.overall_risk must be low, medium, or high")
        generated_at = _parse_dt(summary.get("generated_at"))
        if generated_at is None:
            self.err("summary.generated_at must be RFC3339 date-time")
        elif not _has_tz(generated_at):
            self.err("summary.generated_at must contain timezone offset")

        counts = self._as_dict(summary.get("counts"), "summary.counts")
        if counts is not None:
            keys = ["total_checks", "normal", "warning", "critical", "unevaluated", "not_applicable"]
            self._require_keys(counts, keys, "summary.counts")
            values: dict[str, int] = {}
            for key in keys:
                val = counts.get(key)
                if not isinstance(val, int) or val < 0:
                    self.err(f"summary.counts.{key} must be integer >= 0")
                else:
                    values[key] = val
            if len(values) == len(keys):
                computed = (
                    values["normal"]
                    + values["warning"]
                    + values["critical"]
                    + values["unevaluated"]
                    + values["not_applicable"]
                )
                if values["total_checks"] != computed:
                    self.err(
                        "summary.counts mismatch: "
                        "total_checks must equal normal+warning+critical+unevaluated+not_applicable"
                    )

        for arr_key in ["abnormal_items", "unevaluated_items", "na_items"]:
            if not isinstance(summary.get(arr_key), list):
                self.err(f"summary.{arr_key} must be array")

        unevaluated_items = summary.get("unevaluated_items", [])
        if isinstance(unevaluated_items, list):
            for idx, item in enumerate(unevaluated_items):
                if not isinstance(item, dict):
                    self.err(f"summary.unevaluated_items[{idx}] must be object")
                    continue
                self._require_keys(item, ["check_id", "reason_type", "reason"], f"summary.unevaluated_items[{idx}]")
                if item.get("reason_type") not in {"failed", "skipped"}:
                    self.err(f"summary.unevaluated_items[{idx}].reason_type must be failed or skipped")

        na_items = summary.get("na_items", [])
        if isinstance(na_items, list):
            for idx, item in enumerate(na_items):
                if not isinstance(item, dict):
                    self.err(f"summary.na_items[{idx}] must be object")
                    continue
                self._require_keys(item, ["check_id", "reason_type", "reason"], f"summary.na_items[{idx}]")
                if item.get("reason_type") != "not_applicable":
                    self.err(f"summary.na_items[{idx}].reason_type must be not_applicable")

        if isinstance(counts, dict):
            if isinstance(unevaluated_items, list) and counts.get("unevaluated") != len(unevaluated_items):
                self.err("summary.counts.unevaluated must equal len(summary.unevaluated_items)")
            if isinstance(na_items, list) and counts.get("not_applicable") != len(na_items):
                self.err("summary.counts.not_applicable must equal len(summary.na_items)")

        failure = summary.get("failure")
        if failure is not None:
            if not isinstance(failure, dict):
                self.err("summary.failure must be object when present")
            else:
                self._require_keys(failure, ["exit_code", "reason_type", "message"], "summary.failure")
                if failure.get("exit_code") not in {20, 30}:
                    self.err("summary.failure.exit_code must be 20 or 30")
                if failure.get("reason_type") not in {"collector_failed", "precheck_failed"}:
                    self.err("summary.failure.reason_type must be collector_failed or precheck_failed")
                if not isinstance(failure.get("message"), str) or not failure.get("message").strip():
                    self.err("summary.failure.message must be non-empty string")

    def cross_validate(
        self,
        manifest: dict[str, Any],
        result: dict[str, Any] | None,
        summary: dict[str, Any] | None,
        rule: dict[str, Any] | None,
    ) -> None:
        run_id = manifest.get("run_id")
        if summary is not None:
            if summary.get("run_id") != run_id:
                self.err("summary.run_id must equal manifest.run_id")

        if result is not None:
            db_type = manifest.get("db_type")
            result_db_type = result.get("meta", {}).get("db_type") if isinstance(result.get("meta"), dict) else None
            if result_db_type != db_type:
                self.err("result.meta.db_type must equal manifest.db_type")

        if rule is not None:
            rule_meta = rule.get("rule_meta", {}) if isinstance(rule.get("rule_meta"), dict) else {}
            rule_db_type = rule_meta.get("db_type")
            if rule_db_type is not None and rule_db_type != manifest.get("db_type"):
                self.err("rule.rule_meta.db_type must equal manifest.db_type")
            if summary is not None:
                if summary.get("rule_version") != rule_meta.get("rule_version"):
                    self.err("summary.rule_version must equal rule.rule_meta.rule_version")

        exit_code = manifest.get("exit_code")
        if exit_code in {20, 30} and summary is not None:
            failure = summary.get("failure")
            if not isinstance(failure, dict):
                self.err("manifest.exit_code is 20/30, summary.failure is required")
            else:
                if failure.get("exit_code") != exit_code:
                    self.err("summary.failure.exit_code must equal manifest.exit_code")
                expected_reason = "collector_failed" if exit_code == 20 else "precheck_failed"
                if failure.get("reason_type") != expected_reason:
                    self.err(f"summary.failure.reason_type must be {expected_reason} when exit_code={exit_code}")
            if summary.get("overall_risk") != "high":
                self.err("manifest.exit_code is 20/30, summary.overall_risk must be high")
            counts = summary.get("counts", {})
            if isinstance(counts, dict):
                risk_inputs = [counts.get("normal"), counts.get("warning"), counts.get("critical")]
                if any(isinstance(v, int) and v > 0 for v in risk_inputs):
                    self.err("manifest.exit_code indicates failure, summary must not have normal/warning/critical checks")
            abnormal_items = summary.get("abnormal_items")
            if isinstance(abnormal_items, list) and len(abnormal_items) > 0:
                self.err("manifest.exit_code indicates failure, summary.abnormal_items must be empty")
        if exit_code in {0, 10} and summary is not None and summary.get("failure") is not None:
            self.err("summary.failure is only allowed when manifest.exit_code is 20 or 30")


def _load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _resolve_artifact(run_dir: Path, p: Any) -> Path | None:
    if p is None:
        return None
    if not isinstance(p, str) or not p:
        return None
    candidate = Path(p)
    if not candidate.is_absolute():
        candidate = run_dir / candidate
    return candidate


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate frozen contracts v1")
    parser.add_argument("--run-dir", type=Path, help="run directory containing manifest/result/summary artifacts")
    parser.add_argument("--manifest", type=Path, help="manifest.json path")
    parser.add_argument("--result", type=Path, help="result.json path (optional override)")
    parser.add_argument("--summary", type=Path, help="summary.json path (optional override)")
    parser.add_argument("--rule", type=Path, help="rule.json path (optional, validates against rule schema)")
    parser.add_argument(
        "--strict-schema",
        action="store_true",
        help="fail validation when JSON Schema checks cannot be executed (e.g. missing jsonschema package)",
    )
    args = parser.parse_args()

    if not args.run_dir and not args.manifest:
        parser.error("either --run-dir or --manifest is required")

    run_dir = args.run_dir.resolve() if args.run_dir else args.manifest.resolve().parent
    manifest_path = args.manifest.resolve() if args.manifest else (run_dir / "manifest.json")
    if not manifest_path.exists():
        print(f"[ERROR] manifest not found: {manifest_path}")
        return 1

    v = Validator(strict_schema=args.strict_schema)
    try:
        manifest = _load_json(manifest_path)
    except Exception as exc:  # noqa: BLE001
        print(f"[ERROR] failed to parse manifest: {exc}")
        return 1

    if not isinstance(manifest, dict):
        print("[ERROR] manifest root must be object")
        return 1
    v.validate_with_schema("manifest", manifest, SCHEMA_DIR / "manifest.schema.json")
    v.validate_manifest(manifest)

    artifacts = manifest.get("artifacts", {}) if isinstance(manifest.get("artifacts"), dict) else {}
    result_path = args.result.resolve() if args.result else _resolve_artifact(run_dir, artifacts.get("result"))
    summary_path = args.summary.resolve() if args.summary else _resolve_artifact(run_dir, artifacts.get("summary"))
    rule_path = args.rule.resolve() if args.rule else None

    result_obj: dict[str, Any] | None = None
    summary_obj: dict[str, Any] | None = None
    rule_obj: dict[str, Any] | None = None

    if args.run_dir:
        log_path = _resolve_artifact(run_dir, artifacts.get("log"))
        if log_path is None:
            v.err("manifest.artifacts.log path is missing or invalid")
        elif not log_path.exists():
            v.err(f"log artifact path does not exist: {log_path}")

        report_path = _resolve_artifact(run_dir, artifacts.get("report"))
        if artifacts.get("report") is not None:
            if report_path is None:
                v.err("manifest.artifacts.report path is invalid")
            elif not report_path.exists():
                v.err(f"report artifact path does not exist: {report_path}")

    if result_path is not None:
        if result_path.exists():
            try:
                obj = _load_json(result_path)
                if isinstance(obj, dict):
                    result_obj = obj
                    v.validate_with_schema("result", result_obj, SCHEMA_DIR / "result.schema.json")
                    v.validate_result(result_obj)
                else:
                    v.err(f"result root must be object: {result_path}")
            except Exception as exc:  # noqa: BLE001
                v.err(f"failed to parse result: {exc}")
        else:
            v.err(f"result artifact path does not exist: {result_path}")

    if summary_path is not None:
        if summary_path.exists():
            try:
                obj = _load_json(summary_path)
                if isinstance(obj, dict):
                    summary_obj = obj
                    v.validate_with_schema("summary", summary_obj, SCHEMA_DIR / "summary.schema.json")
                    v.validate_summary(summary_obj)
                else:
                    v.err(f"summary root must be object: {summary_path}")
            except Exception as exc:  # noqa: BLE001
                v.err(f"failed to parse summary: {exc}")
        else:
            v.err(f"summary artifact path does not exist: {summary_path}")

    if rule_path is not None:
        if rule_path.exists():
            try:
                obj = _load_json(rule_path)
                if isinstance(obj, dict):
                    rule_obj = obj
                    v.validate_with_schema("rule", rule_obj, SCHEMA_DIR / "rule.schema.json")
                else:
                    v.err(f"rule root must be object: {rule_path}")
            except Exception as exc:  # noqa: BLE001
                v.err(f"failed to parse rule: {exc}")
        else:
            v.err(f"rule file not found: {rule_path}")

    v.cross_validate(manifest, result_obj, summary_obj, rule_obj)

    for msg in v.warnings:
        print(f"[WARN] {msg}")
    for msg in v.errors:
        print(f"[ERROR] {msg}")

    if v.errors:
        print(f"\nValidation failed: {len(v.errors)} error(s), {len(v.warnings)} warning(s)")
        return 1

    print(f"Validation passed: 0 error(s), {len(v.warnings)} warning(s)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
