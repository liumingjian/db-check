"""Validation stages for analyzer execution flow."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

from analyzer.common.json_io import load_json
from analyzer.model.errors import AnalyzerFailure, EXIT_INPUT_ERROR
from tasks.validate_frozen_contracts import SCHEMA_DIR, Validator


def ensure_readable_file(path: Path, label: str) -> None:
    if not path.exists() or not path.is_file():
        raise AnalyzerFailure(EXIT_INPUT_ERROR, f"{label} 文件不存在: {path}")
    if not path.stat().st_size and label != "summary":
        raise AnalyzerFailure(EXIT_INPUT_ERROR, f"{label} 文件为空: {path}")


def load_object(path: Path, label: str) -> dict[str, Any]:
    try:
        payload = load_json(path)
    except Exception as exc:  # noqa: BLE001
        raise AnalyzerFailure(EXIT_INPUT_ERROR, f"{label} 解析失败: {exc}") from exc
    if not isinstance(payload, dict):
        raise AnalyzerFailure(EXIT_INPUT_ERROR, f"{label} 根节点必须是 JSON object")
    return payload


def run_validation_stage(validator: Validator, action: Callable[[], None]) -> tuple[list[str], list[str]]:
    before_error_count = len(validator.errors)
    before_warn_count = len(validator.warnings)
    action()
    return validator.errors[before_error_count:], validator.warnings[before_warn_count:]


def validate_schema_stage(validator: Validator, manifest: dict[str, Any], result: dict[str, Any], rule: dict[str, Any]) -> tuple[list[str], list[str]]:
    def _run() -> None:
        validator.validate_with_schema("manifest", manifest, SCHEMA_DIR / "manifest.schema.json")
        validator.validate_with_schema("result", result, SCHEMA_DIR / "result.schema.json")
        validator.validate_with_schema("rule", rule, SCHEMA_DIR / "rule.schema.json")

    return run_validation_stage(validator, _run)


def validate_contract_stage(validator: Validator, manifest: dict[str, Any], result: dict[str, Any]) -> tuple[list[str], list[str]]:
    def _run() -> None:
        validator.validate_manifest(manifest)
        validator.validate_result(result)

    return run_validation_stage(validator, _run)


def validate_cross_stage(
    validator: Validator,
    manifest: dict[str, Any],
    result: dict[str, Any],
    rule: dict[str, Any],
) -> tuple[list[str], list[str]]:
    def _run() -> None:
        validator.cross_validate(manifest, result, None, rule)

    return run_validation_stage(validator, _run)
