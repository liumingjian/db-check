"""Reporter input loading and contract validation."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from reporter.common.json_io import load_json
from reporter.model.errors import EXIT_CONTRACT_ERROR, EXIT_INPUT_ERROR, ReporterFailure
from tasks.validate_frozen_contracts import SCHEMA_DIR, Validator


def ensure_file(path: Path, label: str) -> None:
    if not path.exists() or not path.is_file():
        raise ReporterFailure(EXIT_INPUT_ERROR, f"{label} 文件不存在: {path}")


def load_object(path: Path, label: str) -> dict[str, Any]:
    try:
        payload = load_json(path)
    except Exception as exc:  # noqa: BLE001
        raise ReporterFailure(EXIT_CONTRACT_ERROR, f"{label} 解析失败: {exc}") from exc
    if not isinstance(payload, dict):
        raise ReporterFailure(EXIT_CONTRACT_ERROR, f"{label} 根节点必须是 JSON object")
    return payload


def validate_inputs(result: dict[str, Any], summary: dict[str, Any]) -> None:
    validator = Validator(strict_schema=False)
    validator.validate_with_schema("result", result, SCHEMA_DIR / "result.schema.json")
    validator.validate_with_schema("summary", summary, SCHEMA_DIR / "summary.schema.json")
    validator.validate_result(result)
    validator.validate_summary(summary)
    if validator.errors:
        for message in validator.errors:
            print(f"[ERROR] {message}")
        raise ReporterFailure(EXIT_CONTRACT_ERROR, "输入契约校验失败")
    for warning in validator.warnings:
        print(f"[WARN] {warning}")
