#!/usr/bin/env python3
"""Analyzer CLI for contracts-driven summary generation."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Sequence

if __package__ in {None, ""}:
    import sys

    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from analyzer.common.json_io import write_json_atomic
from analyzer.evaluator.rule_engine import generate_summary
from analyzer.model.errors import (
    AnalyzerFailure,
    EXIT_INTERNAL_ERROR,
    EXIT_OK,
    EXIT_OUTPUT_ERROR,
    EXIT_PARAM_ERROR,
    EXIT_RULE_EVAL_ERROR,
    EXIT_SCHEMA_ERROR,
    EXIT_CONSISTENCY_ERROR,
)
from analyzer.validator.stages import (
    Validator,
    ensure_readable_file,
    load_object,
    validate_contract_stage,
    validate_cross_stage,
    validate_schema_stage,
)


class _ArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        raise AnalyzerFailure(EXIT_PARAM_ERROR, f"参数错误: {message}")


def build_parser() -> _ArgumentParser:
    parser = _ArgumentParser(description="Analyze manifest/result/rule and generate summary.json")
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--result", type=Path, required=True)
    parser.add_argument("--rule", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--log-level", default="INFO")
    parser.add_argument("--strict-schema", action="store_true")
    return parser


def parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = build_parser()
    return parser.parse_args(argv)


def _print_messages(prefix: str, messages: list[str]) -> None:
    for message in messages:
        print(f"[{prefix}] {message}")


def _load_inputs(args: argparse.Namespace) -> tuple[dict, dict, dict]:
    ensure_readable_file(args.manifest, "manifest")
    ensure_readable_file(args.result, "result")
    ensure_readable_file(args.rule, "rule")
    manifest = load_object(args.manifest, "manifest")
    result = load_object(args.result, "result")
    rule = load_object(args.rule, "rule")
    return manifest, result, rule


def _run_validations(manifest: dict, result: dict, rule: dict, strict_schema: bool) -> Validator:
    validator = Validator(strict_schema=strict_schema)
    schema_errors, schema_warnings = validate_schema_stage(validator, manifest, result, rule)
    _print_messages("WARN", schema_warnings)
    if schema_errors:
        _print_messages("ERROR", schema_errors)
        raise AnalyzerFailure(EXIT_SCHEMA_ERROR, "schema 校验失败")

    contract_errors, contract_warnings = validate_contract_stage(validator, manifest, result)
    _print_messages("WARN", contract_warnings)
    if contract_errors:
        _print_messages("ERROR", contract_errors)
        raise AnalyzerFailure(EXIT_SCHEMA_ERROR, "输入契约校验失败")

    cross_errors, cross_warnings = validate_cross_stage(validator, manifest, result, rule)
    _print_messages("WARN", cross_warnings)
    if cross_errors:
        _print_messages("ERROR", cross_errors)
        raise AnalyzerFailure(EXIT_CONSISTENCY_ERROR, "跨文件一致性校验失败")
    return validator


def _write_output(path: Path, payload: dict) -> None:
    try:
        write_json_atomic(path, payload)
    except Exception as exc:  # noqa: BLE001
        raise AnalyzerFailure(EXIT_OUTPUT_ERROR, f"summary 写入失败: {exc}") from exc


def run(argv: Sequence[str] | None = None) -> int:
    try:
        args = parse_args(argv)
        manifest, result, rule = _load_inputs(args)
        _run_validations(manifest, result, rule, args.strict_schema)
        try:
            summary = generate_summary(manifest, result, rule)
        except Exception as exc:  # noqa: BLE001
            raise AnalyzerFailure(EXIT_RULE_EVAL_ERROR, f"规则判定失败: {exc}") from exc
        _write_output(args.out, summary)
        print(f"generated: {args.out}")
        return EXIT_OK
    except AnalyzerFailure as exc:
        print(f"[ERROR] {exc}")
        return exc.code
    except Exception as exc:  # noqa: BLE001
        print(f"[ERROR] 未分类内部错误: {exc}")
        return EXIT_INTERNAL_ERROR


def main() -> int:
    return run()


if __name__ == "__main__":
    raise SystemExit(main())
