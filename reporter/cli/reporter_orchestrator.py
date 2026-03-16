#!/usr/bin/env python3
"""Run the full report pipeline from a collector run directory."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence

if __package__ in {None, ""}:
    import sys

    module_root = Path(__file__).resolve().parent / "python_modules"
    if module_root.exists():
        sys.path.insert(0, str(module_root))
    else:
        sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from analyzer.cli.db_analyzer import run as run_analyzer  # noqa: E402
from reporter.cli.db_report_preview import run as run_report_view  # noqa: E402
from reporter.cli.generate_report_meta import run as run_meta  # noqa: E402
from reporter.cli.render_template_docx import run as run_docx  # noqa: E402
from reporter.awr.enrich import write_enriched_result  # noqa: E402
from reporter.common.json_io import load_json  # noqa: E402
from reporter.rules.merge import write_effective_rule  # noqa: E402
from tasks.validate_frozen_contracts import run as run_contracts  # noqa: E402

EXIT_OK = 0
EXIT_PARAM_ERROR = 2
EXIT_RUNTIME_ERROR = 20
EXIT_VALIDATE_ERROR = 30
EXIT_REPORT_ERROR = 40


@dataclass(frozen=True)
class Options:
    run_dir: Path
    rule_file: Path
    template_file: Path
    awr_file: Path | None
    out_docx: Path
    out_md: Path | None
    document_name: str
    inspector: str
    change_description: str
    review_name: str
    review_title: str
    review_contact: str
    review_email: str
    mysql_version: str | None


class _ArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        raise ValueError(f"参数错误: {message}")


def build_parser() -> _ArgumentParser:
    parser = _ArgumentParser(description="Generate final report artifacts from a collector run directory")
    parser.add_argument("--run-dir", type=Path, required=True)
    parser.add_argument("--rule-file", type=Path, required=True)
    parser.add_argument("--template-file", type=Path, required=True)
    parser.add_argument("--awr-file", type=Path)
    parser.add_argument("--out-docx", type=Path)
    parser.add_argument("--out-md", type=Path)
    parser.add_argument("--document-name", default="")
    parser.add_argument("--inspector", default="db-check")
    parser.add_argument("--change-description", default="")
    parser.add_argument("--review-name", default="周海波")
    parser.add_argument("--review-title", default="数据库技术经理")
    parser.add_argument("--review-contact", default="13570391044")
    parser.add_argument("--review-email", default="haibo.zhou@antute.com.cn")
    parser.add_argument("--mysql-version")
    return parser


def parse_args(argv: Sequence[str] | None) -> Options:
    args = build_parser().parse_args(argv)
    run_dir = args.run_dir.resolve()
    out_docx = args.out_docx.resolve() if args.out_docx else run_dir / "report.docx"
    return Options(
        run_dir=run_dir,
        rule_file=args.rule_file.resolve(),
        template_file=args.template_file.resolve(),
        awr_file=args.awr_file.resolve() if args.awr_file else None,
        out_docx=out_docx,
        out_md=args.out_md.resolve() if args.out_md else None,
        document_name=args.document_name or out_docx.name,
        inspector=args.inspector,
        change_description=args.change_description,
        review_name=args.review_name,
        review_title=args.review_title,
        review_contact=args.review_contact,
        review_email=args.review_email,
        mysql_version=args.mysql_version,
    )


def run(argv: Sequence[str] | None = None) -> int:
    try:
        options = parse_args(argv)
        paths = build_paths(options.run_dir)
        ensure_inputs(options, paths)
        result_path = paths.result
        rule_path = options.rule_file
        if options.awr_file is not None:
            result_path = write_enriched_result(run_dir=options.run_dir, awr_file=options.awr_file)
            extension_rule = options.rule_file.parent / "rule.awr.json"
            if not extension_rule.exists():
                raise RuntimeError(f"AWR extension rule not found: {extension_rule}")
            rule_path = write_effective_rule(
                run_dir=options.run_dir,
                base_rule=options.rule_file,
                extension_rule=extension_rule,
            )
        db_version = options.mysql_version or detect_db_version(result_path)
        if not db_version:
            raise RuntimeError("无法从 result.json 自动识别数据库版本，请显式传入 --mysql-version")
        report_view_code = run_pipeline(options, paths, db_version, result_path, rule_path)
        if report_view_code != EXIT_OK:
            return report_view_code
        return validate_outputs(paths, rule_path, result_path)
    except ValueError as exc:
        print(f"[ERROR] {exc}")
        return EXIT_PARAM_ERROR
    except RuntimeError as exc:
        print(f"[ERROR] {exc}")
        return EXIT_RUNTIME_ERROR
    except Exception as exc:  # noqa: BLE001
        print(f"[ERROR] 未分类内部错误: {exc}")
        return EXIT_REPORT_ERROR


@dataclass(frozen=True)
class Paths:
    manifest: Path
    result: Path
    summary: Path
    report_meta: Path
    report_view: Path


def build_paths(run_dir: Path) -> Paths:
    return Paths(
        manifest=run_dir / "manifest.json",
        result=run_dir / "result.json",
        summary=run_dir / "summary.json",
        report_meta=run_dir / "report-meta.json",
        report_view=run_dir / "report-view.json",
    )


def ensure_inputs(options: Options, paths: Paths) -> None:
    if not options.run_dir.is_dir():
        raise ValueError(f"run-dir 不存在: {options.run_dir}")
    for label, path in required_paths(paths, options):
        if not path.exists():
            raise ValueError(f"{label} 文件不存在: {path}")
    if options.awr_file is not None and (not options.awr_file.exists() or not options.awr_file.is_file()):
        raise ValueError(f"awr-file 文件不存在: {options.awr_file}")


def required_paths(paths: Paths, options: Options) -> list[tuple[str, Path]]:
    return [
        ("manifest", paths.manifest),
        ("result", paths.result),
        ("rule", options.rule_file),
        ("template", options.template_file),
    ]


def detect_db_version(result_path: Path) -> str | None:
    result = load_json(result_path)
    candidates = [
        nested_value(result, "db", "basic_info", "version"),
        nested_value(result, "db", "basic_info", "version_vars", "version"),
        nested_value(result, "db", "basic_info", "summary", "version"),
        nested_value(result, "db", "basic_info", "summary", "gaussdb_version"),
    ]
    for value in candidates:
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def nested_value(node: dict[str, Any], *keys: str) -> Any:
    current: Any = node
    for key in keys:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def run_pipeline(options: Options, paths: Paths, db_version: str, result_path: Path, rule_path: Path) -> int:
    summary_code = run_stage(
        "analyze",
        run_analyzer,
        analyzer_args(paths, rule_path, result_path),
        EXIT_VALIDATE_ERROR,
    )
    if summary_code != EXIT_OK:
        return summary_code
    meta_code = run_stage("meta", run_meta, meta_args(paths, options, db_version, result_path), EXIT_REPORT_ERROR)
    if meta_code != EXIT_OK:
        return meta_code
    view_code = run_stage("report-view", run_report_view, report_view_args(paths, options, result_path), EXIT_REPORT_ERROR)
    if view_code != EXIT_OK:
        return view_code
    return run_stage("docx", run_docx, docx_args(paths, options), EXIT_REPORT_ERROR)


def analyzer_args(paths: Paths, rule_file: Path, result_path: Path) -> list[str]:
    return [
        "--manifest", str(paths.manifest),
        "--result", str(result_path),
        "--rule", str(rule_file),
        "--strict-schema",
        "--out", str(paths.summary),
    ]


def meta_args(paths: Paths, options: Options, db_version: str, result_path: Path) -> list[str]:
    return [
        "--result", str(result_path),
        "--summary", str(paths.summary),
        "--mysql-version", db_version,
        "--document-name", options.document_name,
        "--inspector", options.inspector,
        "--change-description", options.change_description,
        "--review-name", options.review_name,
        "--review-title", options.review_title,
        "--review-contact", options.review_contact,
        "--review-email", options.review_email,
        "--out", str(paths.report_meta),
    ]


def report_view_args(paths: Paths, options: Options, result_path: Path) -> list[str]:
    args = [
        "--result", str(result_path),
        "--summary", str(paths.summary),
        "--meta", str(paths.report_meta),
        "--out-json", str(paths.report_view),
    ]
    if options.out_md is not None:
        args.extend(["--out-md", str(options.out_md)])
    return args


def docx_args(paths: Paths, options: Options) -> list[str]:
    return [
        "--report-view", str(paths.report_view),
        "--template", str(options.template_file),
        "--out", str(options.out_docx),
    ]


def run_stage(label: str, func: Any, argv: list[str], error_code: int) -> int:
    print(f"[INFO] stage={label} status=started")
    code = int(func(argv))
    if code != EXIT_OK:
        print(f"[ERROR] stage={label} status=failed exit_code={code}")
        return error_code
    print(f"[INFO] stage={label} status=finished")
    return EXIT_OK


def validate_outputs(paths: Paths, rule_file: Path, result_path: Path) -> int:
    print("[INFO] stage=validate status=started")
    code = run_contracts(
        [
            "--manifest", str(paths.manifest),
            "--result", str(result_path),
            "--summary", str(paths.summary),
            "--rule", str(rule_file),
            "--strict-schema",
        ]
    )
    if code != EXIT_OK:
        print(f"[ERROR] stage=validate status=failed exit_code={code}")
        return EXIT_VALIDATE_ERROR
    print("[INFO] stage=validate status=finished")
    return EXIT_OK


if __name__ == "__main__":
    raise SystemExit(run())
