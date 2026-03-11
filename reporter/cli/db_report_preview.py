#!/usr/bin/env python3
"""Generate formal Markdown report content without touching DOCX."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Sequence

if __package__ in {None, ""}:
    import sys

    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from reporter.content.mysql_report_builder import build_mysql_report_view  # noqa: E402
from reporter.model.errors import EXIT_INTERNAL_ERROR, EXIT_OK, EXIT_PARAM_ERROR, ReporterFailure  # noqa: E402
from reporter.parser.contracts import ensure_file, load_object, validate_inputs  # noqa: E402
from reporter.renderer.markdown_preview import render_markdown_preview  # noqa: E402


class _ArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        raise ReporterFailure(EXIT_PARAM_ERROR, f"参数错误: {message}")


def build_parser() -> _ArgumentParser:
    parser = _ArgumentParser(description="Render markdown report from result/summary/meta")
    parser.add_argument("--result", type=Path, required=True)
    parser.add_argument("--summary", type=Path, required=True)
    parser.add_argument("--meta", type=Path, required=True)
    parser.add_argument("--out-md", type=Path)
    parser.add_argument("--out-json", type=Path, required=True)
    return parser


def run(argv: Sequence[str] | None = None) -> int:
    try:
        args = build_parser().parse_args(argv)
        ensure_file(args.result, "result")
        ensure_file(args.summary, "summary")
        ensure_file(args.meta, "meta")
        result = load_object(args.result, "result")
        summary = load_object(args.summary, "summary")
        meta = load_object(args.meta, "meta")
        validate_inputs(result, summary)
        report = build_mysql_report_view(result, summary, meta)
        if args.out_md is not None:
            _write_text(args.out_md, render_markdown_preview(report))
        _write_text(args.out_json, json.dumps(report.to_dict(), ensure_ascii=False, indent=2) + "\n")
        if args.out_md is not None:
            print(f"generated: {args.out_md}")
        print(f"generated: {args.out_json}")
        return EXIT_OK
    except ReporterFailure as exc:
        print(f"[ERROR] {exc}")
        return exc.code
    except Exception as exc:  # noqa: BLE001
        print(f"[ERROR] 未分类内部错误: {exc}")
        return EXIT_INTERNAL_ERROR


def _write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


if __name__ == "__main__":
    raise SystemExit(run())
