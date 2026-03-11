#!/usr/bin/env python3
"""Render a template-styled DOCX from report.md/report-view.json."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Sequence

if __package__ in {None, ""}:
    import sys

    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from reporter.model.errors import EXIT_INTERNAL_ERROR, EXIT_OK, EXIT_PARAM_ERROR, ReporterFailure  # noqa: E402
from reporter.parser.contracts import ensure_file  # noqa: E402
from reporter.renderer.template_docx_renderer import render_template_docx  # noqa: E402


class _ArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        raise ReporterFailure(EXIT_PARAM_ERROR, f"参数错误: {message}")


def build_parser() -> _ArgumentParser:
    parser = _ArgumentParser(description="Render template-styled docx from report markdown")
    parser.add_argument("--report-md", type=Path, required=True)
    parser.add_argument("--template", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--report-view", type=Path)
    return parser


def parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    return build_parser().parse_args(argv)


def run(argv: Sequence[str] | None = None) -> int:
    try:
        args = parse_args(argv)
        ensure_file(args.report_md, "report-md")
        ensure_file(args.template, "template")
        report_view = args.report_view or args.report_md.with_name("report-view.json")
        ensure_file(report_view, "report-view")
        render_template_docx(args.template, report_view, args.out)
        print(f"generated: {args.out}")
        return EXIT_OK
    except ReporterFailure as exc:
        print(f"[ERROR] {exc}")
        return exc.code
    except Exception as exc:  # noqa: BLE001
        print(f"[ERROR] 未分类内部错误: {exc}")
        return EXIT_INTERNAL_ERROR


def main() -> int:
    return run()


if __name__ == "__main__":
    raise SystemExit(main())
