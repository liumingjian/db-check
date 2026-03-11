#!/usr/bin/env python3
"""Generate report-meta.json for markdown/docx reporting flows."""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Sequence

if __package__ in {None, ""}:
    import sys

    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from reporter.model.errors import EXIT_INTERNAL_ERROR, EXIT_OK, EXIT_PARAM_ERROR, ReporterFailure  # noqa: E402
from reporter.parser.contracts import ensure_file, load_object, validate_inputs  # noqa: E402

DEFAULT_INSPECTOR = "db-check"
DEFAULT_TEMPLATE_VERSION = "v1.0"
DEFAULT_CHANGE_DESCRIPTION = "mysql巡检报告"
DEFAULT_REVIEW_NAME = "周海波"
DEFAULT_REVIEW_TITLE = "数据库技术经理"
DEFAULT_REVIEW_CONTACT = "13570391044"
DEFAULT_REVIEW_EMAIL = "haibo.zhou@antute.com.cn"


@dataclass(frozen=True)
class MetaOptions:
    mysql_version: str
    inspector: str = DEFAULT_INSPECTOR
    data_dir: str = ""
    version_label: str = DEFAULT_TEMPLATE_VERSION
    document_name: str = ""
    change_description: str = DEFAULT_CHANGE_DESCRIPTION
    review_name: str = DEFAULT_REVIEW_NAME
    review_title: str = DEFAULT_REVIEW_TITLE
    review_contact: str = DEFAULT_REVIEW_CONTACT
    review_email: str = DEFAULT_REVIEW_EMAIL


class _ArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        raise ReporterFailure(EXIT_PARAM_ERROR, f"参数错误: {message}")


def build_parser() -> _ArgumentParser:
    parser = _ArgumentParser(description="Generate report-meta.json from result and summary")
    parser.add_argument("--result", type=Path, required=True)
    parser.add_argument("--summary", type=Path, required=True)
    parser.add_argument("--mysql-version", required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--inspector", default=DEFAULT_INSPECTOR)
    parser.add_argument("--author", dest="inspector")
    parser.add_argument("--document-name", default="")
    parser.add_argument("--change-description", default=DEFAULT_CHANGE_DESCRIPTION)
    parser.add_argument("--review-name", default=DEFAULT_REVIEW_NAME)
    parser.add_argument("--review-title", default=DEFAULT_REVIEW_TITLE)
    parser.add_argument("--review-contact", default=DEFAULT_REVIEW_CONTACT)
    parser.add_argument("--review-email", default=DEFAULT_REVIEW_EMAIL)
    parser.add_argument("--data-dir", default="")
    parser.add_argument("--version-label", default=DEFAULT_TEMPLATE_VERSION)
    return parser


def run(argv: Sequence[str] | None = None) -> int:
    try:
        args = build_parser().parse_args(argv)
        ensure_file(args.result, "result")
        ensure_file(args.summary, "summary")
        result = load_object(args.result, "result")
        summary = load_object(args.summary, "summary")
        validate_inputs(result, summary)
        options = MetaOptions(
            mysql_version=args.mysql_version,
            inspector=args.inspector,
            data_dir=args.data_dir,
            version_label=args.version_label,
            document_name=args.document_name or args.out.name,
            change_description=args.change_description,
            review_name=args.review_name,
            review_title=args.review_title,
            review_contact=args.review_contact,
            review_email=args.review_email,
        )
        meta = build_report_meta(result, summary, options)
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(json.dumps(meta, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        print(f"generated: {args.out}")
        return EXIT_OK
    except ReporterFailure as exc:
        print(f"[ERROR] {exc}")
        return exc.code
    except Exception as exc:  # noqa: BLE001
        print(f"[ERROR] 未分类内部错误: {exc}")
        return EXIT_INTERNAL_ERROR


def build_report_meta(
    result: dict[str, Any],
    summary: dict[str, Any],
    options: MetaOptions,
) -> dict[str, Any]:
    result_meta = result.get("meta", {}) if isinstance(result.get("meta"), dict) else {}
    collect_time = str(result_meta.get("collect_time", ""))
    db_host = str(result_meta.get("db_host", ""))
    db_port = result_meta.get("db_port", "")
    issue_date = _issue_date(collect_time)
    architecture_role = _architecture_role(result, summary)
    instance = f"{db_host}:{db_port}".strip(":")
    return {
        "doc_info": {
            "document_name": options.document_name or "report.docx",
            "inspection_time": _inspection_time(collect_time),
            "issue_date": issue_date,
            "author": options.inspector,
            "version": options.version_label,
        },
        "change_log": [
            {
                "date": issue_date,
                "author": options.inspector,
                "version": options.version_label,
                "change": options.change_description,
            }
        ],
        "review_log": [
            {
                "name": options.review_name,
                "title": options.review_title,
                "contact": options.review_contact,
                "email": options.review_email,
            }
        ],
        "scope": {
            "inspection_target": _inspection_target(instance, options.mysql_version),
            "instances": [instance] if instance else [],
            "database_version": options.mysql_version,
            "architecture_role": architecture_role,
            "data_dir": _resolve_data_dir(result, options.data_dir),
        },
    }


def _issue_date(collect_time: str) -> str:
    if collect_time:
        return _parse_time(collect_time).strftime("%Y/%m/%d")
    return datetime.now().strftime("%Y/%m/%d")


def _inspection_time(collect_time: str) -> str:
    if not collect_time:
        return "待补充"
    return _parse_time(collect_time).strftime("%Y/%m/%d %H:%M:%S")


def _parse_time(raw: str) -> datetime:
    return datetime.fromisoformat(raw.replace("Z", "+00:00"))


def _architecture_role(result: dict[str, Any], summary: dict[str, Any]) -> str:
    na_items = summary.get("na_items", []) if isinstance(summary.get("na_items"), list) else []
    na_ids = {item.get("check_id") for item in na_items if isinstance(item, dict)}
    replication = result.get("db", {}).get("replication", {}) if isinstance(result.get("db", {}).get("replication"), dict) else {}
    if "2.0" in na_ids or not replication.get("enabled"):
        return "Standalone"
    snapshot = replication.get("replica_status_snapshot", {}) if isinstance(replication.get("replica_status_snapshot"), dict) else {}
    if snapshot.get("Master_Host"):
        return "Replica"
    return "Replication Enabled / Unknown"


def _inspection_target(instance: str, mysql_version: str) -> str:
    if instance:
        return instance
    return f"MySQL {mysql_version}"


def _resolve_data_dir(result: dict[str, Any], override: str) -> str:
    if override:
        return override
    candidates = (
        _nested_string(result, "db", "basic_info", "datadir"),
        _nested_string(result, "db", "config_check", "datadir"),
    )
    for value in candidates:
        if value:
            return value
    return "待补充"


def _nested_string(node: dict[str, Any], *keys: str) -> str:
    current: Any = node
    for key in keys:
        if not isinstance(current, dict):
            return ""
        current = current.get(key)
    return current.strip() if isinstance(current, str) else ""


if __name__ == "__main__":
    raise SystemExit(run())
