#!/usr/bin/env python3
"""Generate report-meta.json for markdown/docx reporting flows."""

from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Sequence

if __package__ in {None, ""}:
    import sys

    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from reporter.model.errors import EXIT_INTERNAL_ERROR, EXIT_OK, EXIT_PARAM_ERROR, ReporterFailure  # noqa: E402
from reporter.parser.contracts import ensure_file, load_object, validate_inputs  # noqa: E402

DEFAULT_AUTHOR = "db-check"
DEFAULT_TEMPLATE_VERSION = "v1.0"
DEFAULT_DATA_DIR = "/var/lib/mysql"


class _ArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        raise ReporterFailure(EXIT_PARAM_ERROR, f"参数错误: {message}")


def build_parser() -> _ArgumentParser:
    parser = _ArgumentParser(description="Generate report-meta.json from result and summary")
    parser.add_argument("--result", type=Path, required=True)
    parser.add_argument("--summary", type=Path, required=True)
    parser.add_argument("--mysql-version", required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--author", default=DEFAULT_AUTHOR)
    parser.add_argument("--data-dir", default=DEFAULT_DATA_DIR)
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
        meta = build_e2e_meta(result, summary, args.mysql_version, args.author, args.data_dir, args.version_label)
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


def build_e2e_meta(
    result: dict[str, Any],
    summary: dict[str, Any],
    mysql_version: str,
    author: str,
    data_dir: str,
    version_label: str,
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
            "document_name": f"MySQL {mysql_version} Docker E2E 巡检报告",
            "inspection_time": _inspection_time(collect_time),
            "issue_date": issue_date,
            "author": author,
            "version": version_label,
        },
        "change_log": [
            {
                "date": issue_date,
                "author": author,
                "version": version_label,
                "change": "基于 Docker e2e 真实采集结果生成 Markdown 巡检报告",
            }
        ],
        "review_log": [
            {"name": "待补充", "title": "待补充", "contact": "待补充", "email": "待补充"}
        ],
        "scope": {
            "inspection_target": f"Docker E2E MySQL {mysql_version}",
            "instances": [instance] if instance else [],
            "database_version": mysql_version,
            "architecture_role": architecture_role,
            "data_dir": data_dir,
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


if __name__ == "__main__":
    raise SystemExit(run())
