"""Dispatch report view building by database type."""

from __future__ import annotations

from typing import Any

from reporter.content.mysql_report_builder import build_mysql_report_view
from reporter.content.oracle_report_builder import build_oracle_report_view
from reporter.model.errors import EXIT_CONTRACT_ERROR, ReporterFailure
from reporter.model.report_view import ReportView


def build_report_view(result: dict[str, Any], summary: dict[str, Any], meta: dict[str, Any]) -> ReportView:
    db_type = _db_type(result)
    if db_type == "mysql":
        return build_mysql_report_view(result, summary, meta)
    if db_type == "oracle":
        return build_oracle_report_view(result, summary, meta)
    raise ReporterFailure(EXIT_CONTRACT_ERROR, f"暂不支持的 db_type: {db_type}")


def _db_type(result: dict[str, Any]) -> str:
    meta = result.get("meta", {}) if isinstance(result.get("meta"), dict) else {}
    return str(meta.get("db_type") or "").strip().lower()
