"""MySQL report view builder for preview and future docx rendering."""

from __future__ import annotations

from typing import Any

from reporter.content.helpers import full_table, key_value_table
from reporter.content.mysql_details import build_detail_section
from reporter.content.mysql_summary import build_summary_section
from reporter.model.report_view import ReportView, SectionBlock


def build_mysql_report_view(result: dict[str, Any], summary: dict[str, Any], meta: dict[str, Any]) -> ReportView:
    generated_at = str(summary.get("generated_at") or result.get("meta", {}).get("collect_time", ""))
    title = _resolve_title(meta)
    sections = (
        _build_doc_control(meta),
        build_summary_section(result, summary, meta),
        build_detail_section(result, summary, meta),
    )
    return ReportView(title=title, generated_at=generated_at, sections=sections)


def _resolve_title(meta: dict[str, Any]) -> str:
    doc_info = meta.get("doc_info", {}) if isinstance(meta.get("doc_info"), dict) else {}
    return str(doc_info.get("document_name", "MySQL巡检报告"))


def _build_doc_control(meta: dict[str, Any]) -> SectionBlock:
    doc_info = meta.get("doc_info", {}) if isinstance(meta.get("doc_info"), dict) else {}
    change_log = meta.get("change_log") if isinstance(meta.get("change_log"), list) else []
    review_log = meta.get("review_log") if isinstance(meta.get("review_log"), list) else []
    info_section = _doc_info_section(doc_info)
    change_section = _list_section(
        "修改记录",
        ("日期", "作者", "版本", "修改记录"),
        change_log,
        ("date", "author", "version", "change"),
    )
    review_section = _list_section(
        "审阅记录",
        ("姓名", "职位", "联系方式", "邮箱"),
        review_log,
        ("name", "title", "contact", "email"),
    )
    return SectionBlock(title="文档控制", children=(info_section, change_section, review_section))


def _doc_info_section(doc_info: dict[str, Any]) -> SectionBlock:
    rows = (
        ("文档名称", str(doc_info.get("document_name", "待补充"))),
        ("巡检时间", str(doc_info.get("inspection_time", "待补充"))),
        ("出具日期", str(doc_info.get("issue_date", "待补充"))),
        ("巡检人员", str(doc_info.get("author", "待补充"))),
        ("版本", str(doc_info.get("version", "v1.0"))),
    )
    return SectionBlock(title="文档信息", status="external", tables=(key_value_table("文档信息", rows, status="external"),))


def _list_section(title: str, columns: tuple[str, ...], items: list[dict[str, Any]], fields: tuple[str, ...]) -> SectionBlock:
    rows = tuple(tuple(str(item.get(field, "")) for field in fields) for item in items if isinstance(item, dict))
    if not rows:
        rows = (("待补充",) * len(columns),)
    table = full_table(title, columns, rows, status="external")
    return SectionBlock(title=title, status="external", tables=(table,))
