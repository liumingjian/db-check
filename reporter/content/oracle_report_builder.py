"""Oracle report view builder."""

from __future__ import annotations

from typing import Any

from reporter.content.helpers import full_table, key_value_table
from reporter.content.oracle_database_sections import build_oracle_database_sections
from reporter.content.oracle_summary_text import (
    business_dimensions,
    conclusion_paragraphs,
    conclusion_rows,
    display_dimension_name,
    group_abnormal_items,
    health_summary,
    impact_analysis,
    risk_description,
)
from reporter.content.os_sections import OSSectionOptions, build_os_section
from reporter.content.summary_builder import SummaryStrategy, build_summary_section
from reporter.model.report_view import ReportView, SectionBlock

ORACLE_SUMMARY_STRATEGY = SummaryStrategy(
    business_dimensions=business_dimensions,
    display_dimension_name=display_dimension_name,
    group_abnormal_items=group_abnormal_items,
    health_summary=health_summary,
    risk_description=risk_description,
    impact_analysis=impact_analysis,
    conclusion_rows=conclusion_rows,
    conclusion_paragraphs=conclusion_paragraphs,
)


def build_oracle_report_view(result: dict[str, Any], summary: dict[str, Any], meta: dict[str, Any]) -> ReportView:
    generated_at = str(summary.get("generated_at") or result.get("meta", {}).get("collect_time", ""))
    title = _resolve_title(meta)
    sections = (
        _build_doc_control(meta),
        build_summary_section(result, summary, meta, ORACLE_SUMMARY_STRATEGY),
        _build_detail_section(result),
    )
    return ReportView(title=title, generated_at=generated_at, sections=sections)


def _resolve_title(meta: dict[str, Any]) -> str:
    doc_info = meta.get("doc_info", {}) if isinstance(meta.get("doc_info"), dict) else {}
    return str(doc_info.get("document_name", "Oracle巡检报告"))


def _build_doc_control(meta: dict[str, Any]) -> SectionBlock:
    doc_info = meta.get("doc_info", {}) if isinstance(meta.get("doc_info"), dict) else {}
    change_log = meta.get("change_log") if isinstance(meta.get("change_log"), list) else []
    review_log = meta.get("review_log") if isinstance(meta.get("review_log"), list) else []
    return SectionBlock(
        title="文档控制",
        children=(
            SectionBlock(title="文档信息", status="external", tables=(_doc_info_table(doc_info),)),
            SectionBlock(title="修改记录", status="external", tables=(_list_table("修改记录", ("日期", "作者", "版本", "修改记录"), change_log, ("date", "author", "version", "change")),)),
            SectionBlock(title="审阅记录", status="external", tables=(_list_table("审阅记录", ("姓名", "职位", "联系方式", "邮箱"), review_log, ("name", "title", "contact", "email")),)),
        ),
    )


def _doc_info_table(doc_info: dict[str, Any]):
    rows = (
        ("文档名称", str(doc_info.get("document_name", "待补充"))),
        ("巡检时间", str(doc_info.get("inspection_time", "待补充"))),
        ("出具日期", str(doc_info.get("issue_date", "待补充"))),
        ("巡检人员", str(doc_info.get("author", "待补充"))),
        ("版本", str(doc_info.get("version", "v1.0"))),
    )
    return key_value_table("文档信息", rows, status="external")


def _list_table(title: str, columns: tuple[str, ...], items: list[dict[str, Any]], fields: tuple[str, ...]):
    rows = tuple(tuple(str(item.get(field, "")) for field in fields) for item in items if isinstance(item, dict))
    if not rows:
        rows = (("待补充",) * len(columns),)
    return full_table(title, columns, rows, status="external")


def _build_detail_section(result: dict[str, Any]) -> SectionBlock:
    return SectionBlock(
        title="第二章 巡检明细",
        children=(
            build_os_section(
                result,
                OSSectionOptions(section_prefix="2.1", db_process_label="数据库进程 fd 使用率", include_db_process_fd=False),
            ),
            SectionBlock(title="2.2 数据库指标", children=build_oracle_database_sections(result)),
        ),
    )
