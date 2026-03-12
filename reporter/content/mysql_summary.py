"""MySQL chapter-1 builder via shared summary template."""

from __future__ import annotations

from typing import Any

from reporter.content.mysql_summary_text import (
    business_dimensions,
    conclusion_paragraphs,
    conclusion_rows,
    display_dimension_name,
    group_abnormal_items,
    health_summary,
    impact_analysis,
    risk_description,
)
from reporter.content.summary_builder import SummaryStrategy, build_summary_section as build_shared_summary_section
from reporter.model.report_view import SectionBlock

MYSQL_SUMMARY_STRATEGY = SummaryStrategy(
    business_dimensions=business_dimensions,
    display_dimension_name=display_dimension_name,
    group_abnormal_items=group_abnormal_items,
    health_summary=health_summary,
    risk_description=risk_description,
    impact_analysis=impact_analysis,
    conclusion_rows=conclusion_rows,
    conclusion_paragraphs=conclusion_paragraphs,
)


def build_summary_section(result: dict[str, Any], summary: dict[str, Any], meta: dict[str, Any]) -> SectionBlock:
    return build_shared_summary_section(result, summary, meta, MYSQL_SUMMARY_STRATEGY)
