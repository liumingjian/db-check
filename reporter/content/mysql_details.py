"""Assemble MySQL detail sections."""

from __future__ import annotations

from typing import Any

from reporter.content.os_sections import OSSectionOptions, build_os_section
from reporter.content.mysql_backup_details import build_backup_section
from reporter.content.mysql_basic_details import build_mysql_basic_info
from reporter.content.mysql_performance_details import build_mysql_performance
from reporter.model.report_view import SectionBlock


def build_detail_section(result: dict[str, Any], summary: dict[str, Any], meta: dict[str, Any]) -> SectionBlock:
    return SectionBlock(
        title="第二章 巡检明细",
        children=(
            build_os_section(result, OSSectionOptions(section_prefix="2.1", db_process_label="MySQL fd 使用率")),
            build_mysql_basic_info(result, summary, meta),
            build_mysql_performance(result),
            build_backup_section(result),
        ),
    )
