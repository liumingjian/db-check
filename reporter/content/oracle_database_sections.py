"""Oracle database detail section assembly."""

from __future__ import annotations

from reporter.content.oracle_backup_sections import build_backup_section
from reporter.content.oracle_basic_sections import build_basic_and_config_section
from reporter.content.oracle_performance_sections import build_performance_and_session_section
from reporter.content.oracle_security_sections import build_security_section
from reporter.content.oracle_sql_sections import build_sql_analysis_section
from reporter.content.oracle_storage_sections import build_storage_and_log_section
from reporter.model.report_view import SectionBlock


def build_oracle_database_sections(result: dict[str, object]) -> tuple[SectionBlock, ...]:
    return (
        build_basic_and_config_section(result),
        build_storage_and_log_section(result),
        build_performance_and_session_section(result),
        build_sql_analysis_section(result),
        build_security_section(result),
        build_backup_section(result),
    )
