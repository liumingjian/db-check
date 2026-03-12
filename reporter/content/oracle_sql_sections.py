"""Oracle SQL analysis report sections."""

from __future__ import annotations

from reporter.content.helpers import format_number, format_percent, full_table, key_value_table, row_value, unwrap_items
from reporter.content.oracle_section_utils import db_payload
from reporter.model.report_view import SectionBlock, TableBlock


def build_sql_analysis_section(result: dict[str, object]) -> SectionBlock:
    sql_analysis = db_payload(result, "sql_analysis")
    return SectionBlock(
        title="2.2.4 SQL 分析",
        tables=(
            key_value_table("SQL 指标摘要", _summary_rows(sql_analysis)),
            _table(sql_analysis, "top_sql_by_elapsed_time", "Top SQL（按耗时）", ("SQL", "耗时(秒)", "CPU(秒)", "执行次数"), ("sql_text", "elapsed_time_sec", "cpu_time_sec", "executions")),
            _table(sql_analysis, "top_sql_by_buffer_gets", "Top SQL（按逻辑读）", ("SQL", "逻辑读", "物理读", "执行次数"), ("sql_text", "buffer_gets", "disk_reads", "executions")),
            _table(sql_analysis, "top_sql_by_disk_reads", "Top SQL（按物理读）", ("SQL", "物理读", "逻辑读", "执行次数"), ("sql_text", "disk_reads", "buffer_gets", "executions")),
            _table(sql_analysis, "top_sql_by_executions", "Top SQL（按执行次数）", ("SQL", "执行次数", "逻辑读", "物理读"), ("sql_text", "executions", "buffer_gets", "disk_reads")),
            _table(sql_analysis, "high_parse_count_sql", "高解析SQL", ("SQL_ID", "执行次数", "解析次数", "版本数", "共享内存", "SQL"), ("sql_id", "executions", "parse_calls", "version_count", "sharable_mem", "sql_text")),
            _table(sql_analysis, "high_version_count_sql", "高版本SQL", ("SQL_ID", "版本数", "执行次数", "解析次数", "共享内存", "SQL"), ("sql_id", "version_count", "executions", "parse_calls", "sharable_mem", "sql_text")),
        ),
    )


def _summary_rows(sql_analysis: dict[str, object]) -> tuple[tuple[str, str], ...]:
    return (
        ("可复用SQL占比", format_percent(sql_analysis.get("sql_with_executions_ratio_pct"))),
        ("可复用SQL内存占比", format_percent(sql_analysis.get("memory_for_sql_with_executions_ratio_pct"))),
        ("高解析SQL数量", format_number(len(unwrap_items(sql_analysis.get("high_parse_count_sql"))), 0)),
        ("高版本SQL数量", format_number(len(unwrap_items(sql_analysis.get("high_version_count_sql"))), 0)),
    )


def _table(sql_analysis: dict[str, object], key: str, title: str, columns: tuple[str, ...], fields: tuple[str, ...]) -> TableBlock:
    rows = tuple(tuple(_format_value(row_value(item, field)) for field in fields) for item in unwrap_items(sql_analysis.get(key)))
    return full_table(title, columns, rows or (("待补充",) + ("",) * (len(columns) - 1),))


def _format_value(value: object) -> str:
    if isinstance(value, (int, float)):
        return format_number(value, 0 if isinstance(value, int) else 2)
    return "" if value is None else str(value)
