"""Additional contract-backed MySQL performance sections."""

from __future__ import annotations

from typing import Any

from reporter.content.helpers import compact_table, format_number, full_table, row_value, unwrap_items
from reporter.content.mysql_gap_notes import MEMORY_DISTRIBUTION_GAP
from reporter.model.report_view import SectionBlock


def build_top_indexes_by_size(result: dict[str, Any]) -> SectionBlock:
    rows = tuple(
        (
            str(row_value(item, "TABLE_SCHEMA", "table_schema")),
            str(row_value(item, "TABLE_NAME", "table_name")),
            str(row_value(item, "INDEX_NAME", "index_name")),
            _size_mb_text(row_value(item, "size_mb", "SIZE_MB")),
        )
        for item in _storage_rows(result, "top_indexes_by_size")[:10]
    )
    table = compact_table(
        "占用空间top 10的索引",
        ("sch", "tbl", "idx", "size"),
        rows or (("-", "-", "无", "-"),),
        (("sch", "库名"), ("tbl", "表名"), ("idx", "索引名"), ("size", "索引大小")),
    )
    return SectionBlock(title="2.3.6 占用空间top 10的索引", tables=(table,))


def build_many_indexes_section(result: dict[str, Any]) -> SectionBlock:
    rows = tuple(
        (
            str(row_value(item, "TABLE_SCHEMA", "table_schema")),
            str(row_value(item, "TABLE_NAME", "table_name")),
            format_number(row_value(item, "INDEX_COUNT", "index_count")),
        )
        for item in _storage_rows(result, "tables_with_many_indexes")[:10]
    )
    table = full_table("单张表超过6个索引的对象", ("库", "表", "索引数"), rows or (("-", "-", "无"),))
    return SectionBlock(title="2.3.9 单张表超过6个索引的对象", tables=(table,))


def build_wide_composite_indexes_section(result: dict[str, Any]) -> SectionBlock:
    rows = tuple(
        (
            str(row_value(item, "TABLE_SCHEMA", "table_schema")),
            str(row_value(item, "TABLE_NAME", "table_name")),
            str(row_value(item, "INDEX_NAME", "index_name")),
            format_number(row_value(item, "COLUMN_COUNT", "column_count")),
        )
        for item in _storage_rows(result, "wide_composite_indexes")[:10]
    )
    table = full_table("联合索引的字段个数大于4的对象", ("库", "表", "索引", "字段数"), rows or (("-", "-", "无", "-"),))
    return SectionBlock(title="2.3.10 联合索引的字段个数大于4的对象", tables=(table,))


def build_top_tables_by_io_section(result: dict[str, Any]) -> SectionBlock:
    rows = tuple(
        (
            str(row_value(item, "OBJECT_SCHEMA", "object_schema")),
            str(row_value(item, "OBJECT_NAME", "object_name")),
            format_number(row_value(item, "read_ops", "READ_OPS")),
            format_number(row_value(item, "write_ops", "WRITE_OPS")),
            format_number(row_value(item, "total_wait_ms", "TOTAL_WAIT_MS")),
        )
        for item in _performance_rows(result, "top_tables_by_io")[:10]
    )
    table = compact_table(
        "物理IO top 10的表",
        ("sch", "tbl", "rd", "wr", "ttl_ms"),
        rows or (("-", "-", "-", "-", "无"),),
        (("sch", "库名"), ("tbl", "表名"), ("rd", "读操作次数"), ("wr", "写操作次数"), ("ttl_ms", "累计等待时间(ms)")),
    )
    return SectionBlock(title="2.3.12 物理IO top 10的表", tables=(table,))


def build_memory_gap_section() -> SectionBlock:
    table = full_table("数据库内存分布top 10", ("说明",), ((MEMORY_DISTRIBUTION_GAP,),), status="missing", note=MEMORY_DISTRIBUTION_GAP)
    return SectionBlock(title="2.3.13 数据库内存分布top 10", status="missing", tables=(table,))


def build_full_scan_tables_section(result: dict[str, Any]) -> SectionBlock:
    rows = tuple(
        (
            str(row_value(item, "OBJECT_SCHEMA", "object_schema")),
            str(row_value(item, "OBJECT_NAME", "object_name")),
            format_number(row_value(item, "read_ops", "READ_OPS")),
            format_number(row_value(item, "read_wait_ms", "READ_WAIT_MS")),
        )
        for item in _performance_rows(result, "full_scan_tables")[:10]
    )
    table = full_table("全表扫描的表top10", ("库", "表", "扫描次数", "累计等待(ms)"), rows or (("-", "-", "无", "-"),))
    return SectionBlock(title="2.3.16 全表扫描的表top10", tables=(table,))


def build_tmp_table_sqls_section(result: dict[str, Any]) -> SectionBlock:
    rows = tuple(
        (
            str(row_value(item, "DIGEST_TEXT", "digest_text")),
            format_number(row_value(item, "COUNT_STAR", "count_star")),
            format_number(row_value(item, "SUM_CREATED_TMP_TABLES", "sum_created_tmp_tables")),
            format_number(row_value(item, "SUM_CREATED_TMP_DISK_TABLES", "sum_created_tmp_disk_tables")),
        )
        for item in _sql_rows(result, "tmp_table_sqls")[:10]
    )
    table = compact_table(
        "使用临时表的SQL top10",
        ("sql", "exec", "tmp", "disk_tmp"),
        rows or (("无", "-", "-", "-"),),
        (("sql", "SQL 摘要"), ("exec", "执行次数"), ("tmp", "临时表次数"), ("disk_tmp", "磁盘临时表次数")),
    )
    return SectionBlock(title="2.3.17 使用临时表的SQL top10", tables=(table,))


def build_row_ops_top_tables_section(result: dict[str, Any]) -> SectionBlock:
    rows = tuple(
        (
            str(row_value(item, "OBJECT_SCHEMA", "object_schema")),
            str(row_value(item, "OBJECT_NAME", "object_name")),
            format_number(row_value(item, "FETCH_OPS", "fetch_ops")),
            format_number(row_value(item, "INSERT_OPS", "insert_ops")),
            format_number(row_value(item, "UPDATE_OPS", "update_ops")),
            format_number(row_value(item, "DELETE_OPS", "delete_ops")),
            format_number(row_value(item, "TOTAL_OPS", "total_ops")),
        )
        for item in _performance_rows(result, "row_ops_top_tables")[:10]
    )
    table = compact_table(
        "行操作次数top10",
        ("sch", "tbl", "fet", "ins", "upd", "del", "ttl"),
        rows or (("-", "-", "-", "-", "-", "-", "无"),),
        (("sch", "库名"), ("tbl", "表名"), ("fet", "读行次数"), ("ins", "插入次数"), ("upd", "更新次数"), ("del", "删除次数"), ("ttl", "总操作次数")),
    )
    return SectionBlock(title="2.3.18 行操作次数top10", tables=(table,))


def _storage_rows(result: dict[str, Any], key: str) -> tuple[dict[str, Any], ...]:
    return unwrap_items(result.get("db", {}).get("storage", {}).get(key))


def _performance_rows(result: dict[str, Any], key: str) -> tuple[dict[str, Any], ...]:
    return unwrap_items(result.get("db", {}).get("performance", {}).get(key))


def _sql_rows(result: dict[str, Any], key: str) -> tuple[dict[str, Any], ...]:
    return unwrap_items(result.get("db", {}).get("sql_analysis", {}).get(key))


def _size_mb_text(value: Any) -> str:
    if value in ("", None):
        return ""
    return f"{value} MB"
