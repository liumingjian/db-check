"""GaussDB SQL-first database detail sections."""

from __future__ import annotations

from typing import Any

from reporter.content.gaussdb_config_sections import build_config_section
from reporter.content.gaussdb_section_utils import basic_info_rows, details_dict, domain_payload, domain_table, find_item, rows_payload, visible_items
from reporter.content.gaussdb_wdr_sections import build_wdr_section
from reporter.content.helpers import format_bytes, format_percent, full_table, key_value_table
from reporter.model.report_view import SectionBlock, TableBlock


def build_gaussdb_database_sections(result: dict[str, object]) -> tuple[SectionBlock, ...]:
    sections = (
        _overview_section(result),
        build_config_section(result),
        _storage_section(result),
        _connection_section(result),
        _transaction_section(result),
        _performance_section(result),
        _sql_analysis_section(result),
        _wdr_section(result),
    )
    return tuple(section for section in sections if section is not None)


def _overview_section(result: dict[str, object]) -> SectionBlock | None:
    tables: list[TableBlock] = []
    rows = basic_info_rows(result)
    if rows:
        tables.append(key_value_table("数据库概览", rows))
    items = visible_items(result, "basic_info") + visible_items(result, "cluster")
    if items:
        tables.append(domain_table("SQL 基础检查", items))
    tables.extend(_cluster_tables(result))
    tables.append(_sql_index_table(result))
    return SectionBlock(title="2.2.1 数据库概览", tables=tuple(tables)) if tables else None


def _cluster_tables(result: dict[str, object]) -> list[TableBlock]:
    tables: list[TableBlock] = []
    cluster_rows = _detail_rows(find_item(result, "CheckClusterState", "cluster"))
    if cluster_rows:
        tables.append(_row_table("集群节点元数据", ("node_name", "node_type", "node_host", "node_port"), ("节点", "类型", "主机", "端口"), cluster_rows))
    readonly = details_dict(find_item(result, "CheckReadonlyMode", "cluster")).get("readonly")
    if readonly not in ("", None):
        tables.append(key_value_table("只读模式", (("default_transaction_read_only", str(readonly)),)))
    return tables


def _sql_index_table(result: dict[str, object]) -> TableBlock:
    items = domain_payload(result, "sql_raw_index").get("items")
    rows = tuple(_sql_index_row(item) for item in items if isinstance(item, dict)) if isinstance(items, list) else ()
    if not rows:
        rows = (("SQL-first", "-", "-", "-", "run_dir/sql/"),)
    return full_table("SQL 原始结果索引", ("域", "检查项", "标签", "行数", "结果文件"), rows)


def _sql_index_row(item: dict[str, Any]) -> tuple[str, str, str, str, str]:
    return (
        str(item.get("domain") or ""),
        str(item.get("item") or ""),
        str(item.get("label") or ""),
        str(item.get("row_count") or 0),
        str(item.get("result_file") or ""),
    )


def _storage_section(result: dict[str, object]) -> SectionBlock | None:
    tables: list[TableBlock] = []
    items = visible_items(result, "storage")
    if items:
        tables.append(domain_table("存储容量检查", items))
    tables.extend((_tablespace_table(result), _database_size_table(result), _system_table(result)))
    return _section("2.2.3 存储容量", tables)


def _tablespace_table(result: dict[str, object]) -> TableBlock | None:
    rows = _detail_rows(find_item(result, "CheckTableSpace", "storage"))
    if not rows:
        return None
    return _row_table("表空间清单", ("tablespace", "location"), ("表空间", "位置"), rows)


def _database_size_table(result: dict[str, object]) -> TableBlock | None:
    rows = _detail_rows(find_item(result, "CheckKeyDBTableSize", "storage"))
    if not rows:
        return None
    rendered = tuple((str(row.get("database") or ""), format_bytes(row.get("size_bytes"))) for row in rows)
    return full_table("数据库容量", ("数据库", "大小"), rendered)


def _system_table(result: dict[str, object]) -> TableBlock | None:
    rows = details_dict(find_item(result, "CheckSysTable", "storage")).get("tables")
    if not isinstance(rows, list) or not rows:
        return None
    return _row_table("系统表Top20", ("schema", "table_name", "pages", "rows"), ("Schema", "系统表", "页数", "估算行数"), rows)


def _connection_section(result: dict[str, object]) -> SectionBlock | None:
    tables: list[TableBlock] = []
    items = visible_items(result, "connection")
    if items:
        tables.append(domain_table("连接会话检查", items))
    conn = details_dict(find_item(result, "CheckCurConnCount", "connection"))
    if conn:
        tables.append(key_value_table("当前连接数", _connection_rows(conn)))
    return _section("2.2.4 连接会话", tables)


def _connection_rows(details: dict[str, Any]) -> tuple[tuple[str, str], ...]:
    return (
        ("当前连接数", str(details.get("current_connections") or 0)),
        ("最大连接数", str(details.get("max_connections") or 0)),
        ("连接使用率", format_percent(details.get("usage_percent"))),
    )


def _transaction_section(result: dict[str, object]) -> SectionBlock | None:
    items = visible_items(result, "transactions")
    tables = [domain_table("事务与锁检查", items)] if items else []
    return _section("2.2.5 事务与锁", tables)


def _performance_section(result: dict[str, object]) -> SectionBlock | None:
    tables: list[TableBlock] = []
    items = visible_items(result, "performance")
    if items:
        tables.append(domain_table("性能统计检查", items))
    tables.extend((_db_stat_table(result), _bp_hit_ratio_table(result)))
    return _section("2.2.6 性能统计", tables)


def _db_stat_table(result: dict[str, object]) -> TableBlock | None:
    rows = _detail_rows(find_item(result, "CheckDBStat", "performance"))
    if not rows:
        return None
    fields = ("database", "numbackends", "xact_commit", "xact_rollback", "deadlocks")
    columns = ("数据库", "后端数", "提交事务", "回滚事务", "死锁")
    return _row_table("数据库运行统计", fields, columns, rows)


def _bp_hit_ratio_table(result: dict[str, object]) -> TableBlock | None:
    rows = _detail_rows(find_item(result, "CheckBPHitRatio", "performance"))
    if not rows:
        return None
    return _row_table("Buffer命中率", ("database", "hit_ratio"), ("数据库", "命中率"), rows)


def _sql_analysis_section(result: dict[str, object]) -> SectionBlock | None:
    tables: list[TableBlock] = []
    items = visible_items(result, "sql_analysis")
    if items:
        tables.append(domain_table("SQL 分析检查", items))
    tables.extend(_sql_governance_tables(result))
    return _section("2.2.7 SQL 分析", tables)


def _sql_governance_tables(result: dict[str, object]) -> list[TableBlock]:
    return [
        _summary_or_empty("无索引表汇总", rows_payload(result, "sql_analysis", "no_index_summary"), ("owner", "total_table_count", "no_index_count", "percentage"), ("所属用户", "总表数", "无索引表数", "占比(%)"), "未发现无索引表"),
        _summary_or_empty("无主键表汇总", rows_payload(result, "sql_analysis", "no_primary_key_summary"), ("owner", "total_table_count", "no_pk_count", "percentage"), ("所属用户", "总表数", "无主键表数", "占比(%)"), "未发现无主键表"),
        _detail_or_none("无主键表明细", rows_payload(result, "sql_analysis", "no_primary_key_detail"), ("owner", "table_name"), ("所属用户", "表名")),
        _summary_or_empty("统计信息缺失汇总", rows_payload(result, "sql_analysis", "no_statistics_summary"), ("tableowner", "total_table_count", "table_no_stat", "percentage"), ("所属用户", "总表数", "缺失统计信息表数", "占比(%)"), "未发现统计信息缺失表"),
        _detail_or_none("统计信息缺失明细", rows_payload(result, "sql_analysis", "no_statistics_detail"), ("schemaname", "tableowner", "tablename"), ("Schema", "所属用户", "表名")),
    ]


def _summary_or_empty(title: str, payload: dict[str, Any], fields: tuple[str, ...], columns: tuple[str, ...], empty_text: str) -> TableBlock:
    items = payload.get("items", [])
    if not isinstance(items, list) or not items:
        return key_value_table(title, (("检查结论", empty_text),))
    return _row_table(title, fields, columns, items)


def _detail_or_none(title: str, payload: dict[str, Any], fields: tuple[str, ...], columns: tuple[str, ...]) -> TableBlock | None:
    items = payload.get("items", [])
    if not isinstance(items, list) or not items:
        return None
    return _row_table(title, fields, columns, items)


def _detail_rows(item: dict[str, Any] | None) -> tuple[dict[str, Any], ...]:
    rows = details_dict(item).get("rows")
    if not isinstance(rows, list):
        return ()
    return tuple(row for row in rows if isinstance(row, dict))


def _row_table(title: str, fields: tuple[str, ...], columns: tuple[str, ...], rows: Any) -> TableBlock:
    rendered = tuple(tuple(str(row.get(field) or "") for field in fields) for row in rows if isinstance(row, dict))
    return full_table(title, columns, rendered)


def _section(title: str, tables: list[TableBlock | None]) -> SectionBlock | None:
    resolved = tuple(table for table in tables if table is not None)
    return SectionBlock(title=title, tables=resolved) if resolved else None


def _wdr_section(result: dict[str, object]) -> SectionBlock | None:
    db = result.get("db")
    if not isinstance(db, dict) or not isinstance(db.get("wdr"), dict):
        return None
    return build_wdr_section(result)
