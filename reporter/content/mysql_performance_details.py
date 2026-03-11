"""Build MySQL performance detail sections."""
from __future__ import annotations
from typing import Any
from reporter.content.helpers import compact_table, format_number, format_percent, full_table, key_value_table, row_value, unwrap_items
from reporter.content.mysql_performance_contract_sections import build_full_scan_tables_section, build_many_indexes_section, build_memory_gap_section, build_row_ops_top_tables_section, build_tmp_table_sqls_section, build_top_indexes_by_size, build_top_tables_by_io_section, build_wide_composite_indexes_section
from reporter.model.report_view import SectionBlock

def build_mysql_performance(result: dict[str, Any]) -> SectionBlock:
    return SectionBlock(
        title="2.3 数据库性能检查",
        children=(
            _innodb_info(result),
            _innodb_lock_waits(result),
            _metadata_locks(result),
            _connection_checks(result),
            _top_tables(result),
            build_top_indexes_by_size(result),
            _tables_without_pk(result),
            _mixed_engines(result),
            build_many_indexes_section(result),
            build_wide_composite_indexes_section(result),
            _wide_tables(result),
            build_top_tables_by_io_section(result),
            build_memory_gap_section(),
            _slow_sqls(result),
            _full_scan_sqls(result),
            build_full_scan_tables_section(result),
            build_tmp_table_sqls_section(result),
            build_row_ops_top_tables_section(result),
            _unused_indexes(result),
            _auto_increment_usage(result),
            _redundant_indexes(result),
        ),
    )


def _innodb_info(result: dict[str, Any]) -> SectionBlock:
    innodb = _performance_payload(result).get("innodb", {})
    if not isinstance(innodb, dict) or not innodb:
        table = full_table("InnoDB详细信息", ("说明",), (("当前 contracts 未输出 InnoDB 汇总信息。",),), status="missing")
        return SectionBlock(title="2.3.1 InnoDB详细信息", status="missing", tables=(table,))
    rows = tuple((key, format_number(value)) for key, value in sorted(innodb.items()))
    return SectionBlock(
        title="2.3.1 InnoDB详细信息",
        tables=(key_value_table("InnoDB详细信息", rows), _top_wait_events_table(result)),
    )


def _innodb_lock_waits(result: dict[str, Any]) -> SectionBlock:
    perf = _performance_payload(result)
    rows = (
        ("current_lock_waits", format_number(perf.get("current_lock_waits"))),
        ("row_lock_waits_delta", format_number(perf.get("row_lock_waits_delta"))),
        ("long_transactions", format_number(perf.get("long_transactions"))),
        ("latest_deadlock_info", str(perf.get("latest_deadlock_info", ""))),
    )
    return SectionBlock(
        title="2.3.2 InnoDB锁等待",
        tables=(key_value_table("InnoDB锁等待", rows), _row_lock_time_table(result)),
    )


def _metadata_locks(result: dict[str, Any]) -> SectionBlock:
    rows = tuple(
        (
            str(row_value(item, "PROCESSLIST_ID", "processlist_id", "thread_id")),
            str(row_value(item, "OBJECT_TYPE", "object_type")),
            str(row_value(item, "OBJECT_SCHEMA", "object_schema")),
            str(row_value(item, "OBJECT_NAME", "object_name")),
            str(row_value(item, "LOCK_TYPE", "lock_type")),
            str(row_value(item, "LOCK_DURATION", "lock_duration")),
            str(row_value(item, "LOCK_STATUS", "lock_status")),
        )
        for item in unwrap_items(_performance_payload(result).get("metadata_lock_waits"))[:10]
    )
    table = compact_table(
        "元数据锁信息",
        ("proc_id", "obj_type", "obj_sch", "obj_name", "lck_type", "lck_dur", "lck_stat"),
        rows or (("-", "-", "-", "-", "-", "-", "无等待"),),
        (
            ("proc_id", "会话 ID（processlist/thread 标识）"),
            ("obj_type", "锁对象类型"),
            ("obj_sch", "对象所属库名"),
            ("obj_name", "对象名"),
            ("lck_type", "锁类型"),
            ("lck_dur", "锁持续范围"),
            ("lck_stat", "锁状态"),
        ),
    )
    return SectionBlock(title="2.3.3 元数据锁信息", tables=(table,))


def _connection_checks(result: dict[str, Any]) -> SectionBlock:
    perf = _performance_payload(result)
    rows = (
        ("threads_running", format_number(perf.get("threads_running"))),
        ("connection_usage_percent", format_percent(perf.get("connection_usage_percent"))),
        ("max_used_connections_ratio", format_percent(perf.get("max_used_connections_ratio"))),
        ("thread_cache_hit_ratio", format_percent(perf.get("thread_cache_hit_ratio"))),
    )
    return SectionBlock(title="2.3.4 连接数检查", tables=(key_value_table("连接数检查", rows),))


def _top_tables(result: dict[str, Any]) -> SectionBlock:
    rows = tuple(
        (
            str(row_value(item, "TABLE_SCHEMA", "table_schema")),
            str(row_value(item, "TABLE_NAME", "table_name")),
            _size_mb_text(row_value(item, "size_mb", "SIZE_MB")),
            format_number(row_value(item, "TABLE_ROWS", "table_rows")),
        )
        for item in unwrap_items(result.get("db", {}).get("storage", {}).get("top_tables"))[:10]
    )
    table = compact_table(
        "占用空间top 10的表",
        ("sch", "tbl", "size", "rows"),
        rows or (("-", "-", "无", "-"),),
        (("sch", "库名"), ("tbl", "表名"), ("size", "表大小"), ("rows", "估算行数")),
    )
    return SectionBlock(title="2.3.5 占用空间top 10的表", tables=(table,))


def _tables_without_pk(result: dict[str, Any]) -> SectionBlock:
    rows = tuple(
        (
            str(row_value(item, "TABLE_SCHEMA", "table_schema")),
            str(row_value(item, "TABLE_NAME", "table_name")),
            "-",
        )
        for item in unwrap_items(result.get("db", {}).get("storage", {}).get("tables_without_pk"))[:10]
    )
    table = full_table("没有主键或唯一键的表", ("库", "表", "引擎"), rows or (("-", "-", "无"),))
    return SectionBlock(title="2.3.7 没有主键或唯一键的表", tables=(table,))


def _mixed_engines(result: dict[str, Any]) -> SectionBlock:
    rows = tuple(
        (
            str(row_value(item, "TABLE_SCHEMA", "table_schema")),
            str(row_value(item, "TABLE_NAME", "table_name")),
            str(row_value(item, "ENGINE", "engine")),
        )
        for item in unwrap_items(result.get("db", {}).get("storage", {}).get("mixed_engines"))[:10]
    )
    table = full_table("非Innodb引擎的数据对象", ("库", "表", "引擎"), rows or (("-", "-", "无"),))
    return SectionBlock(title="2.3.8 非Innodb引擎的数据对象", tables=(table,))


def _wide_tables(result: dict[str, Any]) -> SectionBlock:
    rows = tuple(
        (
            str(row_value(item, "TABLE_SCHEMA", "table_schema")),
            str(row_value(item, "TABLE_NAME", "table_name")),
            format_number(row_value(item, "COLUMN_COUNT", "column_count")),
        )
        for item in unwrap_items(result.get("db", {}).get("storage", {}).get("wide_tables"))[:10]
    )
    table = full_table("单张表字段个数大于50的对象", ("库", "表", "字段数"), rows or (("-", "-", "无"),))
    return SectionBlock(title="2.3.11 单张表字段个数大于50的对象", tables=(table,))


def _slow_sqls(result: dict[str, Any]) -> SectionBlock:
    rows = tuple(
        (
            str(row_value(item, "DIGEST_TEXT", "digest_text")),
            format_number(row_value(item, "COUNT_STAR", "count_star", "exec_count")),
            format_number(row_value(item, "avg_time_ms", "avg_latency_ms")),
            format_number(row_value(item, "total_time_ms", "total_latency_ms")),
        )
        for item in unwrap_items(result.get("db", {}).get("sql_analysis", {}).get("top_sql_by_time"))[:10]
    )
    table = compact_table(
        "慢SQL top10",
        ("sql", "exec", "avg_ms", "ttl_ms"),
        rows or (("无", "-", "-", "-"),),
        (("sql", "SQL 摘要"), ("exec", "执行次数"), ("avg_ms", "平均耗时(ms)"), ("ttl_ms", "总耗时(ms)")),
    )
    return SectionBlock(title="2.3.14 慢SQL top10", tables=(table,))


def _full_scan_sqls(result: dict[str, Any]) -> SectionBlock:
    analysis = result.get("db", {}).get("sql_analysis", {})
    if not isinstance(analysis, dict):
        analysis = {}
    full_scan_rows = tuple(
        (
            str(row_value(item, "DIGEST_TEXT", "digest_text")),
            format_number(row_value(item, "COUNT_STAR", "count_star", "SUM_NO_INDEX_USED")),
            format_number(row_value(item, "SUM_ROWS_EXAMINED", "rows_examined_avg")),
        )
        for item in unwrap_items(analysis.get("full_scan_sqls"))[:10]
    )
    no_index_rows = tuple(
        (
            str(row_value(item, "DIGEST_TEXT", "digest_text")),
            format_number(row_value(item, "COUNT_STAR", "count_star", "SUM_NO_INDEX_USED")),
            format_number(row_value(item, "SUM_ROWS_EXAMINED", "rows_examined_avg")),
        )
        for item in unwrap_items(analysis.get("no_index_sqls"))[:10]
    )
    return SectionBlock(
        title="2.3.15 全表扫描的SQL top10",
        tables=(
            full_table("全表扫描的SQL top10", ("SQL", "执行次数", "平均扫描行数"), full_scan_rows or (("无", "-", "-"),)),
            full_table("无索引SQL top10", ("SQL", "执行次数", "扫描行数"), no_index_rows or (("无", "-", "-"),)),
        ),
    )


def _unused_indexes(result: dict[str, Any]) -> SectionBlock:
    rows = tuple(
        (
            str(row_value(item, "OBJECT_SCHEMA", "object_schema")),
            str(row_value(item, "OBJECT_NAME", "object_name")),
            str(row_value(item, "INDEX_NAME", "index_name")),
        )
        for item in unwrap_items(result.get("db", {}).get("storage", {}).get("unused_indexes"))[:10]
    )
    table = full_table("未使用的索引", ("库", "表", "索引"), rows or (("-", "-", "无"),))
    return SectionBlock(title="2.3.19 未使用的索引", tables=(table,))


def _auto_increment_usage(result: dict[str, Any]) -> SectionBlock:
    rows = tuple(
        (
            str(row_value(item, "TABLE_SCHEMA", "table_schema")),
            str(row_value(item, "TABLE_NAME", "table_name")),
            format_percent(row_value(item, "usage_percent", "USAGE_PERCENT")),
        )
        for item in unwrap_items(result.get("db", {}).get("storage", {}).get("auto_increment_usage"))[:10]
    )
    table = full_table("自增值使用率top10", ("库", "表", "使用率"), rows or (("-", "-", "无"),))
    return SectionBlock(title="2.3.20 自增值使用率top10", tables=(table,))


def _redundant_indexes(result: dict[str, Any]) -> SectionBlock:
    rows = tuple(
        (
            str(row_value(item, "TABLE_SCHEMA", "table_schema", "object_schema")),
            str(row_value(item, "TABLE_NAME", "table_name", "object_name")),
            str(row_value(item, "INDEX_NAME", "index_name")),
            str(row_value(item, "REDUNDANT_WITH", "covered_by", "redundant_with")),
        )
        for item in unwrap_items(result.get("db", {}).get("storage", {}).get("redundant_indexes"))[:10]
    )
    table = compact_table(
        "冗余索引",
        ("sch", "tbl", "idx", "cover"),
        rows or (("-", "-", "无", "-"),),
        (("sch", "库名"), ("tbl", "表名"), ("idx", "冗余索引"), ("cover", "被覆盖索引")),
    )
    return SectionBlock(title="2.3.21 冗余索引", tables=(table,))


def _performance_payload(result: dict[str, Any]) -> dict[str, Any]:
    performance = result.get("db", {}).get("performance")
    if isinstance(performance, dict):
        return performance
    return {}


def _top_wait_events_table(result: dict[str, Any]):
    rows = tuple(
        (
            str(row_value(item, "EVENT_NAME", "event_name")),
            format_number(row_value(item, "COUNT_STAR", "count_star")),
            format_number(row_value(item, "total_wait_ms")),
        )
        for item in unwrap_items(_performance_payload(result).get("top_wait_events"))[:10]
    )
    return compact_table(
        "Top等待事件",
        ("evt", "cnt", "ttl_ms"),
        rows or (("无", "-", "-"),),
        (("evt", "等待事件名称"), ("cnt", "事件次数"), ("ttl_ms", "累计等待时间(ms)")),
    )


def _row_lock_time_table(result: dict[str, Any]):
    stats = _performance_payload(result).get("row_lock_time_stats", {})
    if not isinstance(stats, dict):
        stats = {}
    rows = (
        ("waits", format_number(stats.get("waits"))),
        ("avg_ms", format_number(stats.get("avg_ms"))),
        ("max_ms", format_number(stats.get("max_ms"))),
        ("total_ms", format_number(stats.get("total_ms"))),
    )
    return key_value_table("行锁等待耗时统计", rows)


def _size_mb_text(value: Any) -> str:
    if value is None or value == "":
        return ""
    return f"{value} MB"
