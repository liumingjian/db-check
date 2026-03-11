"""Build MySQL basic information detail sections."""

from __future__ import annotations

from typing import Any

from reporter.content.helpers import (
    compact_table,
    first_item,
    format_bytes,
    format_number,
    format_percent,
    format_time,
    format_uptime,
    full_table,
    key_value_table,
    na_table,
    row_value,
    unwrap_items,
)
from reporter.model.report_view import SectionBlock

RECENT_ERROR_COLUMNS = ("logged", "prio", "subsys", "detail")
RECENT_ERROR_FIELD_NOTES = (
    ("logged", "日志记录时间"),
    ("prio", "告警级别"),
    ("subsys", "所属子系统"),
    ("detail", "告警详情"),
)


def build_mysql_basic_info(result: dict[str, Any], summary: dict[str, Any], meta: dict[str, Any]) -> SectionBlock:
    return SectionBlock(
        title="2.2 MySQL基础信息",
        children=(
            _basic_instance_info(result, meta),
            _replication_info(result, summary),
            _config_info(result),
            _capacity_info(result),
            _user_info(result),
            _object_counts(result),
            _thread_info(result),
            _file_info(result),
            _status_info(result),
        ),
    )


def _basic_instance_info(result: dict[str, Any], meta: dict[str, Any]) -> SectionBlock:
    result_meta = result.get("meta", {}) if isinstance(result.get("meta"), dict) else {}
    scope = meta.get("scope", {}) if isinstance(meta.get("scope"), dict) else {}
    rows = (
        ("实例地址", _display_value(str(result_meta.get("db_host", "")))),
        ("端口", _display_value(str(result_meta.get("db_port", "")))),
        ("MySQL版本", _display_value(str(scope.get("database_version", "待补充")))),
        ("运行时间", _display_value(format_uptime(result.get("db", {}).get("basic_info", {}).get("uptime_seconds")))),
        ("数据目录", _display_value(str(scope.get("data_dir", "待补充")))),
    )
    table = key_value_table("实例基础信息", rows, status="external")
    return SectionBlock(title="2.2.1 实例基础信息", tables=(table,))


def _replication_info(result: dict[str, Any], summary: dict[str, Any]) -> SectionBlock:
    replication = result.get("db", {}).get("replication", {}) if isinstance(result.get("db", {}).get("replication"), dict) else {}
    na_ids = {item.get("check_id") for item in summary.get("na_items", []) if isinstance(item, dict)}
    if "2.0" in na_ids or not replication.get("enabled"):
        table = na_table("主从集群状态", "当前实例未配置复制，本节按不适用处理。")
        return SectionBlock(title="2.2.2 主从集群状态", status="na", tables=(table,))
    snapshot = replication.get("replica_status_snapshot", {}) if isinstance(replication.get("replica_status_snapshot"), dict) else {}
    rows = (
        ("role", "Replica" if snapshot.get("Master_Host") else "Primary/Unknown"),
        ("Master_Host", str(snapshot.get("Master_Host", ""))),
        ("Master_Port", str(snapshot.get("Master_Port", ""))),
        ("Slave_IO_Running", str(replication.get("io_thread_running", ""))),
        ("Slave_SQL_Running", str(snapshot.get("Slave_SQL_Running", ""))),
        ("Seconds_Behind_Master", format_number(replication.get("seconds_behind_master"))),
    )
    return SectionBlock(title="2.2.2 主从集群状态", tables=(key_value_table("主从集群状态", rows),))


def _config_info(result: dict[str, Any]) -> SectionBlock:
    config = result.get("db", {}).get("config_check", {}) if isinstance(result.get("db", {}).get("config_check"), dict) else {}
    selected_keys = _preferred_config_keys(config)
    rows = tuple((key, _display_value(format_number(config.get(key)))) for key in selected_keys)
    note = "仅展示最能反映当前实例参数状态的关键配置项。"
    return SectionBlock(title="2.2.3 mysql关键参数", tables=(key_value_table("mysql关键参数", rows),), note=note)


def _preferred_config_keys(config: dict[str, Any]) -> tuple[str, ...]:
    preferred = (
        "slow_query_log",
        "long_query_time",
        "max_connections",
        "sync_binlog",
        "innodb_flush_log_at_trx_commit",
        "innodb_flush_method",
        "innodb_buffer_pool_size",
        "innodb_io_capacity",
        "character_set",
        "transaction_isolation",
        "local_infile",
        "performance_schema",
    )
    existing = [key for key in preferred if key in config]
    if existing:
        return tuple(existing)
    return tuple(sorted(config.keys())[:12])


def _capacity_info(result: dict[str, Any]) -> SectionBlock:
    storage = result.get("db", {}).get("storage", {}) if isinstance(result.get("db", {}).get("storage"), dict) else {}
    db_sizes = unwrap_items(storage.get("database_sizes"))
    top_db = first_item(list(db_sizes), {}) or {}
    rows = (
        ("数据库数量", format_number(len(db_sizes))),
        ("最大库名", _display_value(str(row_value(top_db, "TABLE_SCHEMA", "table_schema", "schema_name")))),
        ("最大库容量", _display_value(_size_mb_text(row_value(top_db, "size_mb", "SIZE_MB")))),
        ("binlog 占用", _display_value(format_bytes(storage.get("binlog_disk_usage_bytes")))),
        ("ibdata1 大小", _display_value(format_bytes(storage.get("ibdata1_size_bytes")))),
    )
    return SectionBlock(title="2.2.4 数据库容量", tables=(key_value_table("数据库容量", rows),))


def _size_mb_text(value: Any) -> str:
    if value is None or value == "":
        return ""
    return f"{value} MB"


def _user_info(result: dict[str, Any]) -> SectionBlock:
    security = result.get("db", {}).get("security", {}) if isinstance(result.get("db", {}).get("security"), dict) else {}
    anonymous_users = unwrap_items(security.get("anonymous_users"))
    empty_password_users = unwrap_items(security.get("empty_password_users"))
    super_privilege_users = unwrap_items(security.get("super_privilege_users"))
    rows = (
        ("匿名用户数", format_number(len(anonymous_users))),
        ("空密码用户数", format_number(len(empty_password_users))),
        ("root 远程登录", format_number(security.get("root_remote_login"))),
        ("SSL 启用", _display_value(format_number(security.get("ssl_enabled")))),
        ("密码策略启用", _display_value(format_number(_password_policy(security).get("enabled")))),
        ("登录失败锁定", _display_value(format_number(security.get("login_failure_lockout")))),
        ("超级权限用户数", format_number(len(super_privilege_users))),
    )
    tables = [key_value_table("数据库用户", rows)]
    tables.extend(_user_detail_tables(security))
    return SectionBlock(title="2.2.5 数据库用户", tables=tuple(tables))


def _object_counts(result: dict[str, Any]) -> SectionBlock:
    storage = result.get("db", {}).get("storage", {}) if isinstance(result.get("db", {}).get("storage"), dict) else {}
    counts = storage.get("table_index_counts", {}) if isinstance(storage.get("table_index_counts"), dict) else {}
    routines = storage.get("triggers_procedures_events", {}) if isinstance(storage.get("triggers_procedures_events"), dict) else {}
    rows = (
        ("表数量", format_number(counts.get("tables"))),
        ("索引数量", format_number(counts.get("indexes"))),
        ("触发器数量", format_number(routines.get("triggers"))),
        ("存储过程数量", format_number(routines.get("procedures"))),
        ("事件数量", format_number(routines.get("events"))),
    )
    return SectionBlock(title="2.2.6 数据库对象数量", tables=(key_value_table("数据库对象数量", rows),))


def _thread_info(result: dict[str, Any]) -> SectionBlock:
    perf = result.get("db", {}).get("performance", {}) if isinstance(result.get("db", {}).get("performance"), dict) else {}
    rows = (
        ("threads_running", _display_value(format_number(perf.get("threads_running")))),
        ("连接使用率", _display_value(format_percent(perf.get("connection_usage_percent")))),
        ("max_used_connections_ratio", _display_value(format_percent(perf.get("max_used_connections_ratio")))),
        ("thread_cache_hit_ratio", _display_value(format_percent(perf.get("thread_cache_hit_ratio")))),
        ("aborted_connects", _display_value(format_number(perf.get("aborted_connects")))),
    )
    note = "当前 contracts 已提供线程运行数、连接使用率和线程缓存命中率等聚合指标，未输出完整 processlist/session 明细。"
    return SectionBlock(title="2.2.7 运行线程信息", tables=(key_value_table("运行线程信息", rows),), note=note)


def _file_info(result: dict[str, Any]) -> SectionBlock:
    storage = result.get("db", {}).get("storage", {}) if isinstance(result.get("db", {}).get("storage"), dict) else {}
    rows = (
        ("ibdata1", _display_value(format_bytes(storage.get("ibdata1_size_bytes")))),
        ("undo tablespace", _display_value(format_bytes(storage.get("undo_tablespace_size_bytes")))),
        ("temp tablespace", _display_value(format_bytes(storage.get("temp_tablespace_size_bytes")))),
        ("binlog", _display_value(format_bytes(storage.get("binlog_disk_usage_bytes")))),
        ("redo log", _display_value(_join_log_sizes(storage.get("log_file_sizes")))),
    )
    return SectionBlock(title="2.2.8 数据库文件信息", tables=(key_value_table("数据库文件信息", rows),))


def _join_log_sizes(items: Any) -> str:
    if isinstance(items, (int, float)):
        return f"{format_number(items)} GiB"
    if not isinstance(items, list) or not items:
        return ""
    return "；".join(format_bytes(item) for item in items[:4])


def _status_info(result: dict[str, Any]) -> SectionBlock:
    perf = result.get("db", {}).get("performance", {}) if isinstance(result.get("db", {}).get("performance"), dict) else {}
    rows = (
        ("QPS", _display_value(format_number(perf.get("qps")))),
        ("opened_tables_per_second", _display_value(format_number(perf.get("opened_tables_per_second")))),
        ("tmp_disk_table_ratio", _display_value(format_percent(perf.get("tmp_disk_table_ratio")))),
        ("sort_merge_passes", _display_value(format_number(perf.get("sort_merge_passes")))),
        ("deadlock_frequency", _display_value(format_number(perf.get("deadlock_frequency")))),
    )
    tables = [key_value_table("数据库状态信息", rows)]
    error_table = _recent_error_table(result)
    if error_table is not None:
        tables.append(error_table)
    return SectionBlock(title="2.2.9 数据库状态信息", tables=tuple(tables))


def _recent_error_table(result: dict[str, Any]):
    errors = unwrap_items(result.get("db", {}).get("basic_info", {}).get("recent_errors"))
    if not errors:
        return None
    rows = []
    for item in errors[:10]:
        rows.append(
            (
                format_time(str(row_value(item, "LOGGED", "logged"))),
                str(row_value(item, "PRIO", "prio")),
                str(row_value(item, "SUBSYSTEM", "subsystem")),
                str(row_value(item, "DATA", "data")),
            )
        )
    return compact_table("近期错误日志告警", RECENT_ERROR_COLUMNS, tuple(rows), RECENT_ERROR_FIELD_NOTES)


def _display_value(value: str) -> str:
    if value == "":
        return "未采集"
    return value


def _user_detail_tables(security: dict[str, Any]) -> tuple:
    return (
        _all_db_privilege_table(security),
        _auth_plugin_table(security),
        _legacy_auth_plugin_table(security),
    )


def _all_db_privilege_table(security: dict[str, Any]):
    rows = tuple(
        (
            str(row_value(item, "user", "USER")),
            str(row_value(item, "host", "HOST")),
            str(row_value(item, "db", "DB")),
        )
        for item in unwrap_items(security.get("all_db_privilege_users"))[:10]
    )
    return full_table("全库权限用户", ("用户", "主机", "库"), rows or (("无", "-", "-"),))


def _auth_plugin_table(security: dict[str, Any]):
    rows = tuple(
        (
            str(row_value(item, "plugin", "PLUGIN")),
            format_number(row_value(item, "user_count", "USER_COUNT")),
        )
        for item in unwrap_items(security.get("auth_plugin_check"))[:10]
    )
    return full_table("认证插件分布", ("认证插件", "用户数"), rows or (("无", "-"),))


def _legacy_auth_plugin_table(security: dict[str, Any]):
    rows = tuple(
        (
            str(row_value(item, "user", "USER")),
            str(row_value(item, "host", "HOST")),
            str(row_value(item, "plugin", "PLUGIN")),
        )
        for item in unwrap_items(security.get("legacy_auth_plugin_users"))[:10]
    )
    return full_table("旧认证插件用户", ("用户", "主机", "认证插件"), rows or (("无", "-", "-"),))


def _password_policy(security: dict[str, Any]) -> dict[str, Any]:
    policy = security.get("password_policy")
    if isinstance(policy, dict):
        return policy
    return {}
