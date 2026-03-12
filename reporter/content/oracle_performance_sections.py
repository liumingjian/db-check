"""Oracle performance/session report sections."""

from __future__ import annotations

from reporter.content.helpers import format_number, format_percent, full_table, key_value_table, row_value, unwrap_items
from reporter.content.oracle_section_utils import db_payload, first_row
from reporter.model.report_view import SectionBlock, TableBlock


def build_performance_and_session_section(result: dict[str, object]) -> SectionBlock:
    performance = db_payload(result, "performance")
    summary_rows = (
        ("Redo Nowait", format_percent(performance.get("redo_nowait_pct"))),
        ("活跃会话采样数", format_number(len(unwrap_items(performance.get("active_session_details"))), 0)),
        ("阻塞链数量", format_number(len(unwrap_items(performance.get("blocking_chains"))), 0)),
        ("长事务数量", format_number(len(unwrap_items(performance.get("long_transactions"))), 0)),
    )
    return SectionBlock(
        title="2.2.3 性能与会话",
        tables=(
            key_value_table("性能指标摘要", summary_rows),
            _metric_overview_table(performance),
            _instance_efficiency_table(performance),
            _active_session_details_table(performance),
            _active_sessions_table(performance),
            _long_transactions_table(performance),
            _blocking_chains_table(performance),
            _resource_limits_table(performance),
            _redo_switch_table(performance),
            _tablespace_io_table(performance),
            _wait_events_table(performance),
            _latch_data_table(performance),
            _time_model_table(performance),
            _undo_usage_table(performance),
            _undo_stats_table(performance),
            _sga_resize_ops_table(performance),
        ),
    )


def _metric_overview_table(performance: dict[str, object]) -> TableBlock:
    rows = tuple(
        (
            str(row_value(item, "metric_name")),
            format_number(row_value(item, "average_value")),
            str(row_value(item, "metric_unit")),
        )
        for item in unwrap_items(performance.get("metric_overview"))
    )
    return full_table("核心性能指标", ("指标", "平均值", "单位"), rows or (("待补充", "", ""),))


def _instance_efficiency_table(performance: dict[str, object]) -> TableBlock:
    efficiency = first_row(performance.get("instance_efficiency"))
    rows = (
        ("DB Block Gets", str(efficiency.get("db_block_gets", ""))),
        ("Consistent Gets", str(efficiency.get("consistent_gets", ""))),
        ("DB Block Reads%", str(efficiency.get("db_block_reads_pct", ""))),
        ("DB Block Writes%", str(efficiency.get("db_block_writes_pct", ""))),
    )
    return key_value_table("实例效率", rows)


def _active_sessions_table(performance: dict[str, object]) -> TableBlock:
    rows = tuple((str(row_value(item, "inst_id")), format_number(row_value(item, "active_sessions"), 0)) for item in unwrap_items(performance.get("active_sessions")))
    return full_table("活跃会话概览", ("实例", "活跃会话数"), rows or (("待补充", ""),))


def _active_session_details_table(performance: dict[str, object]) -> TableBlock:
    rows = tuple((str(row_value(item, "sid")), str(row_value(item, "serial")), str(row_value(item, "username")), str(row_value(item, "sql_id")), str(row_value(item, "event")), str(row_value(item, "seconds_in_wait"))) for item in unwrap_items(performance.get("active_session_details")))
    return full_table("活跃会话明细", ("SID", "SERIAL", "用户", "SQL_ID", "等待事件", "等待秒数"), rows or (("无", "", "", "", "", ""),))


def _long_transactions_table(performance: dict[str, object]) -> TableBlock:
    rows = tuple((str(row_value(item, "sid")), str(row_value(item, "serial")), str(row_value(item, "username")), str(row_value(item, "sql_id")), str(row_value(item, "event")), str(row_value(item, "start_time")), format_number(row_value(item, "duration_minutes"))) for item in unwrap_items(performance.get("long_transactions")))
    return full_table("长事务", ("SID", "SERIAL", "用户", "SQL_ID", "等待事件", "开始时间", "持续分钟"), rows or (("无", "", "", "", "", "", ""),))


def _blocking_chains_table(performance: dict[str, object]) -> TableBlock:
    rows = tuple((str(row_value(item, "waiter_sid")), str(row_value(item, "waiter_username")), str(row_value(item, "blocker_sid")), str(row_value(item, "blocker_username")), str(row_value(item, "wait_event")), str(row_value(item, "seconds_in_wait"))) for item in unwrap_items(performance.get("blocking_chains")))
    return full_table("阻塞链", ("等待SID", "等待用户", "阻塞SID", "阻塞用户", "等待事件", "等待秒数"), rows or (("无", "", "", "", "", ""),))


def _resource_limits_table(performance: dict[str, object]) -> TableBlock:
    rows = tuple((str(row_value(item, "inst_id")), str(row_value(item, "resource_name")), str(row_value(item, "current_utilization")).strip(), str(row_value(item, "max_utilization")).strip(), str(row_value(item, "limit_value")).strip()) for item in unwrap_items(performance.get("resource_limits")))
    return full_table("资源限制", ("实例", "资源", "当前值", "峰值", "上限"), rows or (("待补充", "", "", "", ""),))


def _redo_switch_table(performance: dict[str, object]) -> TableBlock:
    rows = tuple((str(row_value(item, "switch_date")), format_number(row_value(item, "switch_count"), 0)) for item in unwrap_items(performance.get("redo_switch_daily")))
    return full_table("Redo切换频率", ("日期", "切换次数"), rows or (("待补充", ""),))


def _tablespace_io_table(performance: dict[str, object]) -> TableBlock:
    rows = tuple((str(row_value(item, "tablespace_name")), str(row_value(item, "file_name")), str(row_value(item, "phyrds")), str(row_value(item, "phyblkrd")), str(row_value(item, "phywrts")), str(row_value(item, "phyblkwrt"))) for item in unwrap_items(performance.get("tablespace_io_stats")))
    return full_table("表空间IO统计", ("表空间", "文件", "物理读", "读块", "物理写", "写块"), rows or (("待补充", "", "", "", "", ""),))


def _wait_events_table(performance: dict[str, object]) -> TableBlock:
    rows = tuple((str(row_value(item, "event")), format_number(row_value(item, "waits"), 0), format_number(row_value(item, "waited_ms")), format_number(row_value(item, "avg_wait_ms"))) for item in unwrap_items(performance.get("wait_events")))
    return full_table("Top等待事件", ("事件", "等待次数", "累计等待(ms)", "平均等待(ms)"), rows or (("待补充", "", "", ""),))


def _latch_data_table(performance: dict[str, object]) -> TableBlock:
    rows = tuple((str(row_value(item, "name")), format_number(row_value(item, "gets"), 0), format_number(row_value(item, "misses"), 0), format_number(row_value(item, "sleeps"), 0)) for item in unwrap_items(performance.get("latch_data")))
    return full_table("Latch统计", ("Latch", "Gets", "Misses", "Sleeps"), rows or (("待补充", "", "", ""),))


def _time_model_table(performance: dict[str, object]) -> TableBlock:
    rows = tuple((str(row_value(item, "stat_name")), format_number(row_value(item, "seconds"))) for item in unwrap_items(performance.get("time_model")))
    return full_table("Time Model", ("统计项", "秒"), rows or (("待补充", ""),))


def _undo_usage_table(performance: dict[str, object]) -> TableBlock:
    rows = tuple(
        (
            str(row_value(item, "tablespace_name")),
            format_number(row_value(item, "total_size_gb")),
            format_number(row_value(item, "used_size_gb")),
            format_number(row_value(item, "active_size_gb")),
            format_number(row_value(item, "unexpired_size_gb")),
            format_number(row_value(item, "expired_size_gb")),
            format_percent(row_value(item, "usage_percent")),
        )
        for item in unwrap_items(performance.get("undo_tablespace_usage"))
    )
    return full_table(
        "UNDO表空间使用情况",
        ("表空间", "总容量(GB)", "已用(GB)", "ACTIVE(GB)", "UNEXPIRED(GB)", "EXPIRED(GB)", "使用率"),
        rows or (("待补充", "", "", "", "", "", ""),),
    )


def _undo_stats_table(performance: dict[str, object]) -> TableBlock:
    rows = tuple(
        (
            str(row_value(item, "begin_time")),
            str(row_value(item, "end_time")),
            format_number(row_value(item, "txncount"), 0),
            format_number(row_value(item, "maxquerylen"), 0),
            format_number(row_value(item, "ssolderrcnt"), 0),
            format_number(row_value(item, "nospaceerrcnt"), 0),
        )
        for item in unwrap_items(performance.get("undo_stats"))
    )
    return full_table(
        "UNDO统计",
        ("开始时间", "结束时间", "事务数", "最长查询(秒)", "ORA-1555", "空间不足"),
        rows or (("待补充", "", "", "", "", ""),),
    )


def _sga_resize_ops_table(performance: dict[str, object]) -> TableBlock:
    rows = tuple(
        (
            str(row_value(item, "component")),
            str(row_value(item, "oper_type")),
            str(row_value(item, "oper_mode")),
            format_number(row_value(item, "initial_size_mb")),
            format_number(row_value(item, "target_size_mb")),
            format_number(row_value(item, "final_size_mb")),
            str(row_value(item, "start_time")),
            str(row_value(item, "status")),
        )
        for item in unwrap_items(performance.get("sga_resize_ops"))
    )
    return full_table(
        "SGA Resize历史",
        ("组件", "操作类型", "模式", "初始(MB)", "目标(MB)", "最终(MB)", "开始时间", "状态"),
        rows or (("待补充", "", "", "", "", "", "", ""),),
    )
