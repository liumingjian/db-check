"""Oracle storage/log report sections."""

from __future__ import annotations

from reporter.content.helpers import format_number, format_percent, full_table, key_value_table, row_value, unwrap_items
from reporter.content.oracle_section_utils import bytes_to_mb, db_payload
from reporter.model.report_view import SectionBlock, TableBlock


def build_storage_and_log_section(result: dict[str, object]) -> SectionBlock:
    storage = db_payload(result, "storage")
    evidence = result.get("db", {}).get("collection_availability", {})
    summary_rows = (
        ("数据文件总量(GB)", format_number(storage.get("datafile_total_gb"), 0)),
        ("表空间数量", format_number(storage.get("tablespace_count"), 0)),
        ("数据文件数量", format_number(storage.get("datafile_count"), 0)),
        ("控制文件数量", format_number(storage.get("controlfile_count"), 0)),
        ("Redo 大小(MB)", format_number(storage.get("redo_size_mb"), 0)),
        ("Redo 组数", format_number(storage.get("redo_group_count"), 0)),
        ("回收站可回收空间(GB)", format_number(storage.get("recyclebin_size_gb"))),
    )
    return SectionBlock(
        title="2.2.2 存储与日志",
        paragraphs=("表空间使用率以文件自动扩展最大容量为分母；临时表空间使用量按实际块大小计算活跃临时段，不计可复用缓存。",),
        tables=(
            key_value_table("存储摘要", summary_rows),
            _tablespace_usage_table(storage),
            _space_detail_table(storage, evidence, "temp_usage", "临时表空间活跃使用量", ("tablespace_name", "block_size", "total_size_gb", "max_size_gb", "used_size_gb", "real_percent"), ("表空间", "块大小(字节)", "当前容量(GB)", "最大容量(GB)", "活跃使用(GB)", "使用率")),
            _space_detail_table(storage, evidence, "sysaux_usage", "SYSAUX 使用情况", ("tablespace_name", "max_size_gb", "used_size_gb", "real_percent"), ("表空间", "最大容量(GB)", "已用(GB)", "使用率")),
            _space_detail_table(storage, evidence, "largest_segments", "最大段明细(前 20 项)", ("owner", "segment_name", "partition_name", "segment_type", "tablespace_name", "size_gb", "tablespace_percent"), ("Owner", "段名", "分区", "类型", "表空间", "大小(GB)", "容量占比")),
            _alert_log_table(result),
            _datafiles_table(storage),
            _control_files_table(storage),
            _redo_logs_table(storage),
            _recover_files_table(storage),
            _table_fragments_table(storage),
            _invalid_objects_table(storage),
            _invalid_indexes_table(storage),
        ),
    )


def _space_detail_table(storage, evidence, name, title, fields, columns):
    rows = tuple(tuple(
        format_percent(item.get(field)) if field in {"real_percent", "tablespace_percent"}
        else format_number(item.get(field)) if field.endswith("_gb")
        else str(item.get(field) or "") for field in fields
    ) for item in unwrap_items(storage.get(name)))
    gap = evidence.get("db.storage." + name, {})
    missing = gap.get("readable") is False or name not in storage
    return full_table(title, columns, rows or (("未采集" if missing else "无",) + ("",) * (len(columns)-1),),
                      status="not_collected" if missing else "collected",
                      note=str(gap.get("remediation", "")))


def _alert_log_table(result):
    alert = db_payload(result, "alert_log")
    gap = result.get("db", {}).get("collection_availability", {}).get("db.alert_log", {})
    missing = gap.get("readable") is False or not alert
    rows = tuple((str(item.get("error_code", "")),
                  format_number(item.get("count"), 0), str(item.get("latest_time", "")), str(item.get("latest_message", "")))
                 for item in unwrap_items(alert.get("errors")))
    days, limit = alert.get("window_days", "未知"), alert.get("record_limit", "未知")
    note = f"最近 {days} 天最新 {limit} 条包含 ORA 错误的记录，按错误码去重，每条记录同一错误码计一次。"
    if alert.get("limit_reached"):
        note += f"已达到 {limit} 条上限，计数不代表窗口内全部错误。"
    if missing:
        note += str(gap.get("reason", "")) + " " + str(gap.get("remediation", "请 DBA 手工检查 ADRCI 或 alert 日志。"))
    return full_table("告警日志 ORA 错误汇总", ("错误码", "匹配记录数", "最近时间", "最近消息"),
                      rows or (("未采集" if missing else "无 ORA 错误", "", "", ""),),
                      status="not_collected" if missing else "collected", note=note)


def _tablespace_usage_table(storage: dict[str, object]) -> TableBlock:
    rows = tuple(
        (
            str(row_value(item, "tablespace_name")),
            format_number(row_value(item, "max_size_gb")),
            format_number(row_value(item, "total_size_gb")),
            format_number(row_value(item, "used_size_gb")),
            format_percent(row_value(item, "real_percent")),
        )
        for item in unwrap_items(storage.get("tablespace_usage"))
    )
    return full_table("表空间使用情况", ("表空间", "最大容量(GB)", "当前容量(GB)", "已用(GB)", "使用率"), rows or (("待补充", "", "", "", ""),))


def _datafiles_table(storage: dict[str, object]) -> TableBlock:
    rows = tuple(
        (str(row_value(item, "file_name")), str(row_value(item, "tablespace_name")), str(row_value(item, "status")), str(row_value(item, "current_gb")), str(row_value(item, "autoextensible")), str(row_value(item, "max_gb")))
        for item in unwrap_items(storage.get("datafiles"))
    )
    return full_table("数据文件明细", ("文件", "表空间", "状态", "当前大小(GB)", "自动扩展", "最大大小(GB)"), rows or (("待补充", "", "", "", "", ""),))


def _control_files_table(storage: dict[str, object]) -> TableBlock:
    rows = tuple((str(row_value(item, "name")),) for item in unwrap_items(storage.get("control_files")))
    return full_table("控制文件明细", ("控制文件路径",), rows or (("待补充",),))


def _redo_logs_table(storage: dict[str, object]) -> TableBlock:
    rows = tuple(
        (str(row_value(item, "group_id")), str(row_value(item, "thread_id")), str(row_value(item, "sequence")), bytes_to_mb(row_value(item, "bytes")), str(row_value(item, "members")), str(row_value(item, "archived")), str(row_value(item, "status")), str(row_value(item, "first_time")))
        for item in unwrap_items(storage.get("redo_logs"))
    )
    return full_table("Redo日志明细", ("组", "线程", "序列", "大小(MB)", "成员数", "是否归档", "状态", "首次时间"), rows or (("待补充", "", "", "", "", "", "", ""),))


def _recover_files_table(storage: dict[str, object]) -> TableBlock:
    rows = tuple(
        (str(row_value(item, "file_id")), str(row_value(item, "online_status")), str(row_value(item, "error")), str(row_value(item, "change_number")), str(row_value(item, "time")))
        for item in unwrap_items(storage.get("recover_files"))
    )
    return full_table("待恢复数据文件", ("文件ID", "在线状态", "错误", "变更号", "时间"), rows or (("无", "", "", "", ""),))


def _table_fragments_table(storage: dict[str, object]) -> TableBlock:
    rows = tuple(
        (str(row_value(item, "owner")), str(row_value(item, "table_name")), format_number(row_value(item, "table_size_mb")), format_percent(row_value(item, "used_pct")), format_number(row_value(item, "safe_space_mb")))
        for item in unwrap_items(storage.get("table_fragments"))
    )
    return full_table("表碎片分析", ("Owner", "表名", "大小(MB)", "使用率", "可回收空间(MB)"), rows or (("无", "", "", "", ""),))


def _invalid_objects_table(storage: dict[str, object]) -> TableBlock:
    rows = tuple((str(row_value(item, "owner")), str(row_value(item, "object_type")), format_number(row_value(item, "object_count"), 0)) for item in unwrap_items(storage.get("invalid_objects")))
    return full_table("无效对象", ("Owner", "对象类型", "数量"), rows or (("无", "", ""),))


def _invalid_indexes_table(storage: dict[str, object]) -> TableBlock:
    rows = tuple((str(row_value(item, "owner")), str(row_value(item, "index_name")), str(row_value(item, "subname")), str(row_value(item, "status"))) for item in unwrap_items(storage.get("invalid_indexes")))
    return full_table("不可用索引", ("Owner", "索引", "子对象", "状态"), rows or (("无", "", "", ""),))
