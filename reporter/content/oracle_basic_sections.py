"""Oracle basic/config report sections."""

from __future__ import annotations

from reporter.content.helpers import full_table, key_value_table, row_value, unwrap_items
from reporter.content.oracle_section_utils import db_payload
from reporter.model.report_view import SectionBlock


def build_basic_and_config_section(result: dict[str, object]) -> SectionBlock:
    basic = db_payload(result, "basic_info")
    config = db_payload(result, "config_check")
    summary_rows = (
        ("数据库名", str(basic.get("db_name", ""))),
        ("实例名", str(basic.get("instance_name", ""))),
        ("版本", str(basic.get("version", ""))),
        ("DBID", str(basic.get("dbid", ""))),
        ("是否 RAC", "是" if basic.get("is_rac") else "否"),
        ("字符集", str(basic.get("character_set", ""))),
        ("日志模式", str(basic.get("log_mode", ""))),
        ("SPFILE", str(config.get("spfile", ""))),
        ("SGA Target(MB)", str(config.get("sga_target_mb", ""))),
        ("DB Block Size(KB)", str(config.get("db_block_size_kb", ""))),
        ("Alert Log", str(basic.get("alert_log", ""))),
    )
    parameters = tuple(
        (str(row_value(item, "name")), str(row_value(item, "value")))
        for item in unwrap_items(config.get("parameters"))
    )
    return SectionBlock(
        title="2.2.1 基础与配置",
        tables=(
            key_value_table("Oracle 实例概览", summary_rows),
            full_table("初始化参数明细", ("参数名称", "当前值"), parameters or (("待补充", ""),)),
        ),
    )
