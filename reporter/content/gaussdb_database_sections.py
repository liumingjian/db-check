"""GaussDB database detail sections."""

from __future__ import annotations

from typing import Any

from reporter.content.gaussdb_config_sections import build_config_section
from reporter.content.gaussdb_section_utils import basic_info_rows, details_dict, domain_table, find_item, rows_payload, visible_items
from reporter.content.helpers import full_table, key_value_table
from reporter.model.report_view import SectionBlock, TableBlock


def build_gaussdb_database_sections(result: dict[str, object]) -> tuple[SectionBlock, ...]:
    sections = (
        _availability_section(result),
        build_config_section(result),
        _object_health_section(result),
        _governance_section(result),
        _log_section(result),
    )
    return tuple(section for section in sections if section is not None)


def _availability_section(result: dict[str, object]) -> SectionBlock | None:
    tables: list[TableBlock] = []
    rows = basic_info_rows(result)
    if rows:
        tables.append(key_value_table("基础与环境", rows))
    basic_items = visible_items(result, "basic_info")
    cluster_items = visible_items(result, "cluster")
    overview_items = tuple(item for item in basic_items + cluster_items if str(item.get("item") or "") in {"CheckDBConnection", "CheckClusterState", "CheckIntegrity", "CheckOMMonitor", "CheckGaussVer"})
    if overview_items:
        tables.append(domain_table("基础可用性检查", overview_items))
    cluster_item = find_item(result, "CheckClusterState", "cluster")
    cluster_details = details_dict(cluster_item)
    if cluster_details:
        tables.append(
            key_value_table(
                "集群状态摘要",
                (
                    ("集群状态", str(cluster_details.get("cluster_state") or "")),
                    ("是否重分布", str(cluster_details.get("redistributing") or "")),
                    ("是否均衡", str(cluster_details.get("balanced") or "")),
                ),
            )
        )
        nodes = cluster_details.get("nodes")
        if isinstance(nodes, list) and nodes:
            rows = tuple((str(item.get("node") or ""), str(item.get("status") or "")) for item in nodes if isinstance(item, dict))
            tables.append(full_table("节点状态", ("节点", "状态"), rows))
    integrity_item = find_item(result, "CheckIntegrity", "cluster")
    integrity_details = details_dict(integrity_item)
    if integrity_item is not None:
        rows = [("检查结论", str(integrity_item.get("summary") or ""))]
        checksum = integrity_details.get("sha256")
        if checksum:
            rows.append(("SHA256", str(checksum)))
        tables.append(key_value_table("数据一致性", tuple(rows)))
    om_item = find_item(result, "CheckOMMonitor", "basic_info")
    if om_item is not None:
        om_rows = [("检查结论", str(om_item.get("summary") or ""))]
        pid = details_dict(om_item).get("pid")
        if pid:
            om_rows.append(("进程PID", str(pid)))
        tables.append(key_value_table("omMonitor进程", tuple(om_rows)))
    if not tables:
        return None
    return SectionBlock(title="2.2.1 基础可用性", tables=tuple(tables))


def _object_health_section(result: dict[str, object]) -> SectionBlock | None:
    tables: list[TableBlock] = []
    return_type_item = find_item(result, "CheckReturnType", "sql_analysis")
    if return_type_item is not None:
        tables.append(key_value_table("自定义函数检查", (("检查结论", str(return_type_item.get("summary") or "")),)))
    sys_table_item = find_item(result, "CheckSysTable", "storage")
    if sys_table_item is not None:
        sys_details = details_dict(sys_table_item)
        tables.append(key_value_table("系统表检查摘要", (("检查结论", str(sys_table_item.get("summary") or "")), ("检查表数量", str(sys_details.get("table_count") or 0)))))
        table_rows = sys_details.get("tables")
        if isinstance(table_rows, list) and table_rows:
            rows = tuple(
                (
                    str(item.get("instance") or ""),
                    str(item.get("table_name") or ""),
                    str(item.get("size_bytes") or ""),
                    str(item.get("row_count") or ""),
                    str(item.get("avg_width") or ""),
                )
                for item in table_rows
                if isinstance(item, dict)
            )
            tables.append(full_table("系统表明细", ("实例", "系统表", "大小(Byte)", "行数", "平均行宽"), rows))
    if not tables:
        return None
    return SectionBlock(title="2.2.3 对象与结构健康", tables=tuple(tables))


def _governance_section(result: dict[str, object]) -> SectionBlock | None:
    tables: list[TableBlock] = []
    large_table_item = find_item(result, "CheckKeyDBTableSize", "storage")
    if large_table_item is not None:
        large_details = details_dict(large_table_item)
        tables.append(key_value_table("大表检查摘要", (("检查结论", str(large_table_item.get("summary") or "")), ("数据库数量", str(large_details.get("database_count") or 0)), ("表数量", str(large_details.get("table_count") or 0)))))
        database_rows = large_details.get("databases")
        if isinstance(database_rows, list) and database_rows:
            rows = tuple((str(item.get("database") or ""), _size_text(item)) for item in database_rows if isinstance(item, dict))
            tables.append(full_table("大库概览", ("数据库", "体量"), rows))
        table_rows = large_details.get("tables")
        if isinstance(table_rows, list) and table_rows:
            rows = tuple((str(item.get("table_name") or ""), _size_text(item)) for item in table_rows if isinstance(item, dict))
            tables.append(full_table("大表明细", ("表名", "体量"), rows))
    tables.extend(_governance_sql_tables(result))
    if not tables:
        return None
    return SectionBlock(title="2.2.4 容量与数据治理", tables=tuple(tables))


def _governance_sql_tables(result: dict[str, object]) -> list[TableBlock]:
    tables: list[TableBlock] = []
    tables.append(_summary_or_empty("无索引表汇总", rows_payload(result, "sql_analysis", "no_index_summary"), ("owner", "total_table_count", "no_index_count", "percentage"), ("所属用户", "总表数", "无索引表数", "占比(%)"), "未发现无索引表"))
    tables.append(_summary_or_empty("无主键表汇总", rows_payload(result, "sql_analysis", "no_primary_key_summary"), ("owner", "total_table_count", "no_pk_count", "percentage"), ("所属用户", "总表数", "无主键表数", "占比(%)"), "未发现无主键表"))
    detail_table = _detail_table("无主键表明细", rows_payload(result, "sql_analysis", "no_primary_key_detail"), ("owner", "table_name"), ("所属用户", "表名"))
    if detail_table is not None:
        tables.append(detail_table)
    tables.append(_summary_or_empty("统计信息缺失汇总", rows_payload(result, "sql_analysis", "no_statistics_summary"), ("tableowner", "total_table_count", "table_no_stat", "percentage"), ("所属用户", "总表数", "缺失统计信息表数", "占比(%)"), "未发现统计信息缺失表"))
    stats_detail = _detail_table("统计信息缺失明细", rows_payload(result, "sql_analysis", "no_statistics_detail"), ("schemaname", "tableowner", "tablename"), ("Schema", "所属用户", "表名"))
    if stats_detail is not None:
        tables.append(stats_detail)
    return [table for table in tables if table is not None]


def _log_section(result: dict[str, object]) -> SectionBlock | None:
    item = find_item(result, "CheckErrorInLog", "performance")
    if item is None:
        return None
    details = details_dict(item)
    tables: list[TableBlock] = [key_value_table("运行日志摘要", (("检查结论", str(item.get("summary") or "")), ("ERROR 数量", str(details.get("error_count") or 0))))]
    samples = details.get("sample_lines")
    if isinstance(samples, list) and samples:
        rows = tuple((str(index + 1), str(line)) for index, line in enumerate(samples))
        tables.append(full_table("运行日志样本", ("序号", "日志内容"), rows))
    return SectionBlock(title="2.2.5 运行日志", tables=tuple(tables))


def _summary_or_empty(title: str, payload: dict[str, Any], fields: tuple[str, ...], columns: tuple[str, ...], empty_text: str) -> TableBlock:
    items = payload.get("items", [])
    if not isinstance(items, list) or not items:
        return key_value_table(title, (("检查结论", empty_text),))
    rows = tuple(tuple(str(item.get(field) or "") for field in fields) for item in items if isinstance(item, dict))
    return full_table(title, columns, rows)


def _detail_table(title: str, payload: dict[str, Any], fields: tuple[str, ...], columns: tuple[str, ...]) -> TableBlock | None:
    items = payload.get("items", [])
    if not isinstance(items, list) or not items:
        return None
    rows = tuple(tuple(str(item.get(field) or "") for field in fields) for item in items if isinstance(item, dict))
    return full_table(title, columns, rows)


def _size_text(item: dict[str, Any]) -> str:
    value = item.get("size_value")
    unit = str(item.get("size_unit") or "")
    if value in ("", None):
        return ""
    return f"{value}{unit}"
