"""Opening Oracle deployment topology summary."""

from __future__ import annotations

from typing import Any

from reporter.content.helpers import full_table, key_value_table, unwrap_items
from reporter.content.oracle_section_utils import bytes_to_mb, db_payload
from reporter.model.report_view import SectionBlock


def build_oracle_topology_section(result: dict[str, Any]) -> SectionBlock:
    version = db_payload(result, "version_info")
    topology = db_payload(result, "deployment_topology")
    raw_version = version.get("version") or db_payload(result, "basic_info").get("version", "未知")
    family = version.get("family", "未知")
    rows = (
        ("数据库版本", f"{family} / {raw_version}"),
        ("实例部署", _facet(topology.get("is_rac"), "RAC", "单机")),
        ("容器数据库", _facet(topology.get("is_cdb"), "是", "否")),
        ("数据库角色", str(topology.get("database_role") or "未知")),
        ("ASM 存储", _facet(topology.get("is_asm"), "是", "否")),
        ("当前容器", str(topology.get("connected_container") or "不适用或未采集")),
    )
    tables = [key_value_table("部署形态概览", rows)]
    if topology.get("is_cdb") is True:
        state = str(topology.get("pdb_list_state", "not_collected"))
        pdb_rows = tuple(
            (str(item.get("name", "")), str(item.get("open_mode", "")), bytes_to_mb(item.get("size_bytes")))
            for item in unwrap_items(topology.get("pdbs"))
        )
        note = "" if state == "collected" else "PDB 清单不完整。请 DBA 使用巡检账号连接 CDB$ROOT 并授予 SYS.V_$PDBS 查询权限后重新采集。当前表只列出可见的 PDB。"
        tables.append(full_table("可插拔数据库清单", ("PDB 名称", "状态", "大小(MB)"), pdb_rows, status=state, note=note))
    return SectionBlock(
        title="部署形态",
        paragraphs=("仅巡检当前连接容器，未切换到其他 PDB 执行检查。",),
        tables=tuple(tables),
    )


def _facet(value: Any, yes: str, no: str) -> str:
    if value is True:
        return yes
    if value is False:
        return no
    return "未知"
