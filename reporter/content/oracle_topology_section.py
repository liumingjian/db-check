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
        ("版本验证级别", _verification_level(family)),
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
        note = "" if state == "collected" else str(topology.get("pdb_list_remediation") or "PDB 清单不完整。请 DBA 在 CDB$ROOT 核对 SYS.V_$PDBS 授权与 CONTAINER_DATA，提供完整清单或使用 SYSDBA 重新采集来确认容器可见性。当前表只列出可见的 PDB。")
        tables.append(full_table("可插拔数据库清单", ("PDB 名称", "状态", "大小(MB)"), pdb_rows, status=state, note=note))
    return SectionBlock(
        title="部署形态",
        paragraphs=(
            "仅巡检当前连接容器，未切换到其他 PDB 执行检查。PDB 清单仅表示账号可见范围。",
            "版本验证级别描述本实现的验证范围，不代表本次采集的所有 SQL 已成功。"
            "权限不足与未采集项见报告末尾。Collector 不采集 AWR、ASH、ADDM；AWR 仅作为工程师上传的输入。",
        ),
        tables=tuple(tables),
    )


def _verification_level(family: str) -> str:
    if family in ("11gR2", "19c"):
        return "2026-10-03 在 Mac 完成单机、primary、非 ASM 容器端到端 smoke 验证通过；RAC、ASM 与 standby 未经容器验证"
    if family in ("12c", "21c", "23ai"):
        return "已提供版本分支，未经过容器验证；18c 归入 12c 系列"
    return "版本未识别或不在支持范围内，兼容性未验证"


def _facet(value: Any, yes: str, no: str) -> str:
    if value is True:
        return yes
    if value is False:
        return no
    return "未知"
