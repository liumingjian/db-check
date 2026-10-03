"""Oracle topology-specific evidence."""

from reporter.content.helpers import full_table, unwrap_items
from reporter.content.oracle_section_utils import db_payload
from reporter.model.report_view import SectionBlock


def build_specialized_section(result: dict[str, object]) -> SectionBlock:
    tables = []
    datasets = (
        ("Data Guard 延迟", "data_guard", "lag", ("name", "value", "unit", "time_computed", "datum_time")),
        ("当前归档阻塞缺口", "data_guard", "archive_gap", ("thread", "low_sequence", "high_sequence")),
        ("standby 归档目的地错误", "data_guard", "destination_errors", ("dest_name", "status", "destination", "error")),
        ("ASM 磁盘组", "asm", "diskgroups", ("name", "state", "redundancy", "total_mb", "free_mb", "used_pct", "usable_file_mb", "required_mirror_free_mb", "offline_disks")),
        ("RAC 可见实例", "rac", "instances", ("inst_id", "instance_name", "host_name", "status", "database_status", "active_state")),
        ("RAC 当前生效参数", "rac", "parameters", ("inst_id", "name", "value", "isdefault")),
    )
    for title, module, key, fields in datasets:
        rows = tuple(tuple(str(item.get(field, "")) for field in fields)
                     for item in unwrap_items(db_payload(result, module).get(key)))
        tables.append(full_table(title, fields, rows or (("无记录或未采集",) + ("",) * (len(fields) - 1),)))
    host = db_payload(result, "host_checks")
    rows = tuple((name, str(host.get(name, {}).get("output", "未采集，请按覆盖缺口建议手工检查")))
                 for name in ("listener", "clusterware"))
    tables.append(full_table("主机服务命令结果", ("服务", "命令输出"), rows))
    mode = db_payload(result, "data_guard").get("protection_mode", "未采集或不适用")
    return SectionBlock(title="2.2.7 Data Guard、ASM 与 RAC", tables=tuple(tables), paragraphs=(
        f"standby 保护模式：{mode}。仅在检测为 standby 时评估 Data Guard。",
        "GV$INSTANCE 仅列出运行中的实例，停机节点须结合 clusterware 资源清单核查。RAC 参数按 inst_id 列出。",
        "ASM 同时展示原始空闲量及考虑冗余的 USABLE_FILE_MB；CONNECTED 与 MOUNTED 均为健康连接状态。",
        "主机命令只检查所连接主机的默认 listener 和 clusterware，其他节点及 listener 需手工核查。单次 Data Guard 样本不证明 datum_time 持续更新。",
    ))
