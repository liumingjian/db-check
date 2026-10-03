"""Oracle security/object-health report sections."""

from __future__ import annotations

from reporter.content.helpers import full_table, key_value_table, row_value, unwrap_items
from reporter.content.oracle_section_utils import count_text, db_payload
from reporter.model.report_view import SectionBlock, TableBlock


def build_security_section(result: dict[str, object]) -> SectionBlock:
    availability = db_payload(result, "collection_availability")
    security = {key: value for key, value in db_payload(result, "security").items()
                if not (isinstance(availability.get(f"db.security.{key}"), dict)
                        and availability[f"db.security.{key}"].get("readable") is False)}
    summary_rows = (
        ("禁用约束数", count_text(security.get("disabled_constraints"))),
        ("禁用触发器数", count_text(security.get("disabled_triggers"))),
        ("高权限账号数", count_text(security.get("dba_role_users"))),
        ("过期账号数", count_text(security.get("expired_users"))),
        ("并行度异常表数", count_text(security.get("table_degree_gt_one"))),
        ("并行度异常索引数", count_text(security.get("indexes_degree_gt_one"))),
        ("默认口令账号数", count_text(security["default_password_users"]) if "default_password_users" in security else "待补充"),
        ("传统审计设置", str(security.get("audit_trail", "待补充"))),
        ("资源限制开关", str(security.get("resource_limit", "待补充"))),
        ("TDE 加密表空间数", str(security.get("encrypted_tablespaces", "待补充"))),
    )
    return SectionBlock(
        title="2.2.5 安全与对象健康",
        paragraphs=(
            "数据库链接仅显示 Owner、链接名、固定用户名和创建时间，不采集口令。固定用户名是可能存储凭据的推断，DBA_DB_LINKS 不能证明口令存在，也不能排除其他类型链接的凭据风险。请客户 DBA 核对链接类型、授权及凭据轮换。",
            "补丁历史记录不是当前安装清单。当前 SQL 补丁按最后一次成功操作判断，成功回滚已移除；失败应用不证明已安装。SQL 清单不能代表 Oracle Home 二进制补丁，也不判断是否达到最新基线。11g 需客户 DBA 提供 opatch lsinventory 和执行日志。",
            "TDE 表空间数量不包含列加密。钱包状态不能单独证明数据已加密。AUDIT_TRAIL 不能证明审计策略覆盖，纯统一审计需 DBA 核查启用策略。",
        ),
        tables=(
            key_value_table("安全与对象健康", summary_rows),
            _table(security, "disabled_constraints", "禁用约束明细", ("Owner", "约束名", "类型", "表名", "状态"), ("owner", "constraint_name", "constraint_type", "table_name", "status")),
            _table(security, "disabled_triggers", "禁用触发器明细", ("Owner", "触发器", "类型", "表名", "状态"), ("owner", "trigger_name", "trigger_type", "table_name", "status")),
            _table(security, "dba_role_users", "高权限账号明细", ("账号", "角色", "可管理", "默认角色"), ("grantee", "granted_role", "admin_option", "default_role")),
            _table(security, "expired_users", "过期账号", ("账号", "状态", "过期时间"), ("username", "account_status", "expiry_date")),
            _table(security, "table_degree_gt_one", "并行度异常表", ("表名", "并行度"), ("table_name", "degree")),
            _table(security, "indexes_degree_gt_one", "并行度异常索引", ("索引名", "并行度"), ("index_name", "degree")),
            _table(security, "default_password_users", "默认口令账号", ("账号",), ("username",)),
            _table(security, "weak_password_profiles", "弱口令策略", ("配置文件", "资源", "配置值", "生效值"), ("profile", "resource_name", "configured_limit", "effective_limit")),
            _table(security, "unlimited_resource_profiles", "无限资源上限", ("配置文件", "资源", "生效值"), ("profile", "resource_name", "effective_limit")),
            _table(security, "public_system_privileges", "PUBLIC 危险系统权限", ("权限", "可管理"), ("privilege", "admin_option")),
            _table(security, "database_links", "固定用户数据库链接", ("Owner", "链接名", "用户名", "创建时间"), ("owner", "db_link", "username", "created")),
            _table(security, "encryption_wallets", "TDE 钱包状态", ("类型", "状态"), ("wallet_type", "status")),
            _table(security, "installed_sql_patches", "当前 SQL 补丁", ("补丁 ID", "UID", "成功应用时间", "说明"), ("patch_id", "patch_uid", "action_time", "description")),
            _table(security, "patch_history", "补丁操作历史", ("补丁 ID", "操作", "状态", "时间", "说明"), ("patch_id", "action", "status", "action_time", "description")),
            _table(security, "failed_patch_attempts", "SQL 补丁最新操作失败", ("补丁 ID", "操作", "状态", "时间", "说明"), ("patch_id", "action", "status", "action_time", "description")),
            _table(security, "invalid_components", "非 VALID 组件", ("组件", "名称", "版本", "状态"), ("comp_id", "comp_name", "version", "status")),
        ),
    )


def _table(payload: dict[str, object], key: str, title: str, columns: tuple[str, ...], fields: tuple[str, ...]) -> TableBlock:
    rows = tuple(tuple("" if row_value(item, field) is None else str(row_value(item, field)) for field in fields) for item in unwrap_items(payload.get(key)))
    empty = "无" if key in payload else "待补充"
    return full_table(title, columns, rows or ((empty,) + ("",) * (len(columns) - 1),))
