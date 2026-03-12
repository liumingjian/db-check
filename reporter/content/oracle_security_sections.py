"""Oracle security/object-health report sections."""

from __future__ import annotations

from reporter.content.helpers import full_table, key_value_table, row_value, unwrap_items
from reporter.content.oracle_section_utils import count_text, db_payload
from reporter.model.report_view import SectionBlock, TableBlock


def build_security_section(result: dict[str, object]) -> SectionBlock:
    security = db_payload(result, "security")
    summary_rows = (
        ("禁用约束数", count_text(security.get("disabled_constraints"))),
        ("禁用触发器数", count_text(security.get("disabled_triggers"))),
        ("高权限账号数", count_text(security.get("dba_role_users"))),
        ("过期账号数", count_text(security.get("expired_users"))),
        ("并行度异常表数", count_text(security.get("table_degree_gt_one"))),
        ("并行度异常索引数", count_text(security.get("indexes_degree_gt_one"))),
    )
    return SectionBlock(
        title="2.2.5 安全与对象健康",
        tables=(
            key_value_table("安全与对象健康", summary_rows),
            _table(security, "disabled_constraints", "禁用约束明细", ("Owner", "约束名", "类型", "表名", "状态"), ("owner", "constraint_name", "constraint_type", "table_name", "status")),
            _table(security, "disabled_triggers", "禁用触发器明细", ("Owner", "触发器", "类型", "表名", "状态"), ("owner", "trigger_name", "trigger_type", "table_name", "status")),
            _table(security, "dba_role_users", "高权限账号明细", ("账号", "角色", "可管理", "默认角色"), ("grantee", "granted_role", "admin_option", "default_role")),
            _table(security, "expired_users", "过期账号", ("账号", "状态", "过期时间"), ("username", "account_status", "expiry_date")),
            _table(security, "table_degree_gt_one", "并行度异常表", ("表名", "并行度"), ("table_name", "degree")),
            _table(security, "indexes_degree_gt_one", "并行度异常索引", ("索引名", "并行度"), ("index_name", "degree")),
        ),
    )


def _table(payload: dict[str, object], key: str, title: str, columns: tuple[str, ...], fields: tuple[str, ...]) -> TableBlock:
    rows = tuple(tuple("" if row_value(item, field) is None else str(row_value(item, field)) for field in fields) for item in unwrap_items(payload.get(key)))
    return full_table(title, columns, rows or (("无",) + ("",) * (len(columns) - 1),))
