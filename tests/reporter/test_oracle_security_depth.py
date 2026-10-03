import unittest

from reporter.content.oracle_report_builder import build_oracle_report_view


class OracleSecurityDepthReportTests(unittest.TestCase):
    def sections(self, result):
        pending = list(build_oracle_report_view(result, {}, {}).sections)
        sections = []
        while pending:
            section = pending.pop()
            sections.append(section)
            pending.extend(section.children)
        return sections

    def test_report_contains_safe_link_metadata_patch_limits_and_recovery_details(self):
        result = {"db": {"security": {
            "database_links": {"items": [{"owner": "APP", "db_link": "REMOTE", "username": "REMOTE_USER", "created": "2026-01-01"}]},
            "installed_sql_patches": {"items": [{"patch_id": 123, "patch_uid": 456, "description": "RU"}]},
        }, "backup": {"successful_data_backup": False, "successful_backup_age_hours": None, "flashback_on": "NO",
                       "block_corruption": {"items": [{"file_number": 2, "block_number": 7, "blocks": 1, "corruption_type": "CHECKSUM"}]}}}}
        sections = self.sections(result)
        security = next(section for section in sections if section.title == "2.2.5 安全与对象健康")
        backup = next(section for section in sections if section.title == "2.2.6 备份与可恢复性")
        links = next(table for table in security.tables if table.title == "固定用户数据库链接")
        self.assertEqual(links.rows, (("APP", "REMOTE", "REMOTE_USER", "2026-01-01"),))
        self.assertIn("不能证明口令存在", "".join(security.paragraphs))
        self.assertIn("opatch lsinventory", "".join(security.paragraphs))
        self.assertEqual(dict(backup.tables[0].rows)["成功数据备份距今小时"], "无成功数据备份记录")
        self.assertEqual(next(table for table in backup.tables if table.title == "已知数据块损坏").rows, (("2", "7", "1", "CHECKSUM"),))

    def test_unavailable_security_and_backup_metrics_do_not_display_healthy_placeholders(self):
        result = {"db": {"security": {"default_password_users": {"items": [], "count": 0}},
                         "backup": {"successful_backup_age_hours": 0},
                         "collection_availability": {
                             "db.security.default_password_users": {"readable": False},
                             "db.backup.successful_backup_age_hours": {"readable": False},
                         }}}
        sections = self.sections(result)
        security = next(section for section in sections if section.title == "2.2.5 安全与对象健康")
        backup = next(section for section in sections if section.title == "2.2.6 备份与可恢复性")
        self.assertEqual(dict(security.tables[0].rows)["默认口令账号数"], "待补充")
        self.assertEqual(dict(backup.tables[0].rows)["成功数据备份距今小时"], "待补充")
