import unittest

from reporter.content.oracle_report_builder import build_oracle_report_view


class OracleGapReportTests(unittest.TestCase):
    def test_gap_is_visible_in_closing_list_and_health_assessment(self):
        gap = {'check_id': '5.4', 'name': '过期账号', 'dimension_name': '安全与权限',
               'reason_type': 'insufficient_privilege', 'reason': 'ORA-01031', 'advice': 'GRANT SELECT ON SYS.DBA_USERS TO CHECKER; 重新采集'}
        summary = {'overall_risk': 'medium', 'abnormal_items': [], 'unevaluated_items': [gap],
                   'counts': {'total_checks': 1, 'normal': 0, 'warning': 0, 'critical': 0, 'insufficient_privilege': 1, 'not_collected': 0}}
        view = build_oracle_report_view({'db': {}, 'os': {}}, summary, {})
        text = str(view.to_dict())
        self.assertIn('权限不足', text)
        self.assertIn('GRANT SELECT ON SYS.DBA_USERS', text)
        self.assertIn('覆盖不完整', text)
        self.assertNotIn('无需整改', text)
        self.assertEqual(view.sections[-1].title, '第三章 巡检覆盖缺口与补采建议')
