import unittest

from reporter.content.oracle_report_builder import build_oracle_report_view


class OracleGapReportTests(unittest.TestCase):
    def test_new_dimensions_reach_comprehensive_assessment(self):
        for dimension in ('告警日志', 'Data Guard', 'ASM 存储', 'RAC 实例', 'Oracle 主机服务', '补丁与组件'):
            with self.subTest(dimension=dimension):
                summary = {'overall_risk': 'high', 'abnormal_items': [{'check_id': 'test', 'name': 'Observed failure',
                           'dimension_name': dimension, 'level': 'critical', 'reason': 'Observed failure'}],
                           'unevaluated_items': [], 'counts': {'critical': 1}}
                view = build_oracle_report_view({'db': {}}, summary, {})
                section = next(s for s in view.sections if s.title == '第一章 巡检总结')
                assessment = next(s for s in section.children if s.title == '1.3 综合健康评估')
                self.assertTrue(any(row[1] == '高风险' for row in assessment.tables[0].rows))

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
