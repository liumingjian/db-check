from __future__ import annotations

import unittest

from reporter.content.oracle_report_builder import build_oracle_report_view


class OraclePerformanceJudgmentReportTests(unittest.TestCase):
    def test_report_explains_cumulative_ratios_and_unknown_denominators(self):
        result = {'db': {'performance': {
            'latch_miss_ratios': {'items': [{'miss_pct': 2.5}]},
            'time_model_ratios': {'items': [{'parse_pct': 12.5}]},
            'resource_limits': {'items': [{'inst_id': 2, 'resource_name': 'sessions', 'current_utilization': 90,
                                          'limit_value': 100, 'usage_pct': 90}]}}}}
        rendered = str(build_oracle_report_view(result, {}, {}).to_dict())
        for expected in ('累计', '增量', '零', 'UNLIMITED', '2.50', '12.50', '90.00%', 'TopN', '10/50'):
            self.assertIn(expected, rendered)
