from __future__ import annotations

import json
import unittest
from pathlib import Path

from analyzer.evaluator.rule_engine import generate_summary

ROOT = Path(__file__).resolve().parents[2]


class OracleSpaceAlertTests(unittest.TestCase):
    def setUp(self):
        self.result = json.loads((ROOT / 'tests/fixtures/oracle_os_unprovided/result.json').read_text())
        self.manifest = json.loads((ROOT / 'tests/fixtures/oracle_os_unprovided/manifest.json').read_text())
        self.rule = json.loads((ROOT / 'rules/oracle/rule.json').read_text())

    def states(self):
        summary = generate_summary(self.manifest, self.result, self.rule)
        return {item['check_id']: item for item in summary['abnormal_items'] + summary['unevaluated_items']}

    def test_space_thresholds_include_fully_used_tablespace(self):
        storage = self.result['db']['storage']
        storage.update(tablespace_usage={'items': [{'tablespace_name': 'FULL', 'real_percent': 100}]},
                       temp_usage_max_pct=91, sysaux_usage_pct=86,
                       largest_segment_usage_pct=81, recyclebin_size_gb=1)
        states = self.states()
        self.assertEqual(states['3.1']['level'], 'critical')
        self.assertEqual(states['3.5']['level'], 'critical')
        for check_id in ('3.6', '3.7', '3.8'):
            self.assertEqual(states[check_id]['level'], 'warning')

    def test_space_threshold_boundaries_and_derived_source_failure(self):
        self.result['db']['storage'].update(temp_usage_max_pct=80, sysaux_usage_pct=80,
                                           largest_segment_usage_pct=80, recyclebin_size_gb=0)
        self.assertTrue(all(check_id not in self.states() for check_id in ('3.5', '3.6', '3.7', '3.8')))
        self.result['db']['collection_availability'] = {
            'db.storage.temp_usage_max_pct': {'readable': False, 'error_code': 'ORA-01031',
                                             'reason': 'missing temp view', 'remediation': 'Grant temp access.'}}
        self.assertEqual(self.states()['3.5']['reason_type'], 'insufficient_privilege')

    def test_alert_severity_and_11g_fallback(self):
        self.result['db']['alert_log'] = {'critical_count': 4, 'warning_count': 1}
        self.assertEqual(self.states()['7.1']['level'], 'critical')
        self.assertEqual(self.states()['7.2']['level'], 'warning')
        self.result['db']['alert_log'] = {'critical_count': 0, 'warning_count': 0}
        self.assertNotIn('7.1', self.states())
        self.assertNotIn('7.2', self.states())
        self.result['db']['version_info'] = {'major': 11}
        self.result['db']['collection_availability'] = {
            'db.alert_log': {'readable': False, 'reason': 'Oracle 11g alert collection unsupported',
                             'remediation': 'Ask DBA to read alert log with ADRCI.'}}
        for check_id in ('7.1', '7.2'):
            self.assertEqual(self.states()[check_id]['reason_type'], 'not_collected')
