from __future__ import annotations

import json
import unittest
from pathlib import Path

from analyzer.evaluator.rule_engine import generate_summary

ROOT = Path(__file__).resolve().parents[2]


class OraclePerformanceJudgmentTests(unittest.TestCase):
    def setUp(self):
        self.result = json.loads((ROOT / 'tests/fixtures/oracle_os_unprovided/result.json').read_text())
        self.manifest = json.loads((ROOT / 'tests/fixtures/oracle_os_unprovided/manifest.json').read_text())
        self.rule = json.loads((ROOT / 'rules/oracle/rule.json').read_text())

    def summary(self):
        return generate_summary(self.manifest, self.result, self.rule)

    def states(self):
        summary = self.summary()
        return {item['check_id']: item for item in summary['abnormal_items'] + summary['unevaluated_items']}

    def test_wait_latency_thresholds(self):
        for latency, level in ((10, None), (10.1, 'warning'), (50, 'warning'), (50.1, 'critical')):
            with self.subTest(latency=latency):
                self.result['db']['performance']['wait_events'] = {'items': [{'event': 'db file sequential read', 'avg_wait_ms': latency}]}
                item = self.states().get('4.11')
                self.assertEqual(item['level'] if item else None, level)

    def test_ratios_cover_latch_resource_and_time_model_boundaries(self):
        performance = self.result['db']['performance']
        for latch, resource, parsing, expected in ((1, 80, 10, None), (1.1, 80.1, 10.1, 'warning'), (5.1, 90.1, 20.1, 'critical')):
            with self.subTest(expected=expected):
                performance['latch_miss_ratios'] = {'items': [{'miss_pct': latch}]}
                performance['resource_limits'] = {'items': [{'resource_name': 'sessions', 'usage_pct': resource}]}
                performance['time_model_ratios'] = {'items': [{'parse_pct': parsing}]}
                states = self.states()
                for check_id in ('4.12', '4.13', '4.14'):
                    item = states.get(check_id)
                    self.assertEqual(item['level'] if item else None, expected)

    def test_unknown_denominators_and_source_failures_are_never_normal(self):
        performance = self.result['db']['performance']
        performance['latch_miss_ratios'] = {'items': [{'miss_pct': None}]}
        performance['resource_limits'] = {'items': [{'limit_value': 'UNLIMITED', 'usage_pct': None}]}
        performance['time_model_ratios'] = {'items': [{'parse_pct': None}]}
        self.result['db']['collection_availability'] = {
            'db.performance.' + source: {'readable': False, 'reason': 'No usable denominator or sample',
                                         'remediation': 'Confirm timing and resource limits, then recollect.'}
            for source in ('latch_miss_ratios', 'resource_limits', 'time_model_ratios')}
        states = self.states()
        for check_id in ('4.12', '4.13', '4.14'):
            self.assertIn(check_id, states)
            self.assertNotIn(states[check_id].get('level'), ('normal', 'warning', 'critical'))
        self.result['db']['collection_availability'] = {
            'db.performance.latch_miss_ratios': {'readable': False, 'error_code': 'ORA-01031', 'reason': 'denied', 'remediation': 'Grant latch access.'},
            'db.performance.time_model_ratios': {'readable': False, 'error_code': 'ORA-03113', 'reason': 'lost connection'}}
        self.assertEqual(self.states()['4.12']['reason_type'], 'insufficient_privilege')
        self.assertEqual(self.states()['4.14']['reason_type'], 'not_collected')

    def test_undo_errors_and_parallel_degrees_produce_findings(self):
        self.result['db']['performance']['undo_stats'] = {'items': [{'error_count': 1}]}
        self.result['db']['security'].update(table_degree_gt_one={'items': [{'table_name': 'ORDERS', 'degree': 'DEFAULT'}]},
                                           indexes_degree_gt_one={'items': [{'index_name': 'ORDER_IDX', 'degree': '16'}]})
        self.assertEqual(self.states()['4.15']['level'], 'critical')
        self.assertEqual(self.states()['4.16']['level'], 'warning')
        self.assertEqual(self.states()['4.17']['level'], 'warning')
        self.result['db']['performance']['undo_stats'] = {'items': [{'error_count': 0}]}
        self.result['db']['security'].update(table_degree_gt_one={'items': []}, indexes_degree_gt_one={'items': []})
        states = self.states()
        for check_id in ('4.15', '4.16', '4.17'):
            self.assertNotIn(check_id, states)

    def test_original_27_identifiers_remain_in_original_dimension(self):
        original = ['1.1', '1.2', '1.3', '2.1', '2.2', '2.3', '3.1', '3.2', '3.3', '3.4',
                    '4.1', '4.2', '4.3', '4.4', '4.5', '4.6', '4.7', '4.8', '4.9', '4.10',
                    '5.1', '5.2', '5.3', '5.4', '6.1', '6.2', '6.3']
        checks = [(dimension['dimension_id'], check['check_id']) for dimension in self.rule['dimensions']
                  for check in dimension['checks']]
        for check_id in original:
            self.assertEqual(checks.count((int(check_id.split('.')[0]), check_id)), 1)
