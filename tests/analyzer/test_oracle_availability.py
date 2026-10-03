from __future__ import annotations

import copy
import json
import unittest
from pathlib import Path

from analyzer.evaluator.rule_engine import generate_summary

ROOT = Path(__file__).resolve().parents[2]


class OracleAvailabilityTests(unittest.TestCase):
    def setUp(self):
        self.result = json.loads((ROOT / 'tests/fixtures/oracle_os_unprovided/result.json').read_text())
        self.manifest = json.loads((ROOT / 'tests/fixtures/oracle_os_unprovided/manifest.json').read_text())
        self.rule = json.loads((ROOT / 'rules/oracle/rule.json').read_text())

    def test_permission_failure_never_makes_empty_accounts_normal(self):
        self.result['db']['security']['expired_users'] = {'items': [], 'count': 0}
        self.result['db']['collection_availability'] = {
            'db.security.expired_users': {'readable': False, 'error_code': 'ORA-01031',
                                          'reason': 'access denied', 'remediation': 'Grant SYS.DBA_USERS and rerun.'}}
        summary = generate_summary(self.manifest, self.result, self.rule)
        gap = next(x for x in summary['unevaluated_items'] if x['check_id'] == '5.4')
        self.assertEqual(gap['reason_type'], 'insufficient_privilege')
        self.assertIn('Grant', gap['advice'])
        self.assertNotEqual(summary['overall_risk'], 'low')
        self.assertEqual(summary['counts']['insufficient_privilege'], 1)
        only_gap_rule = copy.deepcopy(self.rule)
        only_gap_rule['dimensions'] = [copy.deepcopy(self.rule['dimensions'][4])]
        only_gap_rule['dimensions'][0]['checks'] = [copy.deepcopy(self.rule['dimensions'][4]['checks'][3])]
        self.assertEqual(generate_summary(self.manifest, self.result, only_gap_rule)['overall_risk'], 'medium')

    def test_successful_empty_dataset_is_normal_and_other_failure_is_not_collected(self):
        self.result['db']['security']['expired_users'] = {'items': [], 'count': 0}
        self.result['db']['collection_availability'] = {
            'db.security.expired_users': {'readable': True},
            'db.basic_info.is_rac': {'readable': False, 'error_code': 'ORA-03113', 'reason': 'connection lost'}}
        summary = generate_summary(self.manifest, self.result, self.rule)
        gaps = {x['check_id']: x for x in summary['unevaluated_items']}
        self.assertNotIn('5.4', gaps)
        self.assertEqual(gaps['2.1']['reason_type'], 'not_collected')
        counts = summary['counts']
        self.assertEqual(counts['total_checks'], sum(counts[k] for k in ('normal', 'warning', 'critical', 'unevaluated', 'not_applicable')))

    def test_legacy_error_blocks_placeholder_and_unknown_gate_does_not_mark_na(self):
        self.result['db']['collect_errors'] = ['oracle.security.expired_users: ORA-00942: missing view']
        check = copy.deepcopy(self.rule['dimensions'][1]['checks'][0])
        check['evaluation'] = {'method': 'gate', 'gate': {'json_path': 'db.deployment_topology.is_asm', 'operator': '==', 'value': True}, 'na_check_ids': ['5.4']}
        self.rule['dimensions'][1]['checks'][0] = check
        summary = generate_summary(self.manifest, self.result, self.rule)
        self.assertNotIn('5.4', {x['check_id'] for x in summary['na_items']})
        self.assertEqual(next(x for x in summary['unevaluated_items'] if x['check_id'] == '5.4')['reason_type'], 'insufficient_privilege')
