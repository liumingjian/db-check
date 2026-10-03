import json
import unittest
from pathlib import Path

from analyzer.evaluator.rule_engine import generate_summary

ROOT = Path(__file__).resolve().parents[2]


class OracleReviewRegressions(unittest.TestCase):
    def verdict(self, check_id, db):
        rule = json.loads((ROOT / 'rules/oracle/rule.json').read_text())
        rule['dimensions'] = [dict(d, checks=[c for c in d['checks'] if c['check_id'] == check_id])
                              for d in rule['dimensions'] if any(c['check_id'] == check_id for c in d['checks'])]
        return generate_summary({'exit_code': 0}, {'db': db}, rule)

    def test_invalid_capacity_never_reads_normal(self):
        for value in (None, 'invalid', float('nan')):
            with self.subTest(value=value):
                summary = self.verdict('3.1', {'storage': {'tablespace_usage': {'items': [{'real_percent': value}]}}})
                self.assertEqual(summary['counts']['normal'], 0)
                self.assertEqual(summary['counts']['unevaluated'], 1)

    def test_mounted_standby_rac_is_healthy_but_primary_is_not(self):
        db = {'deployment_topology': {'is_rac': True, 'role': 'standby'},
              'rac': {'instances': {'items': [{'status': 'MOUNTED', 'database_status': 'ACTIVE', 'active_state': 'NORMAL'}]}}}
        self.assertEqual(self.verdict('11.1', db)['counts']['normal'], 1)
        db['deployment_topology']['role'] = 'primary'
        self.assertEqual(self.verdict('11.1', db)['counts']['critical'], 1)
        db['deployment_topology']['role'] = 'unknown'
        self.assertEqual(self.verdict('11.1', db)['counts']['not_collected'], 1)

    def test_absent_successful_backup_is_critical_without_fake_age(self):
        summary = self.verdict('6.4', {'backup': {'successful_data_backup': False, 'successful_backup_age_hours': None}})
        self.assertEqual(summary['counts']['critical'], 1)

    def test_alert_judgments_use_raw_codes(self):
        db = {'alert_log': {'errors': {'items': [{'error_code': 'ORA-00600', 'count': 2}, {'error_code': 'ORA-00060', 'count': 1}]}}}
        self.assertEqual(self.verdict('7.1', db)['counts']['critical'], 1)
        self.assertEqual(self.verdict('7.2', db)['counts']['warning'], 1)
