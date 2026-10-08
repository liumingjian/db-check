from __future__ import annotations

import unittest

from reporter.content.oracle_report_builder import build_oracle_report_view


class OracleSpaceAlertReportTests(unittest.TestCase):
    def test_report_includes_space_evidence_and_bounded_alert_summary(self):
        result = {'db': {'storage': {
            'temp_usage': {'items': [{'tablespace_name': 'TEMP16K', 'block_size': 16384,
                                     'total_size_gb': 1, 'max_size_gb': 2, 'used_size_gb': 1, 'real_percent': 50}]},
            'sysaux_usage': {'items': [{'tablespace_name': 'SYSAUX', 'real_percent': 86}]},
            'largest_segments': {'items': [{'owner': 'APP', 'segment_name': 'ORDERS', 'size_gb': 9}]},
            'recyclebin_size_gb': 1.5,
        }, 'alert_log': {'window_days': 7, 'record_limit': 200, 'matching_records': 200, 'limit_reached': True, 'errors': {'items': [
            {'error_code': 'ORA-00600', 'count': 3, 'latest_time': '2026-10-03'}]}}}}
        rendered = str(build_oracle_report_view(result, {}, {}).to_dict())
        for expected in ('TEMP16K', '16384', 'SYSAUX', 'ORDERS', '回收站', '1.50', 'ORA-00600', '200', '7 天'):
            self.assertIn(expected, rendered)

    def test_unread_alert_does_not_claim_no_errors(self):
        result = {'db': {'alert_log': {'errors': {'items': []}}, 'collection_availability': {
            'db.alert_log': {'readable': False, 'reason': '11g unsupported', 'remediation': 'Read ADRCI.'}}}}
        rendered = str(build_oracle_report_view(result, {}, {}).to_dict())
        self.assertIn('Read ADRCI.', rendered)
        self.assertIn('未采集', rendered)
