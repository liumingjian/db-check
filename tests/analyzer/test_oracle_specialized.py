"""Specialized topology check states through the analyzer boundary."""

import json
import unittest
from pathlib import Path

from analyzer.evaluator.rule_engine import generate_summary

ROOT = Path(__file__).resolve().parents[2]


class OracleSpecializedTests(unittest.TestCase):
    def setUp(self):
        self.rule = json.loads((ROOT / "rules/oracle/rule.json").read_text())
        self.rule["dimensions"] = [d for d in self.rule["dimensions"] if d["dimension_id"] in (9, 10, 11, 12)]
        self.db = {
            "deployment_topology": {"role": "standby", "is_asm": True, "is_rac": True},
            "data_guard": {"transport_lag_seconds": 0, "apply_lag_seconds": 0, "archive_gap": {"items": []},
                           "protection_mode": "MAXIMUM AVAILABILITY", "destination_errors": {"items": []}},
            "asm": {"diskgroups": {"items": [{"state": "CONNECTED", "unhealthy": 0, "used_pct": 50, "offline_disks": 0, "usable_file_mb": 500}]}},
            "rac": {"instances": {"items": [{"inst_id": 2, "unhealthy": 0}]}, "parameters": {"items": [{"inst_id": 2, "name": "spfile", "value": "+DATA/spfile"}]}},
            "host_checks": {"listener": {"unhealthy": False}, "clusterware": {"unhealthy": False}},
        }

    def summary(self):
        return generate_summary({"exit_code": 0}, {"db": self.db}, self.rule)

    def test_standby_connected_asm_and_rac_are_normal(self):
        self.assertEqual(self.summary()["counts"]["normal"], 13)

    def test_primary_standalone_non_asm_skips_specialized_checks(self):
        self.db["deployment_topology"] = {"role": "primary", "is_asm": False, "is_rac": False}
        self.assertEqual(self.summary()["counts"]["not_applicable"], 12)

    def test_lag_gap_storage_instance_and_host_failures_are_critical(self):
        self.db["data_guard"].update(transport_lag_seconds=301, apply_lag_seconds=301, archive_gap={"items": [{"thread": 1}]}, destination_errors={"items": [{"error": "ORA-12514"}]})
        self.db["asm"]["diskgroups"]["items"][0].update(used_pct=91, offline_disks=1, unhealthy=1, usable_file_mb=-1)
        self.db["rac"]["instances"]["items"][0]["unhealthy"] = 1
        self.db["host_checks"]["clusterware"]["unhealthy"] = True
        self.assertEqual({i["check_id"] for i in self.summary()["abnormal_items"]}, {"9.1", "9.2", "9.3", "9.5", "10.1", "10.2", "10.3", "10.4", "11.1", "12.2"})

    def test_missing_lag_and_host_access_never_read_normal(self):
        self.db["collection_availability"] = {
            "db.data_guard.apply_lag_seconds": {"readable": False, "reason": "empty lag"},
            "db.host_checks.listener": {"readable": False, "reason": "no host channel"},
            "db.host_checks.clusterware": {"readable": False, "reason": "no host channel"},
        }
        self.assertEqual({i["check_id"]: i["reason_type"] for i in self.summary()["unevaluated_items"]},
                         {"9.2": "not_collected", "12.1": "not_collected", "12.2": "not_collected"})

    def test_unknown_topology_is_unevaluated_not_not_applicable(self):
        self.db = {"deployment_topology": {"role": "unknown", "is_asm": None, "is_rac": None}}
        summary = self.summary()
        self.assertEqual(summary["counts"]["normal"], 0)
        self.assertEqual(summary["counts"]["not_applicable"], 0)
        self.assertEqual(summary["counts"]["unevaluated"], 13)
