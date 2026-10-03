"""Security, SQL patch and recovery checks through the analyzer boundary."""

import json
import unittest
from pathlib import Path

from analyzer.evaluator.rule_engine import generate_summary

ROOT = Path(__file__).resolve().parents[2]


class OracleSecurityDepthTests(unittest.TestCase):
    def setUp(self):
        self.rule = json.loads((ROOT / "rules/oracle/rule.json").read_text())
        self.rule["dimensions"] = [
            {**dimension, "checks": [check for check in dimension["checks"] if check["check_id"] in {
                "5.5", "5.6", "5.7", "5.8", "5.9", "5.10", "5.11", "5.12",
                "6.4", "6.5", "6.6", "6.7", "8.1", "8.2", "8.3", "8.4"}]}
            for dimension in self.rule["dimensions"]
        ]
        self.result = {"db": {"collection_availability": {}, "security": {
            "default_password_users": {"items": []}, "weak_password_profiles": {"items": []},
            "public_system_privileges": {"items": []}, "database_links": {"items": []},
            "audit_trail": "DB", "resource_limit": "TRUE", "encrypted_tablespaces": 1,
            "unlimited_resource_profiles": {"items": []},
            "installed_sql_patches": {"items": [{"patch_id": 123}]},
            "failed_patch_attempts": {"items": []}, "invalid_components": {"items": []},
            "patch_history": {"items": []},
        }, "backup": {"successful_data_backup": True, "successful_backup_age_hours": 12, "failed_jobs": {"items": []},
                       "flashback_on": "YES", "block_corruption": {"items": []}}}}

    def summary(self):
        return generate_summary({"exit_code": 0}, self.result, self.rule)

    def test_clean_evidence_is_normal(self):
        summary = self.summary()
        self.assertEqual(summary["counts"]["normal"], 16)
        self.assertEqual(summary["unevaluated_items"], [])

    def test_security_findings_and_recovery_failures_are_judged(self):
        security = self.result["db"]["security"]
        for name in ("default_password_users", "weak_password_profiles", "public_system_privileges", "database_links", "failed_patch_attempts", "invalid_components", "unlimited_resource_profiles"):
            security[name] = {"items": [{"evidence": "risk"}]}
        security.update(audit_trail="NONE", resource_limit="FALSE", encrypted_tablespaces=0, installed_sql_patches={"items": []})
        self.result["db"]["backup"].update(successful_backup_age_hours=200, failed_jobs={"items": [{"status": "FAILED"}]}, flashback_on="NO", block_corruption={"items": [{"blocks": 1}]})
        levels = {item["check_id"]: item["level"] for item in self.summary()["abnormal_items"]}
        self.assertEqual(levels, {"5.5": "critical", "5.6": "critical", "5.7": "critical", "5.8": "critical", "5.9": "warning", "5.10": "warning", "5.11": "warning", "5.12": "warning", "6.4": "critical", "6.5": "critical", "6.6": "critical", "6.7": "critical", "8.1": "warning", "8.2": "warning", "8.3": "warning"})

    def test_backup_age_distinguishes_missing_stale_and_recent(self):
        for age, level in ((49, "warning"), (169, "critical")):
            with self.subTest(age=age):
                self.result["db"]["backup"]["successful_backup_age_hours"] = age
                item = next(item for item in self.summary()["abnormal_items"] if item["check_id"] == "6.4")
                self.assertEqual(item["level"], level)

    def test_denied_and_unsupported_evidence_never_reads_normal(self):
        self.result["db"]["collection_availability"] = {
            "db.security.default_password_users": {"readable": False, "error_code": "ORA-01031"},
            "db.security.installed_sql_patches": {"readable": False, "reason": "11g history has no status"},
            "db.backup.successful_backup_age_hours": {"readable": False, "error_code": "ORA-00942"},
        }
        gaps = {item["check_id"]: item["reason_type"] for item in self.summary()["unevaluated_items"]}
        self.assertEqual(gaps, {"5.5": "insufficient_privilege", "8.1": "not_collected", "6.4": "insufficient_privilege"})
