from __future__ import annotations

import json
import unittest
from pathlib import Path

from tasks.validate_frozen_contracts import SCHEMA_DIR, Validator

ROOT = Path(__file__).resolve().parents[2]


class RuleSchemaWDRTests(unittest.TestCase):
    def test_rule_wdr_json_matches_schema_and_contract_expectations(self) -> None:
        rule_path = ROOT / "rules" / "gaussdb" / "rule.wdr.json"
        rule = json.loads(rule_path.read_text(encoding="utf-8"))
        self.assertIsInstance(rule, dict)

        v = Validator(strict_schema=True)
        v.validate_with_schema("rule", rule, SCHEMA_DIR / "rule.schema.json")
        self.assertFalse(v.errors, msg="; ".join(v.errors))

        dimensions = rule.get("dimensions")
        self.assertIsInstance(dimensions, list)
        self.assertEqual(len(dimensions), 1)
        self.assertEqual(dimensions[0].get("dimension_id"), 7)
        checks = dimensions[0].get("checks")
        self.assertIsInstance(checks, list)
        check_ids = sorted(str(item.get("check_id") or "") for item in checks)
        self.assertEqual(check_ids, ["7.10", "7.5", "7.6", "7.7", "7.8", "7.9"])

        for check in checks:
            extract = check.get("extract")
            self.assertIsInstance(extract, dict)
            json_path = str(extract.get("json_path") or "")
            self.assertTrue(json_path.startswith("db.wdr."), msg=f"invalid json_path: {json_path}")

        expected_thresholds = {
            "7.5": {"warning": ("<", 95), "critical": ("<", 90)},
            "7.6": {"warning": ("<", 95), "critical": ("<", 90)},
            "7.7": {"warning": ("<", 90), "critical": ("<", 80)},
            "7.8": {"warning": ("<", 95), "critical": ("<", 90)},
            "7.9": {"warning": (">", 80), "critical": (">", 90)},
            "7.10": {"warning": (">", 100000), "critical": (">", 500000)},
        }
        for check in checks:
            cid = str(check.get("check_id") or "")
            self.assertIn(cid, expected_thresholds)
            thresholds = check.get("thresholds")
            self.assertIsInstance(thresholds, dict)
            for level in ["warning", "critical"]:
                rule_item = thresholds.get(level)
                self.assertIsInstance(rule_item, dict)
                exp_op, exp_val = expected_thresholds[cid][level]
                self.assertEqual(rule_item.get("operator"), exp_op)
                self.assertEqual(rule_item.get("value"), exp_val)

