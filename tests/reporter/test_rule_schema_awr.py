from __future__ import annotations

import json
import unittest
from pathlib import Path

from tasks.validate_frozen_contracts import SCHEMA_DIR, Validator

ROOT = Path(__file__).resolve().parents[2]


class RuleSchemaAWRTests(unittest.TestCase):
    def test_rule_awr_json_matches_schema_and_contract_expectations(self) -> None:
        rule_path = ROOT / "rules" / "oracle" / "rule.awr.json"
        rule = json.loads(rule_path.read_text(encoding="utf-8"))
        self.assertIsInstance(rule, dict)

        v = Validator(strict_schema=True)
        v.validate_with_schema("rule", rule, SCHEMA_DIR / "rule.schema.json")
        self.assertFalse(v.errors, msg="; ".join(v.errors))

        dimensions = rule.get("dimensions")
        self.assertIsInstance(dimensions, list)
        self.assertEqual(len(dimensions), 1)
        self.assertEqual(dimensions[0].get("dimension_id"), 4)
        checks = dimensions[0].get("checks")
        self.assertIsInstance(checks, list)
        check_ids = sorted(str(item.get("check_id") or "") for item in checks)
        self.assertEqual(check_ids, ["4.11", "4.12", "4.13", "4.14", "4.15", "4.16"])

        for check in checks:
            extract = check.get("extract")
            self.assertIsInstance(extract, dict)
            json_path = str(extract.get("json_path") or "")
            self.assertTrue(json_path.startswith("db.awr."), msg=f"invalid json_path: {json_path}")

        expected_thresholds = {
            "4.11": {"warning": ("<", 80), "critical": ("<", 50)},
            "4.12": {"warning": ("<", 95), "critical": ("<", 90)},
            "4.13": {"warning": ("<", 98), "critical": ("<", 95)},
            "4.14": {"warning": ("<", 95), "critical": ("<", 90)},
            "4.15": {"warning": (">", 30), "critical": (">", 50)},
            "4.16": {"warning": (">", 20), "critical": (">", 30)},
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

