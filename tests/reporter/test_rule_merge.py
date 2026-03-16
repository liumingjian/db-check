from __future__ import annotations

import copy
import json
import shutil
import tempfile
import unittest
from pathlib import Path

from reporter.rules.merge import EFFECTIVE_RULE_NAME, RuleMergeError, merge_rules, write_effective_rule

ROOT = Path(__file__).resolve().parents[2]


class RuleMergeTests(unittest.TestCase):
    def test_merge_rules_keeps_base_rule_meta_and_appends_checks(self) -> None:
        base = json.loads((ROOT / "rules" / "oracle" / "rule.json").read_text(encoding="utf-8"))
        ext = json.loads((ROOT / "rules" / "oracle" / "rule.awr.json").read_text(encoding="utf-8"))
        merged = merge_rules(base, ext)

        self.assertEqual(merged.get("rule_meta"), base.get("rule_meta"))

        merged_checks = _collect_check_ids(merged)
        self.assertIn("4.11", merged_checks)
        self.assertIn("4.16", merged_checks)
        self.assertEqual(len(merged_checks), len(_collect_check_ids(base)) + len(_collect_check_ids(ext)))

    def test_merge_rules_rejects_check_id_conflict(self) -> None:
        base = json.loads((ROOT / "rules" / "oracle" / "rule.json").read_text(encoding="utf-8"))
        ext = json.loads((ROOT / "rules" / "oracle" / "rule.awr.json").read_text(encoding="utf-8"))
        ext_conflict = copy.deepcopy(ext)
        ext_conflict["dimensions"][0]["checks"][0]["check_id"] = "4.1"

        with self.assertRaises(RuleMergeError) as ctx:
            merge_rules(base, ext_conflict)
        self.assertIn("duplicate check_id", str(ctx.exception))

    def test_write_effective_rule_writes_file(self) -> None:
        run_dir = Path(tempfile.mkdtemp())
        try:
            out = write_effective_rule(
                run_dir=run_dir,
                base_rule=ROOT / "rules" / "oracle" / "rule.json",
                extension_rule=ROOT / "rules" / "oracle" / "rule.awr.json",
            )
            self.assertEqual(out, run_dir / EFFECTIVE_RULE_NAME)
            self.assertTrue(out.exists())
        finally:
            shutil.rmtree(run_dir, ignore_errors=True)


def _collect_check_ids(rule: dict) -> set[str]:
    ids: set[str] = set()
    for dim in rule.get("dimensions", []):
        for check in dim.get("checks", []):
            ids.add(str(check.get("check_id") or ""))
    return ids

