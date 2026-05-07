from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from analyzer.cli.db_analyzer import run

ROOT = Path(__file__).resolve().parents[2]
FIXTURE = ROOT / "tests" / "fixtures" / "oracle_os_skip"


class OracleOSSkipAnalyzerTests(unittest.TestCase):
    def test_oracle_os_skip_partial_manifest_generates_summary(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            out_path = Path(temp_dir) / "summary.json"
            code = run(
                [
                    "--manifest",
                    str(FIXTURE / "manifest.json"),
                    "--result",
                    str(FIXTURE / "result.json"),
                    "--rule",
                    str(ROOT / "rules" / "oracle" / "rule.json"),
                    "--strict-schema",
                    "--out",
                    str(out_path),
                ]
            )

            self.assertEqual(code, 0)
            summary = json.loads(out_path.read_text(encoding="utf-8"))
            skipped_os_ids = {
                item["check_id"]
                for item in summary["unevaluated_items"]
                if item.get("source_module") == "os" and item.get("reason_type") == "skipped"
            }
            self.assertEqual({"1.1", "1.2", "1.3"}, skipped_os_ids)
            self.assertNotIn("failure", summary)


if __name__ == "__main__":
    unittest.main()
