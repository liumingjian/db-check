from __future__ import annotations

import json
import shutil
import tempfile
import unittest
from pathlib import Path

from reporter.cli.reporter_orchestrator import run

ROOT = Path(__file__).resolve().parents[2]
FIXTURE = ROOT / "tests" / "fixtures" / "oracle_os_unprovided"


class OracleOSUnprovidedOrchestratorTests(unittest.TestCase):
    def test_oracle_unprovided_os_run_dir_generates_report_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            run_dir = Path(temp_dir) / "oracle-os-unprovided"
            shutil.copytree(FIXTURE, run_dir)
            code = run(
                [
                    "--run-dir",
                    str(run_dir),
                    "--rule-file",
                    str(ROOT / "rules" / "oracle" / "rule.json"),
                    "--template-file",
                    str(ROOT / "reporter" / "templates" / "mysql-template.docx"),
                ]
            )

            self.assertEqual(code, 0)
            self.assertTrue((run_dir / "summary.json").exists())
            self.assertTrue((run_dir / "report-meta.json").exists())
            self.assertTrue((run_dir / "report-view.json").exists())
            self.assertTrue((run_dir / "report.docx").exists())
            summary = json.loads((run_dir / "summary.json").read_text(encoding="utf-8"))
            skipped_os_ids = {
                item["check_id"]
                for item in summary["unevaluated_items"]
                if item.get("source_module") == "os" and item.get("reason_type") == "skipped"
            }
            self.assertEqual({"1.1", "1.2", "1.3"}, skipped_os_ids)


if __name__ == "__main__":
    unittest.main()
