from __future__ import annotations

import json
import shutil
import tempfile
import unittest
from pathlib import Path

from reporter.cli.generate_report_meta import run

ROOT = Path(__file__).resolve().parents[2]


class ReportMetaCliTests(unittest.TestCase):
    def test_generate_report_meta_cli_uses_output_name_and_defaults(self) -> None:
        temp_dir = Path(tempfile.mkdtemp())
        try:
            out_path = temp_dir / "mysql-e2e-report.docx.meta.json"
            code = run(
                [
                    "--result",
                    str(ROOT / "contracts" / "result.sample.json"),
                    "--summary",
                    str(ROOT / "contracts" / "summary.sample.json"),
                    "--mysql-version",
                    "8.0",
                    "--out",
                    str(out_path),
                ]
            )
            self.assertEqual(code, 0)
            payload = json.loads(out_path.read_text(encoding="utf-8"))
            self.assertEqual("mysql-e2e-report.docx.meta.json", payload["doc_info"]["document_name"])
            self.assertEqual("db-check", payload["doc_info"]["author"])
            self.assertEqual("mysql巡检报告", payload["change_log"][0]["change"])
            self.assertEqual("周海波", payload["review_log"][0]["name"])
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
