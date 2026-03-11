from __future__ import annotations

import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path

from analyzer.cli.db_analyzer import run as run_analyzer
from reporter.cli.db_report_preview import run as run_markdown_report
from reporter.cli.generate_report_meta import run as run_meta_generator
from reporter.cli.render_template_docx import run as run_template_docx

ROOT = Path(__file__).resolve().parents[2]


class PipelineIntegrationTests(unittest.TestCase):
    def test_analyzer_markdown_and_template_docx_pipeline(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            summary_path = temp_path / "summary.json"
            meta_path = temp_path / "report-meta.json"
            markdown_path = temp_path / "report.md"
            report_view_path = temp_path / "report-view.json"
            report_path = temp_path / "report.docx"
            analyzer_code = run_analyzer(
                [
                    "--manifest",
                    str(ROOT / "contracts" / "manifest.sample.json"),
                    "--result",
                    str(ROOT / "contracts" / "result.sample.json"),
                    "--rule",
                    str(ROOT / "rules" / "mysql" / "rule.json"),
                    "--out",
                    str(summary_path),
                ]
            )
            self.assertEqual(analyzer_code, 0)
            self.assertTrue(summary_path.exists())

            meta_code = run_meta_generator(
                [
                    "--result",
                    str(ROOT / "contracts" / "result.sample.json"),
                    "--summary",
                    str(summary_path),
                    "--mysql-version",
                    "8.0",
                    "--out",
                    str(meta_path),
                ]
            )
            self.assertEqual(meta_code, 0)
            self.assertTrue(meta_path.exists())

            markdown_code = run_markdown_report(
                [
                    "--result",
                    str(ROOT / "contracts" / "result.sample.json"),
                    "--summary",
                    str(summary_path),
                    "--meta",
                    str(meta_path),
                    "--out-md",
                    str(markdown_path),
                    "--out-json",
                    str(report_view_path),
                ]
            )
            self.assertEqual(markdown_code, 0)
            self.assertTrue(markdown_path.exists())
            self.assertTrue(report_view_path.exists())

            docx_code = run_template_docx(
                [
                    "--report-md",
                    str(markdown_path),
                    "--report-view",
                    str(report_view_path),
                    "--template",
                    str(ROOT / "reporter" / "templates" / "mysql-template.docx"),
                    "--out",
                    str(report_path),
                ]
            )
            self.assertEqual(docx_code, 0)
            self.assertTrue(report_path.exists())

    def test_collector_os_only_creates_run_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            env = dict(os.environ)
            env["GOCACHE"] = "/tmp/go-cache"
            command = [
                str(ROOT / "bin" / "db-collector"),
                "--db-type",
                "mysql",
                "--os-only",
                "--output-dir",
                str(temp_path),
            ]
            completed = subprocess.run(command, cwd=ROOT, env=env, capture_output=True, text=True, check=False)
            self.assertEqual(completed.returncode, 0, msg=completed.stderr)
            run_id = _extract_run_id(completed.stdout)
            run_dir = temp_path / run_id
            self.assertTrue((run_dir / "manifest.json").exists())
            self.assertTrue((run_dir / "result.json").exists())


def _extract_run_id(stdout: str) -> str:
    for line in stdout.splitlines():
        if line.startswith("run_id="):
            return line.split("=", 1)[1].strip()
    raise AssertionError(f"run_id missing from stdout: {stdout}")


if __name__ == "__main__":
    unittest.main()
