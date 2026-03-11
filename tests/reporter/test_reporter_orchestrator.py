from __future__ import annotations

import shutil
import tempfile
import unittest
from pathlib import Path
import json

from reporter.cli.reporter_orchestrator import run

ROOT = Path(__file__).resolve().parents[2]


class ReporterOrchestratorTests(unittest.TestCase):
    def test_run_dir_generates_docx_without_markdown_by_default(self) -> None:
        run_dir = self._prepare_run_dir()
        try:
            code = run(
                [
                    "--run-dir",
                    str(run_dir),
                    "--rule-file",
                    str(ROOT / "rules" / "mysql" / "rule.json"),
                    "--template-file",
                    str(ROOT / "reporter" / "templates" / "mysql-template.docx"),
                ]
            )
            self.assertEqual(code, 0)
            self.assertTrue((run_dir / "summary.json").exists())
            self.assertTrue((run_dir / "report-meta.json").exists())
            self.assertTrue((run_dir / "report-view.json").exists())
            self.assertTrue((run_dir / "report.docx").exists())
            self.assertFalse((run_dir / "report.md").exists())
        finally:
            shutil.rmtree(run_dir, ignore_errors=True)

    def test_run_dir_generates_markdown_when_requested(self) -> None:
        run_dir = self._prepare_run_dir()
        try:
            code = run(
                [
                    "--run-dir",
                    str(run_dir),
                    "--rule-file",
                    str(ROOT / "rules" / "mysql" / "rule.json"),
                    "--template-file",
                    str(ROOT / "reporter" / "templates" / "mysql-template.docx"),
                    "--out-md",
                    str(run_dir / "report.md"),
                ]
            )
            self.assertEqual(code, 0)
            self.assertTrue((run_dir / "report.md").exists())
        finally:
            shutil.rmtree(run_dir, ignore_errors=True)

    def test_run_dir_detects_version_from_basic_info_version_vars(self) -> None:
        run_dir = self._prepare_run_dir()
        try:
            result_path = run_dir / "result.json"
            result = json.loads(result_path.read_text(encoding="utf-8"))
            result["db"]["basic_info"].pop("version", None)
            result_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
            code = run(
                [
                    "--run-dir",
                    str(run_dir),
                    "--rule-file",
                    str(ROOT / "rules" / "mysql" / "rule.json"),
                    "--template-file",
                    str(ROOT / "reporter" / "templates" / "mysql-template.docx"),
                ]
            )
            self.assertEqual(code, 0)
            self.assertTrue((run_dir / "report.docx").exists())
        finally:
            shutil.rmtree(run_dir, ignore_errors=True)

    def _prepare_run_dir(self) -> Path:
        run_dir = Path(tempfile.mkdtemp())
        shutil.copy(ROOT / "contracts" / "manifest.sample.json", run_dir / "manifest.json")
        shutil.copy(ROOT / "contracts" / "result.sample.json", run_dir / "result.json")
        return run_dir


if __name__ == "__main__":
    unittest.main()
