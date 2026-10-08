"""Diagnostic validation uses the same checks as report enrichment."""
import contextlib
import io
import json
import tempfile
import unittest
from pathlib import Path

from reporter.cli.reporter_orchestrator import run

ROOT = Path(__file__).resolve().parents[2]


class DiagnosticValidationTest(unittest.TestCase):
    def test_real_diagnostics_keep_database_identity_rejection_and_scope_each_file(self):
        cases = [
            ("awr", {"db": {"basic_info": {"db_name": "ORACC", "dbid": 2668322570}}}, "awrrpt_1_19321_19322.html", "database_name_dbid"),
            ("wdr", {"meta": {"db_name": "postgres"}, "db": {}}, "wdr_cluster.html", "database_name"),
        ]
        for diagnostic_type, result, fixture, evidence in cases:
            with self.subTest(diagnostic_type=diagnostic_type), tempfile.TemporaryDirectory() as tmp:
                directory = Path(tmp)
                (directory / "result.json").write_text(json.dumps(result))
                bad = directory / "bad.html"
                bad.write_text("<html>unsupported</html>")
                args = ["--run-dir", tmp, "--rule-file", str(ROOT / "rules/oracle/rule.json"),
                        "--template-file", str(ROOT / "reporter/templates/mysql-template.docx"),
                        f"--{diagnostic_type}-file", str(ROOT / "resources" / fixture), "--validate-diagnostics"]
                if diagnostic_type == "wdr":
                    args += ["--wdr-file", str(bad)]
                output = io.StringIO()
                with contextlib.redirect_stdout(output):
                    self.assertEqual(run(args), 0)
                checks = json.loads(output.getvalue())
                self.assertEqual(checks[0], {"kind": "checked", "evidence": evidence})
                if diagnostic_type == "wdr":
                    self.assertEqual(checks[1]["kind"], "invalid")
                if diagnostic_type == "awr":
                    result["db"]["basic_info"]["dbid"] = 1
                else:
                    result["meta"]["db_name"] = "another_database"
                (directory / "result.json").write_text(json.dumps(result))
                output = io.StringIO()
                with contextlib.redirect_stdout(output):
                    self.assertEqual(run(args), 0)
                self.assertIn("identity mismatch", json.loads(output.getvalue())[0]["message"])

    def test_invalid_attachment_is_named_without_creating_a_report(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            (directory / "result.json").write_text('{"db": {"basic_info": {"db_name": "ORACC", "dbid": 2668322570}}}')
            bad = directory / "broken.html"
            bad.write_text("<html>unsupported</html>")
            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                code = run(["--run-dir", tmp, "--rule-file", str(ROOT / "rules/oracle/rule.json"),
                            "--template-file", str(ROOT / "reporter/templates/mysql-template.docx"),
                            "--awr-file", str(bad), "--validate-diagnostics"])
            self.assertEqual(code, 0)
            checks = json.loads(output.getvalue())
            self.assertEqual(checks[0]["kind"], "invalid")
            self.assertIn("AWR", checks[0]["message"])
            self.assertFalse((directory / "report.docx").exists())
