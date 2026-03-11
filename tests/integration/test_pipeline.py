from __future__ import annotations

import os
import subprocess
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


class PipelineIntegrationTests(unittest.TestCase):
    def test_db_reporter_generates_formal_artifacts_from_run_dir(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            env = dict(os.environ)
            env["GOCACHE"] = "/tmp/go-cache"
            run_dir = temp_path / "sample-run"
            run_dir.mkdir()
            (run_dir / "manifest.json").write_text((ROOT / "contracts" / "manifest.sample.json").read_text(encoding="utf-8"), encoding="utf-8")
            (run_dir / "result.json").write_text((ROOT / "contracts" / "result.sample.json").read_text(encoding="utf-8"), encoding="utf-8")
            command = [
                "go",
                "run",
                str(ROOT / "reporter" / "cmd" / "db-reporter"),
                "--run-dir",
                str(run_dir),
                "--out-md",
                str(run_dir / "report.md"),
            ]
            completed = subprocess.run(command, cwd=ROOT, env=env, capture_output=True, text=True, check=False)
            self.assertEqual(completed.returncode, 0, msg=completed.stderr)
            self.assertTrue((run_dir / "summary.json").exists())
            self.assertTrue((run_dir / "report-meta.json").exists())
            self.assertTrue((run_dir / "report-view.json").exists())
            self.assertTrue((run_dir / "report.md").exists())
            self.assertTrue((run_dir / "report.docx").exists())

    def test_collector_os_only_creates_run_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            env = dict(os.environ)
            env["GOCACHE"] = "/tmp/go-cache"
            command = [
                "go",
                "run",
                str(ROOT / "collector" / "cmd" / "db-collector"),
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
