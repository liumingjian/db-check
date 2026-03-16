from __future__ import annotations

import copy
import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


class PipelineWDRIntegrationTests(unittest.TestCase):
    def test_db_reporter_generates_gaussdb_wdr_artifacts_from_run_dir(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            env = dict(os.environ)
            env["GOCACHE"] = "/tmp/go-cache"
            run_dir = temp_path / "gaussdb-wdr-run"
            run_dir.mkdir()

            manifest = _gaussdb_manifest()
            result = copy.deepcopy(_gaussdb_result())
            (run_dir / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
            (run_dir / "result.json").write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")

            command = [
                "go",
                "run",
                str(ROOT / "reporter" / "cmd" / "db-reporter"),
                "--run-dir",
                str(run_dir),
                "--python-bin",
                str(ROOT / ".venv" / "bin" / "python3"),
                "--wdr-file",
                str(ROOT / "resources" / "wdr_cluster.html"),
            ]
            completed = subprocess.run(command, cwd=ROOT, env=env, capture_output=True, text=True, check=False)
            self.assertEqual(completed.returncode, 0, msg=completed.stderr)

            self.assertTrue((run_dir / "result.enriched.json").exists())
            self.assertTrue((run_dir / "rule.effective.json").exists())
            self.assertTrue((run_dir / "summary.json").exists())
            self.assertTrue((run_dir / "report-meta.json").exists())
            self.assertTrue((run_dir / "report-view.json").exists())
            self.assertTrue((run_dir / "report.docx").exists())

            summary = json.loads((run_dir / "summary.json").read_text(encoding="utf-8"))
            effective_rule = json.loads((run_dir / "rule.effective.json").read_text(encoding="utf-8"))
            self.assertEqual(summary["counts"]["total_checks"], _count_checks(effective_rule))

            abnormal_ids = {item["check_id"] for item in summary.get("abnormal_items", [])}
            self.assertIn("7.6", abnormal_ids)


def _count_checks(rule: dict) -> int:
    total = 0
    for dim in rule.get("dimensions", []):
        total += len(dim.get("checks", []))
    return total


def _gaussdb_manifest() -> dict:
    return {
        "schema_version": "1.0",
        "run_id": "gaussdb-127.0.0.1-20260316T000000Z",
        "db_type": "gaussdb",
        "start_time": "2026-03-16T00:00:00+08:00",
        "end_time": "2026-03-16T00:00:05+08:00",
        "exit_code": 0,
        "overall_status": "success",
        "module_stats": {
            "os": {"status": "success", "duration_ms": 10, "error": None},
            "db_basic": {"status": "success", "duration_ms": 10, "error": None},
            "db_storage": {"status": "success", "duration_ms": 10, "error": None},
            "db_perf": {"status": "success", "duration_ms": 10, "error": None},
            "db_security": {"status": "success", "duration_ms": 10, "error": None},
            "db_backup": {"status": "success", "duration_ms": 10, "error": None},
            "db_sql": {"status": "success", "duration_ms": 10, "error": None},
        },
        "artifacts": {"log": "collector.log", "result": "result.json", "summary": None, "report": None},
    }


def _gaussdb_result() -> dict:
    return {
        "meta": {
            "schema_version": "2.0",
            "collector_version": "1.0.0",
            "db_type": "gaussdb",
            "db_host": "127.0.0.1",
            "db_port": 8000,
            "db_name": "postgres",
            "collect_mode": "remote",
            "collect_time": "2026-03-16T00:00:05+08:00",
            "timezone": "Asia/Shanghai",
        },
        "collect_config": {
            "sample_mode": "single",
            "sample_interval_seconds": None,
            "sample_period_seconds": None,
            "expected_samples": 1,
        },
        "collect_window": {
            "window_start": "2026-03-16T00:00:00+08:00",
            "window_end": "2026-03-16T00:00:05+08:00",
            "duration_seconds": 5,
        },
        "os": {},
        "db": {
            "basic_info": {
                "summary": {
                    "gaussdb_version": "505.2.1.SPC1000",
                    "version": "505.2.1.SPC1000",
                }
            }
        },
    }

