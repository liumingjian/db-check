from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from analyzer.cli.db_analyzer import run

ROOT = Path(__file__).resolve().parents[2]


class AnalyzerCLITests(unittest.TestCase):
    def test_generates_summary_for_oracle_rule(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            manifest = {
                "schema_version": "1.0",
                "run_id": "oracle-10.0.0.8-20260312T003000Z",
                "db_type": "oracle",
                "start_time": "2026-03-12T00:30:00+08:00",
                "end_time": "2026-03-12T00:30:05+08:00",
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
            result = {
                "meta": {
                    "schema_version": "2.0",
                    "collector_version": "1.0.0",
                    "db_type": "oracle",
                    "db_host": "10.0.0.8",
                    "db_port": 1521,
                    "db_name": "ORCL",
                    "collect_mode": "remote",
                    "collect_time": "2026-03-12T00:30:05+08:00",
                    "collect_duration_seconds": 5,
                    "os_sample_count": 1,
                    "timezone": "Asia/Shanghai",
                },
                "collect_config": {
                    "sample_mode": "single",
                    "sample_interval_seconds": None,
                    "sample_period_seconds": None,
                    "expected_samples": 1,
                },
                "collect_window": {
                    "window_start": "2026-03-12T00:30:00+08:00",
                    "window_end": "2026-03-12T00:30:05+08:00",
                    "duration_seconds": 5,
                },
                "os": {
                    "cpu": {"samples": [{"timestamp": "2026-03-12T00:30:05+08:00", "usage_percent": 81.2}]},
                    "memory": {"samples": [{"timestamp": "2026-03-12T00:30:05+08:00", "usage_percent": 62.5}]},
                    "filesystem": {
                        "samples": [{"timestamp": "2026-03-12T00:30:05+08:00", "mountpoints": [{"usage_percent": 91.0}]}]
                    },
                },
                "db": {
                    "basic_info": {"is_rac": False},
                    "config_check": {"spfile": ""},
                    "backup": {
                        "archive_log_mode": "NOARCHIVELOG",
                        "jobs": {"items": [], "count": 0},
                        "recovery_area": {"items": [{"space_used_pct": 92.5}], "count": 1},
                        "archive_destination_errors": {"items": [{"dest_name": "LOG_ARCHIVE_DEST_1"}], "count": 1},
                    },
                    "storage": {
                        "tablespace_usage": {"items": [{"real_percent": 96.3}], "count": 1},
                        "recover_files": {"items": [{"file_id": 7}], "count": 1},
                        "invalid_objects": {"items": [{"owner": "APP", "object_type": "VIEW"}], "count": 1},
                        "invalid_indexes": {"items": [], "count": 0},
                    },
                    "performance": {
                        "redo_switch_daily": {"items": [{"switch_count": 120}], "count": 1},
                        "long_transactions": {"items": [{"sid": 10}], "count": 1},
                        "blocking_chains": {"items": [{"waiter_sid": 11}], "count": 1},
                        "redo_nowait_pct": 91.2,
                        "undo_tablespace_usage": {"items": [{"usage_percent": 94.8}], "count": 1},
                    },
                    "sql_analysis": {
                        "top_sql_by_elapsed_time": {"items": [{"sql_text": "select 1"}], "count": 1},
                        "top_sql_by_buffer_gets": {"items": [], "count": 0},
                        "top_sql_by_disk_reads": {"items": [{"sql_text": "select 2"}], "count": 1},
                        "high_parse_count_sql": {"items": [{"sql_id": "1abcd"}], "count": 1},
                        "high_version_count_sql": {"items": [{"sql_id": "2bcde"}], "count": 1},
                    },
                    "security": {
                        "disabled_constraints": {"items": [{"constraint_name": "C1"}], "count": 1},
                        "disabled_triggers": {"items": [], "count": 0},
                        "dba_role_users": {"items": [{"grantee": "SYSTEM"}, {"grantee": "APP"}, {"grantee": "OPS"}], "count": 3},
                        "expired_users": {"items": [{"username": "LOCKED_JOB"}], "count": 1},
                    },
                },
            }

            manifest_path = temp_path / "manifest.json"
            result_path = temp_path / "result.json"
            out_path = temp_path / "summary.json"
            manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
            result_path.write_text(json.dumps(result), encoding="utf-8")

            code = run(
                [
                    "--manifest",
                    str(manifest_path),
                    "--result",
                    str(result_path),
                    "--rule",
                    str(ROOT / "rules" / "oracle" / "rule.json"),
                    "--out",
                    str(out_path),
                ]
            )

            self.assertEqual(code, 0)
            payload = json.loads(out_path.read_text(encoding="utf-8"))
            self.assertEqual(payload["rule_version"], "1.0.0")
            abnormal_ids = {item["check_id"] for item in payload["abnormal_items"]}
            self.assertTrue({"4.6", "4.7", "4.8", "5.4", "6.2", "6.3"}.issubset(abnormal_ids))
            self.assertGreaterEqual(payload["counts"]["critical"], 7)
            self.assertGreaterEqual(payload["counts"]["warning"], 8)

    def test_sample_generation_success(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            out_path = Path(temp_dir) / "summary.json"
            code = run(
                [
                    "--manifest",
                    str(ROOT / "contracts" / "manifest.sample.json"),
                    "--result",
                    str(ROOT / "contracts" / "result.sample.json"),
                    "--rule",
                    str(ROOT / "contracts" / "rule.sample.json"),
                    "--out",
                    str(out_path),
                ]
            )
            self.assertEqual(code, 0)
            payload = json.loads(out_path.read_text(encoding="utf-8"))
            self.assertEqual(payload["schema_version"], "1.0")
            self.assertTrue(payload["run_id"])

    def test_returns_40_when_required_args_missing(self) -> None:
        code = run([])
        self.assertEqual(code, 40)

    def test_returns_41_when_input_not_found(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            out_path = temp_path / "summary.json"
            code = run(
                [
                    "--manifest",
                    str(temp_path / "missing-manifest.json"),
                    "--result",
                    str(ROOT / "contracts" / "result.sample.json"),
                    "--rule",
                    str(ROOT / "contracts" / "rule.sample.json"),
                    "--out",
                    str(out_path),
                ]
            )
            self.assertEqual(code, 41)

    def test_returns_43_on_cross_file_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            manifest = json.loads((ROOT / "contracts" / "manifest.sample.json").read_text(encoding="utf-8"))
            result = json.loads((ROOT / "contracts" / "result.sample.json").read_text(encoding="utf-8"))
            rule = json.loads((ROOT / "contracts" / "rule.sample.json").read_text(encoding="utf-8"))
            result["meta"]["db_type"] = "oracle"

            manifest_path = temp_path / "manifest.json"
            result_path = temp_path / "result.json"
            rule_path = temp_path / "rule.json"
            out_path = temp_path / "summary.json"

            manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
            result_path.write_text(json.dumps(result), encoding="utf-8")
            rule_path.write_text(json.dumps(rule), encoding="utf-8")

            code = run(
                [
                    "--manifest",
                    str(manifest_path),
                    "--result",
                    str(result_path),
                    "--rule",
                    str(rule_path),
                    "--out",
                    str(out_path),
                ]
            )
            self.assertEqual(code, 43)


if __name__ == "__main__":
    unittest.main()
