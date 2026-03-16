from __future__ import annotations

import json
import shutil
import tempfile
import unittest
from pathlib import Path

from reporter.awr.enrich import ENRICHED_RESULT_NAME, write_enriched_result
from tasks.validate_frozen_contracts import Validator

ROOT = Path(__file__).resolve().parents[2]


class AWREnrichTests(unittest.TestCase):
    def test_write_enriched_result_writes_file_and_is_unit_string_safe(self) -> None:
        run_dir = Path(tempfile.mkdtemp())
        try:
            result = {
                "meta": {
                    "schema_version": "2.0",
                    "collector_version": "1.0.0",
                    "db_type": "oracle",
                    "db_host": "10.0.0.8",
                    "db_port": 1521,
                    "timezone": "Asia/Shanghai",
                    "collect_time": "2026-03-12T00:40:05+08:00",
                },
                "collect_config": {
                    "sample_mode": "single",
                    "sample_interval_seconds": None,
                    "sample_period_seconds": None,
                    "expected_samples": 1,
                },
                "collect_window": {
                    "window_start": "2026-03-12T00:40:00+08:00",
                    "window_end": "2026-03-12T00:40:05+08:00",
                    "duration_seconds": 5,
                },
                "os": {},
                "db": {"basic_info": {"db_name": "oracc", "dbid": 2668322570}},
            }
            (run_dir / "result.json").write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
            out = write_enriched_result(run_dir=run_dir, awr_file=ROOT / "resources" / "awrrpt_1_19321_19322.html")
            self.assertEqual(out, run_dir / ENRICHED_RESULT_NAME)
            self.assertTrue(out.exists())

            enriched = json.loads(out.read_text(encoding="utf-8"))
            self.assertEqual(enriched["db"]["awr"]["metadata"]["db_name"], "ORACC")
            self.assertEqual(enriched["db"]["awr"]["metadata"]["db_id"], 2668322570)
            self.assertIsInstance(enriched["db"]["awr"]["instance_efficiency"]["execute_to_parse_pct"], float)

            validator = Validator(strict_schema=False)
            validator.validate_result(enriched)
            self.assertFalse(validator.errors, msg="; ".join(validator.errors))
        finally:
            shutil.rmtree(run_dir, ignore_errors=True)

    def test_write_enriched_result_fails_on_dbid_or_name_mismatch(self) -> None:
        run_dir = Path(tempfile.mkdtemp())
        try:
            result = {
                "meta": {
                    "schema_version": "2.0",
                    "collector_version": "1.0.0",
                    "db_type": "oracle",
                    "db_host": "10.0.0.8",
                    "db_port": 1521,
                    "timezone": "Asia/Shanghai",
                    "collect_time": "2026-03-12T00:40:05+08:00",
                },
                "collect_config": {
                    "sample_mode": "single",
                    "sample_interval_seconds": None,
                    "sample_period_seconds": None,
                    "expected_samples": 1,
                },
                "collect_window": {
                    "window_start": "2026-03-12T00:40:00+08:00",
                    "window_end": "2026-03-12T00:40:05+08:00",
                    "duration_seconds": 5,
                },
                "os": {},
                "db": {"basic_info": {"db_name": "ORCL", "dbid": 123456}},
            }
            (run_dir / "result.json").write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
            with self.assertRaises(RuntimeError) as ctx:
                write_enriched_result(run_dir=run_dir, awr_file=ROOT / "resources" / "awrrpt_1_19321_19322.html")
            self.assertIn("AWR identity mismatch", str(ctx.exception))
        finally:
            shutil.rmtree(run_dir, ignore_errors=True)

