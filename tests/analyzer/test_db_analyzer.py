from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from analyzer.cli.db_analyzer import run

ROOT = Path(__file__).resolve().parents[2]


class AnalyzerCLITests(unittest.TestCase):
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
