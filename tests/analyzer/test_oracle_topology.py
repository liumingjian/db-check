from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from analyzer.cli.db_analyzer import run
from analyzer.evaluator.rule_engine import generate_summary

ROOT = Path(__file__).resolve().parents[2]
FIXTURE = ROOT / "tests" / "fixtures" / "oracle_os_unprovided"


class OracleTopologyAnalyzerTests(unittest.TestCase):
    def test_version_and_topology_fixture_is_accepted_with_strict_schema(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "summary.json"
            code = run([
                "--manifest", str(FIXTURE / "manifest.json"), "--result", str(FIXTURE / "result.json"),
                "--rule", str(ROOT / "rules" / "oracle" / "rule.json"), "--strict-schema", "--out", str(output),
            ])
            self.assertEqual(code, 0)
            summary = json.loads(output.read_text(encoding="utf-8"))
            self.assertNotIn("failure", summary)

    def test_analyzer_can_judge_detected_version_and_each_topology_facet(self) -> None:
        result = json.loads((FIXTURE / "result.json").read_text(encoding="utf-8"))
        manifest = json.loads((FIXTURE / "manifest.json").read_text(encoding="utf-8"))
        expected = {
            "version_info.major": 19,
            "version_info.family": "19c",
            "deployment_topology.is_rac": False,
            "deployment_topology.is_cdb": True,
            "deployment_topology.role": "primary",
            "deployment_topology.is_asm": False,
            "deployment_topology.connected_container": "CDB$ROOT",
            "deployment_topology.pdbs.items[*].name": "APP",
            "deployment_topology.pdbs.items[*].size_bytes": 209715200,
        }
        checks = [
            {"check_id": f"99.{index}", "name": path, "source_module": "db_basic", "priority": "P1",
             "extract": {"json_path": f"db.{path}", "aggregation": "raw"},
             "thresholds": {"normal": {"operator": "==", "value": value}, "critical": {"operator": "!=", "value": value}}}
            for index, (path, value) in enumerate(expected.items(), 1)
        ]
        summary = generate_summary(manifest, result, {
            "rule_meta": {"rule_version": "1.0"}, "dimensions": [{"dimension_id": 99, "name": "topology", "checks": checks}],
        })
        self.assertEqual(summary["counts"]["normal"], 9)
        self.assertEqual(summary["abnormal_items"], [])
        self.assertEqual(summary["unevaluated_items"], [])
