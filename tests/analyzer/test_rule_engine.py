from __future__ import annotations

import unittest

from analyzer.evaluator.rule_engine import generate_summary


def base_manifest() -> dict:
    return {
        "run_id": "mysql-127.0.0.1-20260309T030000Z",
        "exit_code": 0,
        "module_stats": {
            "os": {"status": "success", "duration_ms": 1, "error": None},
            "db_basic": {"status": "success", "duration_ms": 1, "error": None},
            "db_replication": {"status": "success", "duration_ms": 1, "error": None},
            "db_perf": {"status": "success", "duration_ms": 1, "error": None},
        },
    }


def base_result(sample_mode: str = "single", expected_samples: int = 1) -> dict:
    return {
        "collect_config": {
            "sample_mode": sample_mode,
            "expected_samples": expected_samples,
        },
        "db": {},
        "os": {},
    }


def base_rule(checks: list[dict]) -> dict:
    return {
        "rule_meta": {"rule_version": "2.1"},
        "dimensions": [{"dimension_id": "x", "name": "demo", "checks": checks}],
    }


class RuleEngineTests(unittest.TestCase):
    def test_gate_marks_dimension_as_not_applicable(self) -> None:
        manifest = base_manifest()
        result = base_result()
        result["db"] = {
            "replication": {
                "enabled": False,
                "io_thread_running": False,
            }
        }
        rule = {
            "rule_meta": {"rule_version": "2.1"},
            "dimensions": [
                {
                    "dimension_id": "2",
                    "name": "replication",
                    "checks": [
                        {
                            "check_id": "2.0",
                            "name": "rep gate",
                            "priority": "P2",
                            "extract": {"json_path": "db.replication.enabled", "aggregation": "raw"},
                            "evaluation": {
                                "method": "gate",
                                "gate": {
                                    "json_path": "db.replication.enabled",
                                    "aggregation": "raw",
                                    "operator": "==",
                                    "value": True,
                                    "reason": "replication is not configured on this instance",
                                },
                                "na_dimension": "2",
                            },
                        },
                        {
                            "check_id": "2.1",
                            "name": "rep health",
                            "priority": "P0",
                            "extract": {"json_path": "db.replication.io_thread_running", "aggregation": "raw"},
                            "thresholds": {
                                "normal": {"operator": "==", "value": True},
                                "critical": {"operator": "==", "value": False},
                            },
                        },
                    ],
                }
            ],
        }

        summary = generate_summary(manifest, result, rule)

        self.assertEqual(2, summary["counts"]["not_applicable"])
        self.assertEqual([], summary["abnormal_items"])
        self.assertEqual({"2.0", "2.1"}, {item["check_id"] for item in summary["na_items"]})

    def test_thresholdless_list_defaults_to_informational_normal(self) -> None:
        manifest = base_manifest()
        result = base_result()
        result["db"] = {"sql_analysis": {"top_sql_by_time": {"items": [{"digest": "abc"}]}}}
        rule = base_rule(
            [
                {
                    "check_id": "4.4",
                    "name": "top sql",
                    "priority": "P1",
                    "extract": {"json_path": "db.sql_analysis.top_sql_by_time", "aggregation": "raw"},
                    "result_type": "list",
                    "thresholds": {},
                }
            ]
        )

        summary = generate_summary(manifest, result, rule)

        self.assertEqual(1, summary["counts"]["normal"])
        self.assertEqual([], summary["abnormal_items"])
        self.assertEqual([], summary["unevaluated_items"])

    def test_sample_requirement_marks_check_not_applicable(self) -> None:
        manifest = base_manifest()
        result = base_result(sample_mode="single", expected_samples=1)
        result["db"] = {"performance": {"row_lock_waits_delta": 5}}
        rule = base_rule(
            [
                {
                    "check_id": "11.7",
                    "name": "row lock waits",
                    "priority": "P1",
                    "extract": {"json_path": "db.performance.row_lock_waits_delta", "aggregation": "raw"},
                    "evaluation": {
                        "method": "threshold",
                        "sample_mode_required": "periodic",
                        "min_expected_samples": 2,
                        "thresholds": {
                            "normal": {"operator": "==", "value": 0},
                            "warning": {"operator": ">", "value": 0},
                        },
                    },
                }
            ]
        )

        summary = generate_summary(manifest, result, rule)

        self.assertEqual(1, summary["counts"]["not_applicable"])
        self.assertEqual([], summary["abnormal_items"])
        self.assertEqual("11.7", summary["na_items"][0]["check_id"])

    def test_threshold_can_use_list_payload_max_value(self) -> None:
        manifest = base_manifest()
        result = base_result()
        result["db"] = {
            "storage": {
                "auto_increment_usage": {
                    "items": [{"table_schema": "dbcheck", "table_name": "auto_inc_case", "usage_percent": 74.5}],
                    "count": 1,
                    "max_value": 74.5,
                }
            }
        }
        rule = base_rule(
            [
                {
                    "check_id": "10.3",
                    "name": "auto increment usage",
                    "priority": "P0",
                    "extract": {"json_path": "db.storage.auto_increment_usage", "aggregation": "raw"},
                    "result_type": "list",
                    "thresholds": {
                        "warning": {"operator": ">", "value": 50},
                        "critical": {"operator": ">", "value": 80},
                    },
                }
            ]
        )

        summary = generate_summary(manifest, result, rule)

        self.assertEqual(1, summary["counts"]["warning"])
        self.assertEqual("10.3", summary["abnormal_items"][0]["check_id"])


if __name__ == "__main__":
    unittest.main()
