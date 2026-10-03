"""Reject Oracle smoke artifacts that hide SQL failures behind a rendered report."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def validate_artifacts(run_dir: Path, version: str) -> None:
    def load(name: str) -> dict[str, Any]:
        return json.loads((run_dir / name).read_text(encoding="utf-8"))

    result, summary, view = load("result.json"), load("summary.json"), load("report-view.json")
    db = result["db"]
    expected = "11gR2" if version == "11g" else "19c"
    if db["version_info"]["family"] != expected:
        raise ValueError(f"expected {expected} version evidence")
    topology = db["deployment_topology"]
    if topology.get("role") != "primary" or any(topology.get(key) is not False for key in ("is_rac", "is_asm")):
        raise ValueError("expected primary, standalone, non-ASM smoke topology")
    if topology.get("is_cdb") is not (version == "19c"):
        raise ValueError("unexpected CDB evidence")
    if db.get("collect_errors"):
        raise ValueError(f"database query errors: {db['collect_errors']}")
    probes = db["inspection_account"]["object_probes"]
    if not probes or any(item.get("readable") is not True for item in probes):
        raise ValueError("privilege pre-check incomplete or failed")

    allowed_paths = {
        "db.host_checks.listener", "db.deployment_topology.pdbs",
        "db.performance.wait_events", "db.performance.latch_miss_ratios",
        "db.performance.resource_limits", "db.performance.time_model_ratios",
    }
    allowed_ids = {"12.1", "13.1", "4.11", "4.12", "4.13", "4.14"}
    if version == "11g":
        allowed_paths.update({"db.alert_log", "db.security.installed_sql_patches", "db.security.failed_patch_attempts"})
        allowed_ids.update({"7.1", "7.2", "8.1", "8.2"})
    evidence = db["collection_availability"]
    if not evidence:
        raise ValueError("missing query availability evidence")
    for path, item in evidence.items():
        if item.get("readable") is not True:
            if path not in allowed_paths or item.get("error_code") or not item.get("reason") or not item.get("remediation"):
                raise ValueError(f"unexpected unavailable dataset: {path}: {item}")

    required = {
        "storage": ("tablespace_usage", "temp_usage", "sysaux_usage", "largest_segments", "recyclebin_size_gb"),
        "security": ("default_password_users", "weak_password_profiles", "unlimited_resource_profiles",
                     "public_system_privileges", "audit_trail", "resource_limit", "encrypted_tablespaces",
                     "encryption_wallets", "database_links", "patch_history", "invalid_components",
                     "table_degree_gt_one", "indexes_degree_gt_one"),
        "backup": ("successful_backup_age_hours", "failed_jobs", "flashback_on", "block_corruption", "archive_destination_errors"),
        "performance": ("wait_events", "latch_data", "latch_miss_ratios", "resource_limits", "time_model",
                        "time_model_ratios", "undo_stats"),
    }
    for section, names in required.items():
        for name in names:
            path = f"db.{section}.{name}"
            if name not in db[section] or path not in evidence:
                raise ValueError(f"missing collected dataset/query evidence: {path}")
    if version == "19c":
        for path in ("db.alert_log", "db.security.installed_sql_patches", "db.security.failed_patch_attempts"):
            if evidence.get(path, {}).get("readable") is not True:
                raise ValueError(f"missing 19c capability evidence: {path}")
        if not topology.get("connected_container") or not topology["pdbs"]["items"]:
            raise ValueError("missing 19c connected container/PDB list")
    for item in summary["unevaluated_items"]:
        if item["check_id"] not in allowed_ids or item.get("reason_type") != "not_collected":
            raise ValueError(f"unexpected unevaluated check: {item}")
    counts = summary["counts"]
    if counts["total_checks"] != 70 or sum(counts[key] for key in ("normal", "warning", "critical", "unevaluated", "not_applicable")) != 70:
        raise ValueError("expected all 70 Oracle checks to be accounted for")
    na_ids = {item["check_id"] for item in summary["na_items"]}
    if not {"9.1", "9.2", "9.3", "9.4", "9.5", "10.1", "10.2", "10.3", "10.4", "11.1", "11.2", "12.2"}.issubset(na_ids):
        raise ValueError("missing topology-gated not-applicable checks")
    opening, closing = view["sections"][0], view["sections"][-1]
    if opening["title"] != "部署形态" or "版本验证级别" not in dict(opening["tables"][0]["rows"]):
        raise ValueError("missing opening topology/version verification")
    if "巡检覆盖缺口与补采建议" not in closing["title"]:
        raise ValueError("missing closing coverage section")
    for item in summary["unevaluated_items"]:
        rows = [row for row in closing["tables"][0]["rows"] if row[0] == item["check_id"]]
        if not rows or any(row[2] != "未采集" or not row[4].strip() for row in rows):
            raise ValueError(f"missing coverage state/remediation: {item['check_id']}")
    for name in ("report.md", "report.docx"):
        if (run_dir / name).stat().st_size == 0:
            raise ValueError(f"empty report: {name}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("run_dir", type=Path)
    parser.add_argument("--version", choices=("11g", "19c"), required=True)
    args = parser.parse_args()
    validate_artifacts(args.run_dir, args.version)
