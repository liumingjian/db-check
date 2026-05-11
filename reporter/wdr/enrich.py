"""WDR result enrichment (result.json -> result.enriched.json)."""

from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any

from reporter.wdr.html_parser import WDRPayload, parse_wdr_html

ENRICHED_RESULT_NAME = "result.enriched.json"


def write_enriched_result(*, run_dir: Path, wdr_files: list[Path] | tuple[Path, ...]) -> Path:
    result_path = run_dir / "result.json"
    if not result_path.exists() or not result_path.is_file():
        raise RuntimeError(f"result.json not found: {result_path}")
    base = json.loads(result_path.read_text(encoding="utf-8"))
    if not isinstance(base, dict):
        raise RuntimeError("result.json root must be object")

    paths = _resolve_wdr_files(wdr_files)
    wdrs = [parse_wdr_html(path) for path in paths]
    _validate_wdr_identity_matches_result(base, wdrs)
    enriched = _enrich_result(base, wdrs)

    out = run_dir / ENRICHED_RESULT_NAME
    out.write_text(json.dumps(enriched, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return out


def _resolve_wdr_files(wdr_files: list[Path] | tuple[Path, ...]) -> list[Path]:
    paths = list(wdr_files)
    if not paths:
        raise RuntimeError("at least one WDR file is required")
    return paths


def _validate_wdr_identity_matches_result(result: dict[str, Any], wdrs: list[WDRPayload]) -> None:
    meta = result.get("meta")
    if not isinstance(meta, dict):
        raise RuntimeError("result.meta must be object")
    db_name = str(meta.get("db_name") or "").strip()
    if not db_name:
        raise RuntimeError("result.meta.db_name is missing")

    for wdr in wdrs:
        wdr_db_names = {name.lower() for name in wdr.metadata.db_names}
        if db_name.lower() not in wdr_db_names:
            raise RuntimeError(f"WDR identity mismatch: DB Name result={db_name!r} wdr={sorted(wdr.metadata.db_names)!r}")


def _enrich_result(result: dict[str, Any], wdrs: list[WDRPayload]) -> dict[str, Any]:
    enriched = copy.deepcopy(result)
    db = enriched.get("db")
    if not isinstance(db, dict):
        raise RuntimeError("result.db must be object")
    reports = [wdr.to_result_payload() for wdr in wdrs]
    db["wdr_reports"] = reports
    db["wdr"] = _aggregate_wdr_reports(reports)
    return enriched


def _aggregate_wdr_reports(reports: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "metadata": _aggregate_metadata(reports),
        "database_stat": _concat_payload(reports, "database_stat"),
        "load_profile": _first_load_profile(reports),
        "instance_efficiency": _aggregate_instance_efficiency(reports),
        "wait_events": _concat_payload(reports, "wait_events"),
        "io_profile": _concat_payload(reports, "io_profile"),
        "sql": {
            "by_elapsed_time": _aggregate_sql(reports, "by_elapsed_time", "total_elapse_time_us"),
            "by_cpu_time": _aggregate_sql(reports, "by_cpu_time", "cpu_time_us"),
        },
        "appendix": _aggregate_appendix(reports),
    }


def _aggregate_metadata(reports: list[dict[str, Any]]) -> dict[str, Any]:
    node_names: set[str] = set()
    db_names: set[str] = set()
    sources = []
    for report in reports:
        meta = report.get("metadata") if isinstance(report.get("metadata"), dict) else {}
        node_names.update(str(item) for item in meta.get("node_names", []) if str(item).strip())
        db_names.update(str(item) for item in meta.get("db_names", []) if str(item).strip())
        sources.append(
            {
                "report_type": str(meta.get("report_type") or ""),
                "report_scope": str(meta.get("report_scope") or ""),
                "report_node": str(meta.get("report_node") or ""),
                "snapshots": meta.get("snapshots", []),
            }
        )
    return {"node_names": sorted(node_names), "db_names": sorted(db_names), "sources": sources}


def _concat_payload(reports: list[dict[str, Any]], key: str) -> dict[str, Any]:
    items = []
    for report in reports:
        meta = report.get("metadata") if isinstance(report.get("metadata"), dict) else {}
        payload = report.get(key)
        if not isinstance(payload, dict) or not isinstance(payload.get("items"), list):
            continue
        for item in payload["items"]:
            if isinstance(item, dict):
                enriched = dict(item)
                enriched["source_scope"] = str(meta.get("report_scope") or "")
                enriched["source_node"] = str(meta.get("report_node") or enriched.get("node_name") or _metadata_nodes(meta))
                items.append(enriched)
    return {"items": items, "count": len(items)}


def _first_load_profile(reports: list[dict[str, Any]]) -> dict[str, Any]:
    return copy.deepcopy(reports[0].get("load_profile") if reports else {})


def _aggregate_instance_efficiency(reports: list[dict[str, Any]]) -> dict[str, Any]:
    keys = ("buffer_hit_pct", "effective_cpu_pct", "walwrite_nowait_pct", "soft_parse_pct", "non_parse_cpu_pct")
    values: dict[str, list[float]] = {key: [] for key in keys}
    all_items = []
    for report in reports:
        meta = report.get("metadata") if isinstance(report.get("metadata"), dict) else {}
        payload = report.get("instance_efficiency")
        if not isinstance(payload, dict):
            continue
        for key in keys:
            value = payload.get(key)
            if isinstance(value, (int, float)):
                values[key].append(float(value))
        for item in payload.get("items", []):
            if isinstance(item, dict):
                enriched = dict(item)
                enriched["source_scope"] = str(meta.get("report_scope") or "")
                enriched["source_node"] = str(meta.get("report_node") or _metadata_nodes(meta))
                all_items.append(enriched)
    out: dict[str, Any] = {key: (min(vals) if vals else None) for key, vals in values.items()}
    out["items"] = all_items
    out["count"] = len(all_items)
    return out


def _aggregate_sql(reports: list[dict[str, Any]], key: str, sort_key: str) -> dict[str, Any]:
    items = []
    for report in reports:
        meta = report.get("metadata") if isinstance(report.get("metadata"), dict) else {}
        sql = report.get("sql") if isinstance(report.get("sql"), dict) else {}
        payload = sql.get(key)
        if not isinstance(payload, dict) or not isinstance(payload.get("items"), list):
            continue
        for item in payload["items"]:
            if isinstance(item, dict):
                enriched = dict(item)
                enriched["source_scope"] = str(meta.get("report_scope") or "")
                enriched["source_node"] = str(meta.get("report_node") or enriched.get("node_name") or _metadata_nodes(meta))
                items.append(enriched)
    items.sort(key=lambda item: float(item.get(sort_key) or 0), reverse=True)
    return {"items": items[:50], "count": len(items)}


def _aggregate_appendix(reports: list[dict[str, Any]]) -> dict[str, Any]:
    bad_lock_items = []
    for report in reports:
        appendix = report.get("appendix") if isinstance(report.get("appendix"), dict) else {}
        bad_lock = appendix.get("bad_lock_stats") if isinstance(appendix.get("bad_lock_stats"), dict) else {}
        for item in bad_lock.get("items", []):
            if isinstance(item, dict):
                bad_lock_items.append(item)
    return {"bad_lock_stats": {"items": bad_lock_items, "count": len(bad_lock_items)}} if bad_lock_items else {}


def _metadata_nodes(meta: dict[str, Any]) -> str:
    node_names = meta.get("node_names")
    if not isinstance(node_names, list):
        return ""
    return ", ".join(str(name) for name in node_names if str(name).strip())
