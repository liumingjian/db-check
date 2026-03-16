"""AWR result enrichment (result.json -> result.enriched.json)."""

from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any

from reporter.awr.html_parser import AWRPayload, parse_awr_html

ENRICHED_RESULT_NAME = "result.enriched.json"


def write_enriched_result(*, run_dir: Path, awr_file: Path) -> Path:
    result_path = run_dir / "result.json"
    if not result_path.exists() or not result_path.is_file():
        raise RuntimeError(f"result.json not found: {result_path}")
    base = json.loads(result_path.read_text(encoding="utf-8"))
    if not isinstance(base, dict):
        raise RuntimeError("result.json root must be object")

    awr = parse_awr_html(awr_file)
    _validate_awr_identity_matches_result(base, awr)
    enriched = _enrich_result(base, awr)

    out = run_dir / ENRICHED_RESULT_NAME
    out.write_text(json.dumps(enriched, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return out


def _validate_awr_identity_matches_result(result: dict[str, Any], awr: AWRPayload) -> None:
    db = result.get("db")
    if not isinstance(db, dict):
        raise RuntimeError("result.db must be object")
    basic = db.get("basic_info")
    if not isinstance(basic, dict):
        raise RuntimeError("result.db.basic_info must be object")

    db_name = str(basic.get("db_name") or "").strip()
    if not db_name:
        raise RuntimeError("result.db.basic_info.db_name is missing")
    dbid = basic.get("dbid")
    if not isinstance(dbid, int):
        raise RuntimeError("result.db.basic_info.dbid must be integer")

    if dbid != awr.metadata.db_id:
        raise RuntimeError(f"AWR identity mismatch: DBID result={dbid} awr={awr.metadata.db_id}")
    if db_name.lower() != awr.metadata.db_name.lower():
        raise RuntimeError(f"AWR identity mismatch: DB Name result={db_name!r} awr={awr.metadata.db_name!r}")


def _enrich_result(result: dict[str, Any], awr: AWRPayload) -> dict[str, Any]:
    enriched = copy.deepcopy(result)
    db = enriched.get("db")
    if not isinstance(db, dict):
        raise RuntimeError("result.db must be object")
    db["awr"] = awr.to_result_payload()
    return enriched

