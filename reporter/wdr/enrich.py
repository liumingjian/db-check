"""WDR result enrichment (result.json -> result.enriched.json)."""

from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any

from reporter.wdr.html_parser import WDRPayload, parse_wdr_html

ENRICHED_RESULT_NAME = "result.enriched.json"


def write_enriched_result(*, run_dir: Path, wdr_file: Path) -> Path:
    result_path = run_dir / "result.json"
    if not result_path.exists() or not result_path.is_file():
        raise RuntimeError(f"result.json not found: {result_path}")
    base = json.loads(result_path.read_text(encoding="utf-8"))
    if not isinstance(base, dict):
        raise RuntimeError("result.json root must be object")

    wdr = parse_wdr_html(wdr_file)
    _validate_wdr_identity_matches_result(base, wdr)
    enriched = _enrich_result(base, wdr)

    out = run_dir / ENRICHED_RESULT_NAME
    out.write_text(json.dumps(enriched, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return out


def _validate_wdr_identity_matches_result(result: dict[str, Any], wdr: WDRPayload) -> None:
    meta = result.get("meta")
    if not isinstance(meta, dict):
        raise RuntimeError("result.meta must be object")
    db_name = str(meta.get("db_name") or "").strip()
    if not db_name:
        raise RuntimeError("result.meta.db_name is missing")

    wdr_db_names = {name.lower() for name in wdr.metadata.db_names}
    if db_name.lower() not in wdr_db_names:
        raise RuntimeError(f"WDR identity mismatch: DB Name result={db_name!r} wdr={sorted(wdr.metadata.db_names)!r}")


def _enrich_result(result: dict[str, Any], wdr: WDRPayload) -> dict[str, Any]:
    enriched = copy.deepcopy(result)
    db = enriched.get("db")
    if not isinstance(db, dict):
        raise RuntimeError("result.db must be object")
    db["wdr"] = wdr.to_result_payload()
    return enriched

