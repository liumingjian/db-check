"""Parse GaussDB WDR HTML into structured, unit-safe metrics.

This parser is intentionally strict:
- Missing required (core) tables is an error.
- Required tables with zero rows is an error.
- Numeric values are normalized into numbers.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from reporter.wdr.errors import WDRParseError
from reporter.wdr.html_tables import (
    _Table,
    _TableCollector,
    _normalized_header,
    _optional_tables,
    _require_table,
    _row_dict,
)
from reporter.wdr.appendix_parser import parse_appendix
from reporter.wdr.sql_parser import parse_sql_by_cpu, parse_sql_by_elapsed
from reporter.wdr.html_tables import _parse_int, _parse_number


@dataclass(frozen=True)
class WDRMetadata:
    node_names: tuple[str, ...]
    db_names: tuple[str, ...]


@dataclass(frozen=True)
class WDRPayload:
    metadata: WDRMetadata
    database_stat: dict[str, Any]
    load_profile: dict[str, Any]
    instance_efficiency: dict[str, Any]
    io_profile: dict[str, Any]
    sql: dict[str, Any]
    appendix: dict[str, Any]

    def to_result_payload(self) -> dict[str, Any]:
        return {
            "metadata": {"node_names": list(self.metadata.node_names), "db_names": list(self.metadata.db_names)},
            "database_stat": self.database_stat,
            "load_profile": self.load_profile,
            "instance_efficiency": self.instance_efficiency,
            "io_profile": self.io_profile,
            "sql": self.sql,
            "appendix": self.appendix,
        }


def parse_wdr_html(path: Path) -> WDRPayload:
    if not path.exists() or not path.is_file():
        raise WDRParseError(f"wdr file not found: {path}")

    collector = _TableCollector()
    collector.feed(path.read_text(encoding="utf-8", errors="strict"))
    tables = collector.tables

    database_stat = _parse_database_stat(_require_table(tables, summary_contains="database stat"))
    metadata = _parse_metadata(database_stat)

    workload_table = _require_load_profile_workload_table(tables)
    response_table = _optional_load_profile_response_time_table(tables)
    load_profile = _parse_load_profile(workload_table, response_table)

    instance_efficiency = _parse_instance_efficiency(
        _require_table(tables, summary_contains="instance efficiency percentages")
    )
    io_profile = _parse_io_profile(_require_table(tables, summary_contains="io profile"))
    sql_elapsed = parse_sql_by_elapsed(_require_table(tables, summary_contains="sql ordered by elapsed time"))
    sql_cpu = parse_sql_by_cpu(_require_table(tables, summary_contains="sql ordered by cpu time"))

    appendix = parse_appendix(tables)

    return WDRPayload(
        metadata=metadata,
        database_stat=database_stat,
        load_profile=load_profile,
        instance_efficiency=instance_efficiency,
        io_profile=io_profile,
        sql={"by_elapsed_time": sql_elapsed, "by_cpu_time": sql_cpu},
        appendix=appendix,
    )


def _parse_metadata(database_stat: dict[str, Any]) -> WDRMetadata:
    items = database_stat.get("items")
    if not isinstance(items, list) or not items:
        raise WDRParseError("Database Stat table is empty")
    node_names = {str(item.get("node_name") or "").strip() for item in items if isinstance(item, dict)}
    node_names.discard("")
    db_names = {str(item.get("db_name") or "").strip() for item in items if isinstance(item, dict)}
    db_names.discard("")
    if not node_names:
        raise WDRParseError("Database Stat: node_names is empty")
    if not db_names:
        raise WDRParseError("Database Stat: db_names is empty")
    return WDRMetadata(node_names=tuple(sorted(node_names)), db_names=tuple(sorted(db_names)))


def _parse_database_stat(table: _Table) -> dict[str, Any]:
    header = _normalized_header(table)
    if "node name" not in header or "db name" not in header:
        raise WDRParseError("Database Stat header is invalid (missing Node Name/DB Name)")
    items: list[dict[str, Any]] = []
    for row in table.rows[1:]:
        if len(row) < len(header):
            continue
        record = _row_dict(header, row)
        node_name = str(record.get("node name") or "").strip()
        db_name = str(record.get("db name") or "").strip()
        if not node_name or not db_name:
            continue
        items.append(
            {
                "node_name": node_name,
                "db_name": db_name,
                "backends": _parse_int(record.get("backends")),
                "xact_commit": _parse_int(record.get("xact commit")),
                "xact_rollback": _parse_int(record.get("xact rollback")),
                "blks_read": _parse_int(record.get("blks read")),
                "blks_hit": _parse_int(record.get("blks hit")),
                "tuple_returned": _parse_int(record.get("tuple returned")),
                "tuple_fetched": _parse_int(record.get("tuple fetched")),
                "tuple_inserted": _parse_int(record.get("tuple inserted")),
                "tuple_updated": _parse_int(record.get("tuple updated")),
                "tuple_deleted": _parse_int(record.get("tup deleted")),
                "conflicts": _parse_int(record.get("conflicts")),
                "temp_files": _parse_int(record.get("temp files")),
                "temp_bytes": _parse_int(record.get("temp bytes")),
                "deadlocks": _parse_int(record.get("deadlocks")),
                "blk_read_time": _parse_number(record.get("blk read time")),
                "blk_write_time": _parse_number(record.get("blk write time")),
                "stats_reset": str(record.get("stats reset") or "").strip(),
            }
        )
    if not items:
        raise WDRParseError("Database Stat table is empty")
    return {"items": items, "count": len(items)}


def _require_load_profile_workload_table(tables: list[_Table]) -> _Table:
    candidates = _optional_tables(tables, summary_contains="load profile")
    for table in candidates:
        header = _normalized_header(table)
        if "metric" in header and "per second" in header:
            return table
    raise WDRParseError("missing required WDR table: load profile (workload)")


def _optional_load_profile_response_time_table(tables: list[_Table]) -> _Table | None:
    candidates = _optional_tables(tables, summary_contains="load profile")
    for table in candidates:
        header = _normalized_header(table)
        if header == ["metric", "value"]:
            return table
    return None


def _parse_load_profile(workload: _Table, response_time: _Table | None) -> dict[str, Any]:
    workload_items = _parse_load_profile_workload(workload)
    out: dict[str, Any] = {"workload": workload_items}
    if response_time is not None:
        out["sql_response_time"] = _parse_load_profile_response_time(response_time)
    return out


def _parse_load_profile_workload(table: _Table) -> dict[str, Any]:
    rows = []
    for item in table.rows[1:]:
        if len(item) < 2:
            continue
        metric = item[0].rstrip(":").strip()
        if not metric:
            continue
        rows.append(
            {
                "metric": metric,
                "per_second": _parse_number(item[1]),
                "per_transaction": _parse_number(item[2]) if len(item) > 2 else None,
                "per_exec": _parse_number(item[3]) if len(item) > 3 else None,
            }
        )
    if not rows:
        raise WDRParseError("Load Profile (workload) table is empty")
    return {"items": rows, "count": len(rows)}


def _parse_load_profile_response_time(table: _Table) -> dict[str, Any]:
    rows = []
    for item in table.rows[1:]:
        if len(item) < 2:
            continue
        metric = item[0].rstrip(":").strip()
        if not metric:
            continue
        rows.append({"metric": metric, "value": _parse_number(item[1])})
    by_metric = {str(row.get("metric") or ""): row.get("value") for row in rows if isinstance(row, dict)}
    return {
        "items": rows,
        "count": len(rows),
        "p95_us": by_metric.get("SQL response time P95(us)"),
        "p80_us": by_metric.get("SQL response time P80(us)"),
    }


def _parse_instance_efficiency(table: _Table) -> dict[str, Any]:
    header = _normalized_header(table)
    if "metric name" not in header or "metric value" not in header:
        raise WDRParseError("Instance Efficiency header is invalid")
    items = []
    for row in table.rows[1:]:
        if len(row) < 2:
            continue
        label = row[0].rstrip(":").strip()
        if not label:
            continue
        value = _parse_number(row[1])
        items.append({"label": label, "key": _efficiency_key(label), "value_pct": value})
    if not items:
        raise WDRParseError("Instance Efficiency table is empty")
    by_key = {m["key"]: m["value_pct"] for m in items if isinstance(m.get("value_pct"), (int, float))}
    return {
        "buffer_hit_pct": by_key.get("buffer_hit_pct"),
        "effective_cpu_pct": by_key.get("effective_cpu_pct"),
        "walwrite_nowait_pct": by_key.get("walwrite_nowait_pct"),
        "soft_parse_pct": by_key.get("soft_parse_pct"),
        "non_parse_cpu_pct": by_key.get("non_parse_cpu_pct"),
        "items": items,
        "count": len(items),
    }


def _efficiency_key(label: str) -> str:
    normalized = (
        str(label or "")
        .strip()
        .lower()
        .replace(" ", "")
        .replace("-", "")
        .replace("/", "")
        .replace("\\", "")
        .replace("(", "")
        .replace(")", "")
    )
    mapping = {
        "bufferhit%": "buffer_hit_pct",
        "effectivecpu%": "effective_cpu_pct",
        "walwritenowait%": "walwrite_nowait_pct",
        "softparse%": "soft_parse_pct",
        "nonparsecpu%": "non_parse_cpu_pct",
    }
    return mapping.get(normalized, normalized)


def _parse_io_profile(table: _Table) -> dict[str, Any]:
    header = _normalized_header(table)
    if "metric" not in header or "read + write per sec" not in header:
        raise WDRParseError("IO Profile header is invalid")
    items = []
    for row in table.rows[1:]:
        if len(row) < 2:
            continue
        record = _row_dict(header, row)
        metric = str(record.get("metric") or "").strip()
        if not metric:
            continue
        items.append(
            {
                "metric": metric,
                "read_write_per_sec": _parse_number(record.get("read + write per sec")),
                "read_per_sec": _parse_number(record.get("read per sec")),
                "write_per_sec": _parse_number(record.get("write per sec")),
            }
        )
    if not items:
        raise WDRParseError("IO Profile table is empty")
    return {"items": items, "count": len(items)}
