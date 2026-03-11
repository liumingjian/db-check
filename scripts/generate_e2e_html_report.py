#!/usr/bin/env python3
"""Generate a standalone HTML report from an e2e run root."""

from __future__ import annotations

import argparse
import html
import json
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate HTML report for a db-check e2e run root")
    parser.add_argument("--run-root", type=Path, default=None)
    parser.add_argument("--out", type=Path, default=None)
    return parser


def parse_args() -> argparse.Namespace:
    return build_parser().parse_args()


def latest_run_root(runs_root: Path) -> Path:
    candidates = [path for path in runs_root.iterdir() if path.is_dir()]
    if not candidates:
        raise FileNotFoundError(f"未找到 e2e runs 目录: {runs_root}")
    return sorted(candidates, key=lambda item: item.stat().st_mtime, reverse=True)[0]


def detect_run_root(value: Path | None) -> Path:
    runs_root = Path("tests/e2e/runs")
    return value if value is not None else latest_run_root(runs_root)


def detect_output_path(run_root: Path, value: Path | None) -> Path:
    return value if value is not None else run_root / "e2e-report.html"


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def version_run_dirs(run_root: Path) -> list[tuple[str, Path]]:
    version_roots = sorted(path for path in run_root.iterdir() if path.is_dir() and path.name.startswith("mysql-"))
    result: list[tuple[str, Path]] = []
    for version_root in version_roots:
        run_dirs = sorted(path for path in version_root.iterdir() if path.is_dir())
        if not run_dirs:
            continue
        version = version_root.name.removeprefix("mysql-")
        result.append((version, run_dirs[-1]))
    if not result:
        raise FileNotFoundError(f"在 {run_root} 下未找到版本产物目录")
    return result


def collect_metric_paths(node: Any, prefix: str, output: set[str]) -> None:
    if isinstance(node, dict):
        for key, value in node.items():
            next_prefix = f"{prefix}.{key}" if prefix else key
            collect_metric_paths(value, next_prefix, output)
        return
    if isinstance(node, list):
        next_prefix = f"{prefix}[*]" if prefix else "[*]"
        if not node:
            output.add(next_prefix)
            return
        for item in node:
            collect_metric_paths(item, next_prefix, output)
        return
    output.add(prefix)


def grouped_metric_paths(result: dict[str, Any]) -> dict[str, list[str]]:
    output: set[str] = set()
    for key in ("os", "db"):
        collect_metric_paths(result.get(key), key, output)
    grouped: dict[str, list[str]] = defaultdict(list)
    for path in sorted(output):
        parts = path.split(".")
        group = ".".join(parts[:2]) if len(parts) > 1 else parts[0]
        grouped[group].append(path)
    return dict(grouped)


def get_value(data: dict[str, Any], *path: str) -> Any:
    current: Any = data
    for part in path:
        if not isinstance(current, dict):
            return None
        current = current.get(part)
    return current


def snapshot_items(result: dict[str, Any]) -> list[tuple[str, Any]]:
    return [
        ("CPU 使用率", get_value(result, "os", "cpu", "samples")),
        ("内存使用率", get_value(result, "os", "memory", "samples")),
        ("慢日志开关", get_value(result, "db", "config_check", "slow_query_log")),
        ("慢查询数量", get_value(result, "db", "performance", "slow_queries_count")),
        ("全表扫描比例", get_value(result, "db", "performance", "full_scan_ratio")),
        ("线程缓存命中率", get_value(result, "db", "performance", "thread_cache_hit_ratio")),
        ("行锁等待次数", get_value(result, "db", "performance", "row_lock_waits_delta")),
        ("无主键表数量", get_items_count(get_value(result, "db", "storage", "tables_without_pk"))),
        ("全表扫描 SQL 数量", get_items_count(get_value(result, "db", "sql_analysis", "full_scan_sqls"))),
    ]


def get_items_count(value: Any) -> int:
    if isinstance(value, dict):
        items = value.get("items")
        if isinstance(items, list):
            return len(items)
    return 0


def format_snapshot_value(label: str, value: Any) -> str:
    if label in {"CPU 使用率", "内存使用率"} and isinstance(value, list) and value:
        percent = value[0].get("usage_percent")
        if isinstance(percent, (int, float)):
            return f"{percent:.2f}%"
    if isinstance(value, float):
        return f"{value:.2f}"
    if value is None:
        return "-"
    return str(value)


def build_context(version: str, run_dir: Path) -> dict[str, Any]:
    result = load_json(run_dir / "result.json")
    summary = load_json(run_dir / "summary.json")
    return {
        "version": version,
        "run_dir": run_dir,
        "result": result,
        "summary": summary,
        "metric_groups": grouped_metric_paths(result),
        "snapshots": snapshot_items(result),
    }


def render_metric_groups(metric_groups: dict[str, list[str]]) -> str:
    blocks: list[str] = []
    for group, paths in metric_groups.items():
        items = "".join(f"<li><code>{escape_text(path)}</code></li>" for path in paths)
        blocks.append(
            "<details class='metric-group'>"
            f"<summary>{escape_text(group)} ({len(paths)})</summary>"
            f"<ul>{items}</ul>"
            "</details>"
        )
    return "".join(blocks)


def render_snapshots(items: list[tuple[str, Any]]) -> str:
    rows = []
    for label, value in items:
        rows.append(
            "<tr>"
            f"<th>{escape_text(label)}</th>"
            f"<td>{escape_text(format_snapshot_value(label, value))}</td>"
            "</tr>"
        )
    return "".join(rows)


def render_abnormal_items(items: list[dict[str, Any]]) -> str:
    if not items:
        return "<p class='empty'>无异常项</p>"
    rows = []
    for item in items:
        rows.append(
            "<tr>"
            f"<td>{escape_text(item.get('check_id', ''))}</td>"
            f"<td>{escape_text(item.get('level', ''))}</td>"
            f"<td>{escape_text(item.get('name', ''))}</td>"
            f"<td>{escape_text(item.get('reason', ''))}</td>"
            "</tr>"
        )
    return (
        "<table><thead><tr><th>检查项</th><th>级别</th><th>名称</th><th>原因</th></tr></thead>"
        f"<tbody>{''.join(rows)}</tbody></table>"
    )


def render_counts(counts: dict[str, Any]) -> str:
    rows = []
    for key in ("total_checks", "normal", "warning", "critical", "unevaluated", "not_applicable"):
        rows.append(
            "<tr>"
            f"<th>{escape_text(key)}</th>"
            f"<td>{escape_text(str(counts.get(key, 0)))}</td>"
            "</tr>"
        )
    return f"<table class='compact'><tbody>{''.join(rows)}</tbody></table>"


def escape_text(value: Any) -> str:
    return html.escape("" if value is None else str(value))


def render_version_section(context: dict[str, Any]) -> str:
    summary = context["summary"]
    abnormal_items = [item for item in summary.get("abnormal_items", []) if isinstance(item, dict)]
    na_items = [item.get("check_id") for item in summary.get("na_items", []) if isinstance(item, dict)]
    generated_at = summary.get("generated_at", "")
    return (
        "<section class='version-card'>"
        f"<h2>MySQL {escape_text(context['version'])}</h2>"
        "<div class='grid'>"
        "<div>"
        f"<p><strong>风险等级：</strong>{escape_text(summary.get('overall_risk', ''))}</p>"
        f"<p><strong>生成时间：</strong>{escape_text(generated_at)}</p>"
        f"<p><strong>产物目录：</strong><code>{escape_text(context['run_dir'])}</code></p>"
        f"{render_counts(summary.get('counts', {}))}"
        "</div>"
        "<div>"
        "<h3>核心采集快照</h3>"
        f"<table class='compact'><tbody>{render_snapshots(context['snapshots'])}</tbody></table>"
        "</div>"
        "</div>"
        "<h3>最终报告内容</h3>"
        f"{render_abnormal_items(abnormal_items)}"
        f"<p><strong>NA 检查项：</strong>{escape_text(', '.join(na_items) if na_items else '-')}</p>"
        "<h3>已采集指标清单</h3>"
        f"{render_metric_groups(context['metric_groups'])}"
        "</section>"
    )


def render_html(run_root: Path, contexts: list[dict[str, Any]]) -> str:
    sections = "".join(render_version_section(context) for context in contexts)
    generated_at = datetime.now().astimezone().isoformat(timespec="seconds")
    return f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>DB-Check E2E HTML Report</title>
  <style>
    :root {{
      --bg: #f5f1e8;
      --card: #fffdf8;
      --ink: #1f2937;
      --muted: #6b7280;
      --line: #d6cfc2;
      --accent: #b45309;
      --accent-soft: #f59e0b;
    }}
    body {{ margin: 0; font: 15px/1.6 "IBM Plex Sans", "PingFang SC", sans-serif; color: var(--ink); background: linear-gradient(180deg, #efe7d8, var(--bg)); }}
    main {{ max-width: 1200px; margin: 0 auto; padding: 32px 20px 56px; }}
    h1, h2, h3 {{ margin: 0 0 12px; }}
    .hero {{ padding: 28px; background: var(--card); border: 1px solid var(--line); border-radius: 18px; box-shadow: 0 18px 50px rgba(31, 41, 55, 0.08); }}
    .hero p {{ margin: 6px 0; color: var(--muted); }}
    .version-card {{ margin-top: 22px; padding: 24px; background: var(--card); border: 1px solid var(--line); border-radius: 18px; box-shadow: 0 10px 30px rgba(31, 41, 55, 0.06); }}
    .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap: 20px; }}
    table {{ width: 100%; border-collapse: collapse; margin: 10px 0 16px; }}
    th, td {{ padding: 8px 10px; border-bottom: 1px solid var(--line); text-align: left; vertical-align: top; }}
    th {{ color: var(--muted); font-weight: 600; width: 180px; }}
    .compact th {{ width: 220px; }}
    .empty {{ color: var(--muted); }}
    details {{ margin: 10px 0; border: 1px solid var(--line); border-radius: 12px; background: #fffaf0; }}
    summary {{ cursor: pointer; padding: 10px 12px; font-weight: 600; }}
    ul {{ margin: 0; padding: 0 20px 12px 36px; }}
    code {{ font-family: "JetBrains Mono", "SFMono-Regular", monospace; font-size: 12px; color: #7c2d12; }}
    .badge {{ display: inline-block; padding: 4px 10px; border-radius: 999px; background: rgba(180, 83, 9, 0.12); color: var(--accent); font-weight: 700; }}
  </style>
</head>
<body>
  <main>
    <section class="hero">
      <span class="badge">DB-Check / HTML Report</span>
      <h1>端到端巡检汇总报告</h1>
      <p><strong>e2e 目录：</strong><code>{escape_text(run_root)}</code></p>
      <p><strong>生成时间：</strong>{escape_text(generated_at)}</p>
      <p>本报告按 MySQL 5.6 / 5.7 / 8.0 汇总当前 e2e 采集到的指标路径、核心快照和最终异常结论。</p>
    </section>
    {sections}
  </main>
</body>
</html>"""


def main() -> int:
    args = parse_args()
    run_root = detect_run_root(args.run_root)
    output = detect_output_path(run_root, args.out)
    contexts = [build_context(version, run_dir) for version, run_dir in version_run_dirs(run_root)]
    output.write_text(render_html(run_root, contexts), encoding="utf-8")
    print(f"generated: {output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
