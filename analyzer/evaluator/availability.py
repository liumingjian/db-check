"""Factual Oracle collection evidence, independent of threshold evaluation."""

from __future__ import annotations

from typing import Any

from analyzer.evaluator.path_eval import extract_values

GAP_REASONS = {'insufficient_privilege', 'not_collected'}


def canonical_path(scope: str) -> str:
    aliases = {'basic': 'basic_info', 'config': 'config_check', 'sql': 'sql_analysis',
               'topology': 'deployment_topology', 'version': 'version_info'}
    parts = scope.split('.')
    if parts[0] == 'oracle':
        parts[0] = 'db'
        if len(parts) > 1:
            parts[1] = aliases.get(parts[1], parts[1])
    sql_aliases = {'top_elapsed': 'top_sql_by_elapsed_time', 'top_buffer_gets': 'top_sql_by_buffer_gets',
                   'top_disk_reads': 'top_sql_by_disk_reads', 'top_executions': 'top_sql_by_executions'}
    if len(parts) > 2 and parts[1] == 'sql_analysis':
        parts[2] = sql_aliases.get(parts[2], parts[2])
    return '.'.join(parts)


def path_gap(result: dict[str, Any], path: str) -> dict[str, Any] | None:
    db = result.get('db', {})
    evidence = db.get('collection_availability', {})
    matches = [(key, value) for key, value in evidence.items()
               if isinstance(value, dict) and (path == key or path.startswith(key + '.') or path.startswith(key + '['))]
    if matches:
        _, item = max(matches, key=lambda pair: len(pair[0]))
        if item.get('readable') is True:
            return None
        code = str(item.get('error_code', ''))
        return {'reason_type': 'insufficient_privilege' if code in {'ORA-01031', 'ORA-00942'} else 'not_collected',
                'reason': str(item.get('reason') or item.get('error_message') or '数据未采集'),
                'advice': str(item.get('remediation') or '请 DBA 检查采集错误与视图访问权限，在同一连接容器重新运行 Collector。')}
    for error in db.get('collect_errors', []):
        scope, _, message = str(error).partition(':')
        key = canonical_path(scope)
        section = '.'.join(key.split('.')[:2])
        unknown_scope = scope.startswith('oracle.') and not extract_values(result, key)
        if path == key or path.startswith(key + '.') or path.startswith(key + '[') or (unknown_scope and path.startswith(section + '.')):
            return {'reason_type': 'insufficient_privilege' if 'ORA-01031' in message or 'ORA-00942' in message else 'not_collected',
                    'reason': message.strip(), 'advice': '请 DBA 根据 Oracle 错误检查对应视图权限，授权后在同一连接容器重新运行 Collector。'}
    return None


def check_gap(check: dict[str, Any], result: dict[str, Any]) -> dict[str, Any] | None:
    path = str(check.get('extract', {}).get('json_path', ''))
    gap = path_gap(result, path)
    if gap:
        return gap
    meta = check.get('evaluation', {})
    for dependency in meta.get('requires', []):
        gap = path_gap(result, dependency)
        if gap:
            return gap
    for dependency in meta.get('requires_known', []):
        gap = path_gap(result, dependency)
        values = extract_values(result, dependency)
        if gap:
            return gap
        if not values or values[0] in (None, 'unknown', ''):
            return {'reason_type': 'not_collected', 'reason': '必要的部署形态未确定',
                    'advice': '请 DBA 修复部署形态探测权限后重新采集。'}
    for key in ('gate', 'na_when'):
        condition = meta.get(key)
        if isinstance(condition, dict):
            condition_path = str(condition.get('json_path', ''))
            gap = path_gap(result, condition_path)
            values = extract_values(result, condition_path)
            if gap:
                return gap
            if condition_path.startswith('db.deployment_topology') and (not values or values[0] in (None, 'unknown', '')):
                return {'reason_type': 'not_collected', 'reason': '部署形态未知，无法判断检查是否适用',
                        'advice': '请 DBA 开放部署形态视图读取权限后，在同一连接容器重新运行 Collector。'}
    return None
