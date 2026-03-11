"""Stable DOCX table width rules for report output."""

from __future__ import annotations

from typing import Sequence

TITLE_WIDTH_WEIGHTS = {
    "文档信息": (28, 72),
    "巡检范围": (28, 72),
    "修改记录": (14, 14, 12, 60),
    "审阅记录": (14, 18, 24, 44),
    "巡检告警定义": (12, 8, 58, 22),
    "综合健康评估": (18, 12, 70),
    "风险发现与整改建议": (10, 16, 26, 28, 20),
    "系统指标": (18, 14, 68),
    "近期错误日志告警": (16, 10, 14, 60),
    "Top等待事件": (58, 14, 18),
    "元数据锁信息": (10, 12, 12, 16, 18, 16, 16),
    "占用空间top 10的表": (12, 20, 18, 16),
    "占用空间top 10的索引": (12, 18, 26, 14),
    "慢SQL top10": (58, 12, 15, 15),
    "全表扫描的SQL top10": (68, 14, 18),
    "无索引SQL top10": (68, 14, 18),
    "物理IO top 10的表": (14, 22, 12, 12, 20),
    "使用临时表的SQL top10": (58, 12, 15, 15),
    "行操作次数top10": (12, 18, 10, 10, 10, 10, 14),
    "冗余索引": (12, 18, 22, 28),
    "最近备份记录": (62, 38),
}

COLUMN_WEIGHT_HINTS = {
    "风险标识": 8,
    "风险等级": 14,
    "检查维度": 18,
    "指标": 18,
    "当前值": 14,
    "参数名称": 28,
    "定义": 52,
    "关键发现": 70,
    "风险描述": 28,
    "影响分析": 28,
    "整改建议": 20,
    "说明": 60,
    "SQL": 68,
    "sql": 58,
    "detail": 60,
    "obj_name": 16,
    "idx": 22,
    "cover": 28,
    "evt": 58,
    "disk_tmp": 15,
}

DEFAULT_WEIGHT = 16


def resolve_column_width_weights(title: str, columns: Sequence[str]) -> tuple[int, ...]:
    title_weights = TITLE_WIDTH_WEIGHTS.get(title)
    if title_weights and len(title_weights) == len(columns):
        return tuple(title_weights)
    return tuple(_column_weight(column) for column in columns)


def _column_weight(column: str) -> int:
    for token, weight in COLUMN_WEIGHT_HINTS.items():
        if token in column:
            return weight
    return DEFAULT_WEIGHT
