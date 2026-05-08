"""Risk-level table cell formatting."""

from __future__ import annotations

from docx.shared import RGBColor

RISK_LEVEL_COLORS = {
    "高风险": RGBColor(0xC0, 0x00, 0x00),
    "中风险": RGBColor(0xC6, 0x92, 0x00),
    "低风险": RGBColor(0x15, 0x65, 0xC0),
    "正常": RGBColor(0x2E, 0x7D, 0x32),
}


def is_risk_level_column(column: str) -> bool:
    return column == "风险等级"


def risk_level_color(text: str) -> RGBColor | None:
    return RISK_LEVEL_COLORS.get(text.strip())
