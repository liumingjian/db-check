"""Inline emphasis rendering for DOCX paragraphs and table cells."""

from __future__ import annotations

import re
from typing import Any

from docx.enum.text import WD_COLOR_INDEX
from docx.shared import RGBColor

EMPHASIS_COLOR = RGBColor(0xC0, 0x00, 0x00)


def append_inline_runs(
    paragraph: Any,
    text: str,
    font_name: str,
    size_pt: float,
    color: RGBColor,
    font_setter: Any,
    emoji_only: bool = False,
) -> None:
    for is_bold, content in inline_segments(text):
        if not content:
            continue
        run = paragraph.add_run(content)
        if emoji_only:
            font_setter(run, font_name, size_pt, color=color)
            continue
        font_setter(
            run,
            font_name,
            size_pt,
            bold=is_bold,
            color=EMPHASIS_COLOR if is_bold else color,
        )
        if is_bold:
            run.font.highlight_color = WD_COLOR_INDEX.YELLOW


def inline_segments(text: str) -> list[tuple[bool, str]]:
    if "**" not in text:
        return [(False, text)]
    parts = re.split(r"(\*\*.*?\*\*)", text)
    segments: list[tuple[bool, str]] = []
    for part in parts:
        if not part:
            continue
        if part.startswith("**") and part.endswith("**") and len(part) >= 4:
            segments.append((True, part[2:-2]))
            continue
        segments.append((False, part))
    return segments
