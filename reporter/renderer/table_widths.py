"""Stable DOCX table width calculation and geometry writing."""

from __future__ import annotations

from typing import Any

from docx.oxml import OxmlElement
from docx.oxml.ns import qn


def column_widths(document: Any, columns: tuple[str, ...], raw_weights: Any) -> list[int]:
    section = document.sections[0]
    available = section.page_width.twips - section.left_margin.twips - section.right_margin.twips
    weights = _normalize_width_weights(columns, raw_weights)
    total = sum(weights)
    widths = [int(available * weight / total) for weight in weights]
    widths[-1] += available - sum(widths)
    return widths


def apply_table_geometry(table: Any, widths: list[int]) -> None:
    _set_table_width(table, sum(widths))
    _set_table_grid(table, widths)


def _normalize_width_weights(columns: tuple[str, ...], raw_weights: Any) -> list[int]:
    if not isinstance(raw_weights, list):
        raw_weights = tuple(raw_weights) if isinstance(raw_weights, tuple) else ()
    if len(raw_weights) != len(columns):
        return [1] * len(columns)
    weights = [int(weight) for weight in raw_weights]
    if any(weight <= 0 for weight in weights):
        return [1] * len(columns)
    return weights


def _set_table_width(table: Any, total_width: int) -> None:
    tbl_pr = table._tbl.tblPr
    tbl_w = tbl_pr.first_child_found_in("w:tblW")
    if tbl_w is None:
        tbl_w = OxmlElement("w:tblW")
        tbl_pr.append(tbl_w)
    tbl_w.set(qn("w:type"), "dxa")
    tbl_w.set(qn("w:w"), str(total_width))


def _set_table_grid(table: Any, widths: list[int]) -> None:
    tbl_grid = table._tbl.tblGrid
    if tbl_grid is None:
        tbl_grid = OxmlElement("w:tblGrid")
        table._tbl.insert(1, tbl_grid)
    for child in list(tbl_grid):
        tbl_grid.remove(child)
    for width in widths:
        grid_col = OxmlElement("w:gridCol")
        grid_col.set(qn("w:w"), str(width))
        tbl_grid.append(grid_col)
