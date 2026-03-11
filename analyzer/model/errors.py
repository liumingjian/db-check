"""Analyzer exit code and exception definitions."""

from __future__ import annotations

from dataclasses import dataclass

EXIT_OK = 0
EXIT_PARAM_ERROR = 40
EXIT_INPUT_ERROR = 41
EXIT_SCHEMA_ERROR = 42
EXIT_CONSISTENCY_ERROR = 43
EXIT_RULE_EVAL_ERROR = 44
EXIT_OUTPUT_ERROR = 45
EXIT_INTERNAL_ERROR = 49


@dataclass(frozen=True)
class AnalyzerFailure(Exception):
    code: int
    message: str

    def __str__(self) -> str:
        return self.message
