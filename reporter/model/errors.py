"""Reporter exit code and exception definitions."""

from __future__ import annotations

from dataclasses import dataclass

EXIT_OK = 0
EXIT_PARAM_ERROR = 50
EXIT_INPUT_ERROR = 51
EXIT_CONTRACT_ERROR = 52
EXIT_TEMPLATE_ERROR = 53
EXIT_RENDER_ERROR = 54
EXIT_OUTPUT_ERROR = 55
EXIT_INTERNAL_ERROR = 59


@dataclass(frozen=True)
class ReporterFailure(Exception):
    code: int
    message: str

    def __str__(self) -> str:
        return self.message
