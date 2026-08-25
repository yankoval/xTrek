"""Rendering options shared by all report backends."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class OutputFormat(str, Enum):
    HTML = "html"
    MARKDOWN = "md"
    PDF = "pdf"

    @classmethod
    def parse(cls, value: str) -> "OutputFormat":
        normalized = value.lower().strip()
        if normalized == "markdown":
            normalized = "md"
        try:
            return cls(normalized)
        except ValueError as exc:
            raise ValueError(f"Unsupported report format: {value}") from exc


class RenderProfile(str, Enum):
    BROWSER = "browser"
    PRINTER = "printer"
    MESSENGER = "messenger"

    @classmethod
    def parse(cls, value: str) -> "RenderProfile":
        try:
            return cls(value.lower().strip())
        except ValueError as exc:
            raise ValueError(f"Unsupported report profile: {value}") from exc


@dataclass(frozen=True)
class RenderOptions:
    output_format: OutputFormat = OutputFormat.HTML
    profile: RenderProfile = RenderProfile.BROWSER
    theme: str = "default"
