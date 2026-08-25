"""Neutral, presentation-independent report model."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import ClassVar, Mapping, Optional, Sequence, Tuple, Union


@dataclass(frozen=True)
class Heading:
    text: str
    level: int = 1
    kind: ClassVar[str] = "heading"

    def __post_init__(self) -> None:
        if not 1 <= self.level <= 6:
            raise ValueError("Heading level must be between 1 and 6")


@dataclass(frozen=True)
class Paragraph:
    text: str
    kind: ClassVar[str] = "paragraph"


@dataclass(frozen=True)
class KeyValue:
    label: str
    value: str
    kind: ClassVar[str] = "key_value"


@dataclass(frozen=True)
class Table:
    headers: Tuple[str, ...]
    rows: Tuple[Tuple[str, ...], ...]
    caption: Optional[str] = None
    kind: ClassVar[str] = "table"

    def __post_init__(self) -> None:
        width = len(self.headers)
        if width == 0:
            raise ValueError("Table must have at least one column")
        if any(len(row) != width for row in self.rows):
            raise ValueError("Every table row must match the number of headers")


Block = Union[Heading, Paragraph, KeyValue, Table]


@dataclass(frozen=True)
class Page:
    blocks: Tuple[Block, ...]
    header: Tuple[Block, ...] = field(default_factory=tuple)
    footer: Tuple[Block, ...] = field(default_factory=tuple)
    break_after: bool = True


@dataclass(frozen=True)
class Report:
    title: str
    pages: Tuple[Page, ...]
    header: Tuple[Block, ...] = field(default_factory=tuple)
    footer: Tuple[Block, ...] = field(default_factory=tuple)
    metadata: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.pages:
            raise ValueError("Report must contain at least one page")


def as_tuple(items: Sequence[Block]) -> Tuple[Block, ...]:
    """Convert a convenient list-like block collection to the model type."""

    return tuple(items)
