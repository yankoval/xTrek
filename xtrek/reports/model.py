"""Neutral, presentation-independent report model."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
import re
from typing import ClassVar, Mapping, Optional, Sequence, Tuple, Union
from urllib.parse import urlsplit


ALIGNMENTS = {"auto", "left", "center", "right", "decimal"}
BORDER_STYLES = {"none", "solid", "dashed", "dotted", "double"}
VERTICAL_ALIGNMENTS = {"top", "middle", "bottom"}
MESSENGER_TABLE_LAYOUTS = {"cards", "key_value"}
_COLUMN_WIDTH = re.compile(r"^(?:auto|\d+(?:\.\d+)?(?:%|px|pt|em|rem|ch|cm|mm|in)?)$")
_BORDER_COLOR = re.compile(r"^(?:#[0-9a-fA-F]{3,8}|[a-zA-Z]+)$")


@dataclass(frozen=True)
class Link:
    text: str
    url: str
    title: Optional[str] = None
    kind: ClassVar[str] = "link"

    def __post_init__(self) -> None:
        scheme = urlsplit(self.url).scheme.lower()
        if scheme not in {"", "http", "https", "mailto"}:
            raise ValueError(f"Unsupported link scheme: {scheme}")


@dataclass(frozen=True)
class NumberFormat:
    decimal_places: int = 0
    thousands_separator: str = " "
    decimal_separator: str = ","
    prefix: str = ""
    suffix: str = ""

    def __post_init__(self) -> None:
        if self.decimal_places < 0:
            raise ValueError("decimal_places cannot be negative")
        if self.thousands_separator == self.decimal_separator:
            raise ValueError("Decimal and thousands separators must differ")


@dataclass(frozen=True)
class Border:
    width: float = 1.0
    style: str = "solid"
    color: str = "#d1d5db"

    def __post_init__(self) -> None:
        if self.width < 0:
            raise ValueError("Border width cannot be negative")
        if self.style not in BORDER_STYLES:
            raise ValueError(f"Unsupported border style: {self.style}")
        if not _BORDER_COLOR.fullmatch(self.color):
            raise ValueError(f"Unsupported border color: {self.color}")


@dataclass(frozen=True)
class TableColumn:
    title: str
    align: str = "auto"
    vertical_align: str = "middle"
    width: Optional[str] = None
    number_format: Optional[NumberFormat] = None
    header_align: Optional[str] = None

    def __post_init__(self) -> None:
        if self.align not in ALIGNMENTS:
            raise ValueError(f"Unsupported column alignment: {self.align}")
        if self.header_align is not None and self.header_align not in ALIGNMENTS - {"auto"}:
            raise ValueError(f"Unsupported header alignment: {self.header_align}")
        if self.vertical_align not in VERTICAL_ALIGNMENTS:
            raise ValueError(f"Unsupported vertical alignment: {self.vertical_align}")
        if self.width is not None and not _COLUMN_WIDTH.fullmatch(self.width):
            raise ValueError(f"Unsupported column width: {self.width}")


@dataclass(frozen=True)
class TableStyle:
    outer_border: Border = field(default_factory=lambda: Border(width=1.0))
    row_border: Border = field(default_factory=lambda: Border(width=0.5))
    column_border: Border = field(default_factory=lambda: Border(width=0.5))
    header_border: Border = field(default_factory=lambda: Border(width=1.0))
    cell_padding: float = 8.0
    repeat_header: bool = True
    striped_rows: bool = False
    messenger_layout: str = "cards"

    def __post_init__(self) -> None:
        if self.cell_padding < 0:
            raise ValueError("Table cell padding cannot be negative")
        if self.messenger_layout not in MESSENGER_TABLE_LAYOUTS:
            raise ValueError(
                f"Unsupported messenger table layout: {self.messenger_layout}"
            )


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
    text: "Value"
    kind: ClassVar[str] = "paragraph"


@dataclass(frozen=True)
class KeyValue:
    label: str
    value: "Value"
    kind: ClassVar[str] = "key_value"


@dataclass(frozen=True)
class Table:
    headers: Tuple[str, ...] = field(default_factory=tuple)
    rows: Tuple[Tuple["Value", ...], ...] = field(default_factory=tuple)
    caption: Optional[str] = None
    columns: Tuple[TableColumn, ...] = field(default_factory=tuple)
    style: TableStyle = field(default_factory=TableStyle)
    kind: ClassVar[str] = "table"

    def __post_init__(self) -> None:
        if self.columns and self.headers and len(self.columns) != len(self.headers):
            raise ValueError("Table columns and headers must have the same width")
        if not self.columns:
            object.__setattr__(
                self,
                "columns",
                tuple(TableColumn(title=header) for header in self.headers),
            )
        if not self.headers:
            object.__setattr__(
                self,
                "headers",
                tuple(column.title for column in self.columns),
            )
        width = len(self.columns)
        if width == 0:
            raise ValueError("Table must have at least one column")
        if any(len(row) != width for row in self.rows):
            raise ValueError("Every table row must match the number of headers")


ScalarValue = Union[str, int, float, Decimal, date, datetime]
Value = Union[ScalarValue, Link]
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
