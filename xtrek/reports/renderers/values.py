"""Shared value formatting for all report renderers."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from numbers import Number
from typing import Optional
from urllib.parse import quote

from ..model import Link, NumberFormat, TableColumn, Value


def _format_number(value: Number, number_format: NumberFormat) -> str:
    try:
        decimal = Decimal(str(value))
    except InvalidOperation:
        return str(value)
    formatted = f"{decimal:,.{number_format.decimal_places}f}"
    formatted = formatted.replace(",", "\x00").replace(".", number_format.decimal_separator)
    formatted = formatted.replace("\x00", number_format.thousands_separator)
    return f"{number_format.prefix}{formatted}{number_format.suffix}"


def raw_value(value: Value) -> Value:
    return value.text if isinstance(value, Link) else value


def format_value(value: Value, column: Optional[TableColumn] = None) -> str:
    scalar = raw_value(value)
    if column and column.number_format and isinstance(scalar, Number):
        return _format_number(scalar, column.number_format)
    if isinstance(scalar, datetime):
        return scalar.isoformat(sep=" ", timespec="seconds")
    if isinstance(scalar, date):
        return scalar.isoformat()
    return str(scalar)


def effective_alignment(value: Value, column: TableColumn) -> str:
    if column.align != "auto":
        return "right" if column.align == "decimal" else column.align
    return "right" if isinstance(raw_value(value), Number) else "left"


def markdown_alignment(column: TableColumn) -> str:
    if column.align == "center":
        return ":---:"
    if column.align in {"right", "decimal"}:
        return "---:"
    return ":---"


def markdown_url(value: str) -> str:
    return quote(value, safe="/:#?&=%@+~,;._-")
