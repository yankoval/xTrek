"""Collect and calculate data for the received-tasks report."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping, Union
from zoneinfo import ZoneInfo

from .source import TaskObjectRef, TaskSource


@dataclass(frozen=True)
class TasksReportData:
    day: date
    tasks: int
    labels: Decimal
    codes: Decimal


class TasksDataError(ValueError):
    """A task object cannot be safely included in the report."""


def _day(value: Union[str, date]) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(value)
    except (TypeError, ValueError) as exc:
        raise TasksDataError(f"Invalid report date: {value}") from exc


def _decimal(value: Any, *, field: str, ref: TaskObjectRef) -> Decimal:
    if value in (None, ""):
        return Decimal(0)
    try:
        return Decimal(str(value).replace(",", "."))
    except InvalidOperation as exc:
        raise TasksDataError(
            f"Invalid {field} value in task object {ref.key}: {value!r}"
        ) from exc


def _local_date(modified: datetime, timezone_name: str) -> date:
    if modified.tzinfo is None:
        modified = modified.replace(tzinfo=timezone.utc)
    try:
        zone = ZoneInfo(timezone_name)
    except Exception as exc:
        raise TasksDataError(f"Unknown timezone: {timezone_name}") from exc
    return modified.astimezone(zone).date()


def collect(
    source: TaskSource,
    *,
    day: Union[str, date],
    timezone_name: str = "Europe/Moscow",
) -> TasksReportData:
    """Collect one local calendar day without performing presentation work."""

    target_day = _day(day)
    tasks = 0
    labels = Decimal(0)
    codes = Decimal(0)

    for ref in source.list_objects():
        if _local_date(ref.last_modified, timezone_name) != target_day:
            continue
        try:
            payload = source.read_object(ref)
        except (OSError, ValueError, KeyError, TypeError) as exc:
            raise TasksDataError(f"Cannot read task object {ref.key}: {exc}") from exc

        passport = payload.get("PasportData") or {}
        if not isinstance(passport, Mapping):
            raise TasksDataError(f"PasportData must be an object in task {ref.key}")
        quantity = _decimal(payload.get("Quantity"), field="Quantity", ref=ref)
        pack_quantity = _decimal(
            passport.get("Product_PackQty"),
            field="PasportData.Product_PackQty",
            ref=ref,
        )
        tasks += 1
        labels += quantity
        codes += quantity * pack_quantity

    return TasksReportData(
        day=target_day,
        tasks=tasks,
        labels=labels,
        codes=codes,
    )
