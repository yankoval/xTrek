"""Collect and calculate data for the received-tasks report."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
import re
from typing import Any, Mapping, Optional, Union
from zoneinfo import ZoneInfo

from .source import TaskObjectRef, TaskSource


_MINUTE_VALUE = re.compile(r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}$")


@dataclass(frozen=True)
class TaskPeriod:
    start: datetime
    end: datetime

    @property
    def end_exclusive(self) -> datetime:
        """First instant after the inclusive ending minute."""

        return self.end + timedelta(minutes=1)


@dataclass(frozen=True)
class TasksReportData:
    day: Optional[date]
    tasks: int
    labels: Decimal
    codes: Decimal
    period: Optional[TaskPeriod] = None


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
    return _local_datetime(modified, timezone_name).date()


def _zone(timezone_name: str) -> ZoneInfo:
    try:
        return ZoneInfo(timezone_name)
    except Exception as exc:
        raise TasksDataError(f"Unknown timezone: {timezone_name}") from exc


def _local_datetime(modified: datetime, timezone_name: str) -> datetime:
    if modified.tzinfo is None:
        modified = modified.replace(tzinfo=timezone.utc)
    return modified.astimezone(_zone(timezone_name))


def _minute(value: Union[str, datetime], *, field: str, timezone_name: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
        if parsed.second or parsed.microsecond:
            raise TasksDataError(f"{field} must have minute precision")
    else:
        if not isinstance(value, str) or not _MINUTE_VALUE.fullmatch(value):
            raise TasksDataError(
                f"Invalid {field}: {value!r}; expected YYYY-MM-DDTHH:MM"
            )
        try:
            parsed = datetime.fromisoformat(value.replace(" ", "T"))
        except ValueError as exc:
            raise TasksDataError(f"Invalid {field}: {value!r}") from exc
    zone = _zone(timezone_name)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=zone)
    return parsed.astimezone(zone)


def _period(
    date_from: Union[str, datetime],
    date_to: Union[str, datetime],
    timezone_name: str,
) -> TaskPeriod:
    start = _minute(date_from, field="date/time from", timezone_name=timezone_name)
    end = _minute(date_to, field="date/time to", timezone_name=timezone_name)
    if end < start:
        raise TasksDataError("Date/time to must not be earlier than date/time from")
    return TaskPeriod(start=start, end=end)


def _totals(source: TaskSource, include) -> tuple[int, Decimal, Decimal]:
    tasks = 0
    labels = Decimal(0)
    codes = Decimal(0)

    for ref in source.list_objects():
        if not include(ref):
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
    return tasks, labels, codes


def collect(
    source: TaskSource,
    *,
    day: Union[str, date],
    timezone_name: str = "Europe/Moscow",
) -> TasksReportData:
    """Collect one local calendar day without performing presentation work."""

    target_day = _day(day)
    tasks, labels, codes = _totals(
        source,
        lambda ref: _local_date(ref.last_modified, timezone_name) == target_day,
    )

    return TasksReportData(
        day=target_day,
        tasks=tasks,
        labels=labels,
        codes=codes,
    )


def collect_range(
    source: TaskSource,
    *,
    date_from: Union[str, datetime],
    date_to: Union[str, datetime],
    timezone_name: str = "Europe/Moscow",
) -> TasksReportData:
    """Collect an inclusive local date/time range with minute precision."""

    period = _period(date_from, date_to, timezone_name)
    tasks, labels, codes = _totals(
        source,
        lambda ref: period.start
        <= _local_datetime(ref.last_modified, timezone_name)
        < period.end_exclusive,
    )
    return TasksReportData(
        day=None,
        tasks=tasks,
        labels=labels,
        codes=codes,
        period=period,
    )
