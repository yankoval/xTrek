"""Collect task totals with zero to two ordered article/operator groupings."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import PurePosixPath
from typing import Callable, Mapping, Optional, Sequence, Union
from zoneinfo import ZoneInfo

from ..tasks.data import (
    TaskPeriod,
    TasksDataError,
    _day,
    _decimal,
    _local_datetime,
    _period,
)
from ..tasks.source import TaskObjectRef, TaskSource


MISSING_ARTICLE = "Без артикула"
MISSING_OPERATOR = "Без оператора"
Grouping = Optional[Union[str, Sequence[str]]]


def normalize_group_by(group_by: Grouping) -> tuple[str, ...]:
    """Validate an ordered list of zero, one or two distinct grouping fields."""
    if group_by is None:
        fields = ()
    elif isinstance(group_by, str):
        fields = tuple(group_by.split())
    elif isinstance(group_by, (list, tuple)):
        fields = tuple(group_by)
    else:
        raise TasksDataError("group_by must be an ordered list of article/operator fields")
    if len(fields) > 2:
        raise TasksDataError("group_by: допускается не более двух уровней группировки")
    if any(field not in ("article", "operator") for field in fields):
        raise TasksDataError("group_by must be article or operator (допустимы только article и operator)")
    if len(set(fields)) != len(fields):
        raise TasksDataError("group_by: поля группировки не должны повторяться")
    return fields


@dataclass(frozen=True)
class TaskFileData:
    modified_at: datetime
    file: str
    labels: Decimal
    codes: Decimal

    @property
    def time(self) -> str:
        return self.modified_at.strftime("%H:%M")


class _GroupTotals:
    files: tuple[TaskFileData, ...]

    @property
    def tasks(self) -> int:
        return len(self.files)

    @property
    def labels(self) -> Decimal:
        return sum((item.labels for item in self.files), Decimal(0))

    @property
    def codes(self) -> Decimal:
        return sum((item.codes for item in self.files), Decimal(0))


@dataclass(frozen=True)
class ArticleTasksData(_GroupTotals):
    article: str
    files: tuple[TaskFileData, ...]
    groups: tuple[Union[ArticleTasksData, OperatorTasksData], ...] = ()

    @property
    def value(self) -> str:
        return self.article


@dataclass(frozen=True)
class OperatorTasksData(_GroupTotals):
    operator: str
    files: tuple[TaskFileData, ...]
    groups: tuple[Union[ArticleTasksData, OperatorTasksData], ...] = ()

    @property
    def value(self) -> str:
        return self.operator


@dataclass(frozen=True)
class TasksGroupedReportData:
    day: Optional[date]
    generated_at: datetime
    groups: tuple[Union[ArticleTasksData, OperatorTasksData], ...]
    period: Optional[TaskPeriod] = None
    group_by: tuple[str, ...] = ()
    files: tuple[TaskFileData, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "group_by", normalize_group_by(self.group_by))

    @property
    def tasks(self) -> int:
        return sum(group.tasks for group in self.groups) if self.groups else len(self.files)

    @property
    def labels(self) -> Decimal:
        items = self.groups or self.files
        return sum((item.labels for item in items), Decimal(0))

    @property
    def codes(self) -> Decimal:
        items = self.groups or self.files
        return sum((item.codes for item in items), Decimal(0))


def _generated_at(value: Optional[datetime], timezone_name: str) -> datetime:
    try:
        zone = ZoneInfo(timezone_name)
    except Exception as exc:
        raise TasksDataError(f"Unknown timezone: {timezone_name}") from exc
    if value is None:
        value = datetime.now(timezone.utc)
    elif value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(zone)


def _make_groups(records, fields: tuple[str, ...], depth: int = 0):
    if depth == len(fields):
        return ()
    buckets = defaultdict(list)
    for values, item in records:
        buckets[values[depth]].append((values, item))
    group_type = OperatorTasksData if fields[depth] == "operator" else ArticleTasksData
    return tuple(
        group_type(
            value,
            files=tuple(item for _, item in entries),
            groups=_make_groups(entries, fields, depth + 1),
        )
        for value, entries in sorted(buckets.items(), key=lambda item: (item[0].casefold(), item[0]))
    )


def _collect(
    source: TaskSource,
    *,
    include: Callable[[TaskObjectRef], bool],
    timezone_name: str,
    day: Optional[date],
    period: Optional[TaskPeriod],
    generated_at: Optional[datetime],
    group_by: Grouping,
) -> TasksGroupedReportData:
    fields = normalize_group_by(group_by)
    records = []

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
        if quantity == 0:
            continue
        pack_quantity = _decimal(
            passport.get("Product_PackQty"),
            field="PasportData.Product_PackQty",
            ref=ref,
        )
        article = str(payload.get("Article") or "").strip() or MISSING_ARTICLE
        values = {"article": article}
        if "operator" in fields:
            operator = passport.get("operator")
            if operator is not None and not isinstance(operator, str):
                raise TasksDataError(f"PasportData.operator must be a string in task {ref.key}")
            values["operator"] = (operator or "").strip() or MISSING_OPERATOR
        records.append((
            tuple(values[field] for field in fields),
            TaskFileData(
                modified_at=_local_datetime(ref.last_modified, timezone_name),
                file=PurePosixPath(ref.key).name,
                labels=quantity,
                codes=quantity * pack_quantity,
            )
        ))

    records.sort(key=lambda record: (record[1].modified_at, record[1].file))
    return TasksGroupedReportData(
        day=day,
        generated_at=_generated_at(generated_at, timezone_name),
        groups=_make_groups(records, fields),
        period=period,
        group_by=fields,
        files=tuple(item for _, item in records),
    )


def collect(
    source: TaskSource,
    *,
    day: Union[str, date],
    timezone_name: str = "Europe/Moscow",
    generated_at: Optional[datetime] = None,
    group_by: Grouping = None,
) -> TasksGroupedReportData:
    """Collect a local day's totals with optional ordered grouping fields."""

    target_day = _day(day)
    return _collect(
        source,
        day=target_day,
        period=None,
        generated_at=generated_at,
        group_by=group_by,
        timezone_name=timezone_name,
        include=lambda ref: _local_datetime(ref.last_modified, timezone_name).date()
        == target_day,
    )


def collect_range(
    source: TaskSource,
    *,
    date_from: Union[str, datetime],
    date_to: Union[str, datetime],
    timezone_name: str = "Europe/Moscow",
    generated_at: Optional[datetime] = None,
    group_by: Grouping = None,
) -> TasksGroupedReportData:
    """Collect an inclusive minute range with optional ordered grouping fields."""

    period = _period(date_from, date_to, timezone_name)
    return _collect(
        source,
        day=None,
        period=period,
        generated_at=generated_at,
        group_by=group_by,
        timezone_name=timezone_name,
        include=lambda ref: period.start
        <= _local_datetime(ref.last_modified, timezone_name)
        < period.end_exclusive,
    )
