"""Collect daily task totals grouped by article."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import PurePosixPath
from typing import Callable, DefaultDict, List, Mapping, Optional, Union
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


@dataclass(frozen=True)
class TaskFileData:
    modified_at: datetime
    file: str
    labels: Decimal
    codes: Decimal

    @property
    def time(self) -> str:
        return self.modified_at.strftime("%H:%M")


@dataclass(frozen=True)
class ArticleTasksData:
    article: str
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
class TasksGroupedReportData:
    day: Optional[date]
    generated_at: datetime
    groups: tuple[ArticleTasksData, ...]
    period: Optional[TaskPeriod] = None

    @property
    def tasks(self) -> int:
        return sum(group.tasks for group in self.groups)

    @property
    def labels(self) -> Decimal:
        return sum((group.labels for group in self.groups), Decimal(0))

    @property
    def codes(self) -> Decimal:
        return sum((group.codes for group in self.groups), Decimal(0))


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


def _collect(
    source: TaskSource,
    *,
    include: Callable[[TaskObjectRef], bool],
    timezone_name: str,
    day: Optional[date],
    period: Optional[TaskPeriod],
    generated_at: Optional[datetime],
) -> TasksGroupedReportData:
    grouped: DefaultDict[str, List[TaskFileData]] = defaultdict(list)

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
        article = str(payload.get("Article") or "").strip() or MISSING_ARTICLE
        grouped[article].append(
            TaskFileData(
                modified_at=_local_datetime(ref.last_modified, timezone_name),
                file=PurePosixPath(ref.key).name,
                labels=quantity,
                codes=quantity * pack_quantity,
            )
        )

    groups = tuple(
        ArticleTasksData(
            article=article,
            files=tuple(
                sorted(files, key=lambda item: (item.modified_at, item.file))
            ),
        )
        for article, files in sorted(grouped.items(), key=lambda item: item[0].casefold())
    )
    return TasksGroupedReportData(
        day=day,
        generated_at=_generated_at(generated_at, timezone_name),
        groups=groups,
        period=period,
    )


def collect(
    source: TaskSource,
    *,
    day: Union[str, date],
    timezone_name: str = "Europe/Moscow",
    generated_at: Optional[datetime] = None,
) -> TasksGroupedReportData:
    """Collect one local calendar day and group task totals by ``Article``."""

    target_day = _day(day)
    return _collect(
        source,
        day=target_day,
        period=None,
        generated_at=generated_at,
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
) -> TasksGroupedReportData:
    """Collect an inclusive minute range and group totals by ``Article``."""

    period = _period(date_from, date_to, timezone_name)
    return _collect(
        source,
        day=None,
        period=period,
        generated_at=generated_at,
        timezone_name=timezone_name,
        include=lambda ref: period.start
        <= _local_datetime(ref.last_modified, timezone_name)
        < period.end_exclusive,
    )
