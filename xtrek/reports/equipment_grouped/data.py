"""Collect finished equipment output grouped by operator and article."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from pathlib import PurePosixPath
from typing import Mapping, Optional, Union
from zoneinfo import ZoneInfo

from ..tasks.data import (
    TaskPeriod,
    TasksDataError,
    _day,
    _local_datetime,
    _period,
)
from .source import EquipmentReportObjectRef, EquipmentReportSource


MISSING_ARTICLE = "Без артикула"
MISSING_OPERATOR = "Без оператора"


@dataclass(frozen=True)
class EquipmentReportDetail:
    ended_at: datetime
    report_id: str
    article: str
    operator: str
    boxes: int
    codes: int


@dataclass(frozen=True)
class EquipmentArticleData:
    article: str
    reports: int
    boxes: int
    codes: int
    details: tuple[EquipmentReportDetail, ...]


@dataclass(frozen=True)
class EquipmentOperatorData:
    operator: str
    reports: int
    boxes: int
    codes: int
    articles: tuple[EquipmentArticleData, ...]


@dataclass(frozen=True)
class EquipmentReportData:
    day: Optional[date]
    generated_at: datetime
    operators: tuple[EquipmentOperatorData, ...]
    period: Optional[TaskPeriod] = None
    details: tuple[EquipmentReportDetail, ...] = ()

    @property
    def reports(self) -> int:
        return len(self.details)

    @property
    def boxes(self) -> int:
        return sum(item.boxes for item in self.details)

    @property
    def codes(self) -> int:
        return sum(item.codes for item in self.details)


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


def _report_id(ref: EquipmentReportObjectRef, payload: Mapping[str, Any]) -> str:
    key_id = PurePosixPath(ref.key).stem
    payload_id = payload.get("id")
    if payload_id is not None and str(payload_id).strip() != key_id:
        raise TasksDataError(
            f"Equipment report id {payload_id!r} does not match object name {key_id!r}"
        )
    return key_id


def _report_datetime(
    payload: Mapping[str, Any],
    fallback: datetime,
    timezone_name: str,
    report_id: str,
) -> datetime:
    value = payload.get("endTime")
    if value in (None, ""):
        return _local_datetime(fallback, timezone_name)
    if not isinstance(value, str):
        raise TasksDataError(f"endTime must be an ISO date/time in report {report_id}")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return _local_datetime(parsed, timezone_name)
    except ValueError as exc:
        raise TasksDataError(f"Invalid endTime in equipment report {report_id}: {value!r}") from exc


def _count_box_codes(boxes: list[Any], report_id: str) -> int:
    codes = 0
    for box in boxes:
        if not isinstance(box, Mapping):
            raise TasksDataError(f"Invalid box in equipment report {report_id}")
        full_codes = box.get("productNumbersFull")
        short_codes = box.get("productNumbers")
        if isinstance(full_codes, list):
            codes += len(full_codes)
        elif isinstance(short_codes, list):
            codes += len(short_codes)
    return codes


def _count_output(payload: Mapping[str, Any], report_id: str) -> tuple[int, int]:
    if "readyPallet" not in payload:
        # Legacy equipment reports store boxes in one flat readyBox array and
        # do not record which pallet each box belongs to.
        boxes = payload.get("readyBox") or []
        if not isinstance(boxes, list):
            raise TasksDataError(f"readyBox must be an array in report {report_id}")
        return len(boxes), _count_box_codes(boxes, report_id)

    pallets = payload.get("readyPallet") or []
    if not isinstance(pallets, list):
        raise TasksDataError(f"readyPallet must be an array in report {report_id}")
    boxes = 0
    codes = 0
    for pallet in pallets:
        if not isinstance(pallet, Mapping):
            raise TasksDataError(f"Invalid pallet in equipment report {report_id}")
        pallet_boxes = pallet.get("readyBox") or []
        if not isinstance(pallet_boxes, list):
            raise TasksDataError(f"readyBox must be an array in report {report_id}")
        boxes += len(pallet_boxes)
        codes += _count_box_codes(pallet_boxes, report_id)
    return boxes, codes


def _aggregate(details: list[EquipmentReportDetail]) -> tuple[EquipmentOperatorData, ...]:
    by_operator = defaultdict(list)
    for item in details:
        by_operator[item.operator].append(item)
    operators = []
    for operator, operator_details in sorted(
        by_operator.items(), key=lambda item: (item[0].casefold(), item[0])
    ):
        by_article = defaultdict(list)
        for item in operator_details:
            by_article[item.article].append(item)
        articles = tuple(
            EquipmentArticleData(
                article=article,
                reports=len(items),
                boxes=sum(item.boxes for item in items),
                codes=sum(item.codes for item in items),
                details=tuple(items),
            )
            for article, items in sorted(
                by_article.items(), key=lambda item: (item[0].casefold(), item[0])
            )
        )
        operators.append(
            EquipmentOperatorData(
                operator=operator,
                reports=len(operator_details),
                boxes=sum(item.boxes for item in operator_details),
                codes=sum(item.codes for item in operator_details),
                articles=articles,
            )
        )
    return tuple(operators)


def _collect(
    source: EquipmentReportSource,
    *,
    date_from: datetime,
    date_to_exclusive: datetime,
    timezone_name: str,
    day: Optional[date],
    period: Optional[TaskPeriod],
    generated_at: Optional[datetime],
) -> EquipmentReportData:
    details = []
    for ref in source.list_finished_reports(
        date_from=date_from,
        date_to_exclusive=date_to_exclusive,
        timezone_name=timezone_name,
    ):
        try:
            payload = source.read_report(ref)
            report_id = _report_id(ref, payload)
            ended_at = _report_datetime(
                payload,
                ref.last_modified,
                timezone_name,
                report_id,
            )
            order = source.read_production_order(report_id)
        except TasksDataError:
            raise
        except (OSError, ValueError, KeyError, TypeError) as exc:
            raise TasksDataError(
                f"Cannot read equipment report or production order for {ref.key}: {exc}"
            ) from exc

        passport = order.get("PasportData") or {}
        if not isinstance(passport, Mapping):
            raise TasksDataError(
                f"PasportData must be an object in production order {report_id}"
            )
        operator = passport.get("operator")
        if operator is not None and not isinstance(operator, str):
            raise TasksDataError(
                f"PasportData.operator must be a string in production order {report_id}"
            )
        operator_name = (operator or "").strip() or MISSING_OPERATOR

        article = str(order.get("Article") or "").strip() or MISSING_ARTICLE
        boxes, codes = _count_output(payload, report_id)
        details.append(
            EquipmentReportDetail(
                ended_at=ended_at,
                report_id=report_id,
                article=article,
                operator=operator_name,
                boxes=boxes,
                codes=codes,
            )
        )

    details.sort(key=lambda item: (item.ended_at, item.report_id))
    return EquipmentReportData(
        day=day,
        generated_at=_generated_at(generated_at, timezone_name),
        operators=_aggregate(details),
        period=period,
        details=tuple(details),
    )


def collect(
    source: EquipmentReportSource,
    *,
    day: Union[str, date],
    timezone_name: str = "Europe/Moscow",
    generated_at: Optional[datetime] = None,
) -> EquipmentReportData:
    target_day = _day(day)
    try:
        zone = ZoneInfo(timezone_name)
    except Exception as exc:
        raise TasksDataError(f"Unknown timezone: {timezone_name}") from exc
    date_from = datetime.combine(target_day, time.min, tzinfo=zone)
    return _collect(
        source,
        date_from=date_from,
        date_to_exclusive=date_from + timedelta(days=1),
        timezone_name=timezone_name,
        day=target_day,
        period=None,
        generated_at=generated_at,
    )


def collect_range(
    source: EquipmentReportSource,
    *,
    date_from: Union[str, datetime],
    date_to: Union[str, datetime],
    timezone_name: str = "Europe/Moscow",
    generated_at: Optional[datetime] = None,
) -> EquipmentReportData:
    period = _period(date_from, date_to, timezone_name)
    return _collect(
        source,
        date_from=period.start,
        date_to_exclusive=period.end_exclusive,
        timezone_name=timezone_name,
        day=None,
        period=period,
        generated_at=generated_at,
    )
