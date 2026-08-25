"""Build the neutral document for the received-tasks report."""

from __future__ import annotations

from decimal import Decimal

from ..model import KeyValue, Page, Report
from .data import TasksReportData


REPORT_TITLE = "Отчёт о полученных заданиях"


def _number(value: Decimal) -> str:
    return f"{value:,.0f}".replace(",", " ")


def build(data: TasksReportData) -> Report:
    return Report(
        title=REPORT_TITLE,
        metadata={"report": "tasks", "date": data.day.isoformat()},
        pages=(
            Page(
                blocks=(
                    KeyValue("Дата", data.day.strftime("%d.%m.%Y")),
                    KeyValue("Заданий", str(data.tasks)),
                    KeyValue("Паспортов (ярлыков)", _number(data.labels)),
                    KeyValue("Кодов", _number(data.codes)),
                ),
                break_after=False,
            ),
        ),
    )
