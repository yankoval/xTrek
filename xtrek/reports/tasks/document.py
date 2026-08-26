"""Build the neutral document for the received-tasks report."""

from __future__ import annotations

from ..model import (
    Border,
    NumberFormat,
    Page,
    Report,
    Table,
    TableColumn,
    TableStyle,
)
from .data import TasksReportData


REPORT_TITLE = "Отчёт о полученных заданиях"


def build(data: TasksReportData) -> Report:
    integer_format = NumberFormat(
        decimal_places=0,
        thousands_separator=" ",
        decimal_separator=",",
    )
    if data.period is not None:
        first_column = TableColumn("Период", align="left", width="38%")
        period_value = (
            f"{data.period.start.strftime('%d.%m.%Y %H:%M')} — "
            f"{data.period.end.strftime('%d.%m.%Y %H:%M')}"
        )
        metadata = {
            "report": "tasks",
            "date_from": data.period.start.isoformat(timespec="minutes"),
            "date_to": data.period.end.isoformat(timespec="minutes"),
        }
    else:
        if data.day is None:
            raise ValueError("Tasks report must contain a day or a period")
        first_column = TableColumn("Дата", align="left", width="22%")
        period_value = data.day.strftime("%d.%m.%Y")
        metadata = {"report": "tasks", "date": data.day.isoformat()}

    return Report(
        title=REPORT_TITLE,
        metadata=metadata,
        pages=(
            Page(
                blocks=(
                    Table(
                        columns=(
                            first_column,
                            TableColumn(
                                "Заданий",
                                align="right",
                                number_format=integer_format,
                            ),
                            TableColumn(
                                "Паспортов (ярлыков)",
                                align="right",
                                number_format=integer_format,
                            ),
                            TableColumn(
                                "Кодов",
                                align="right",
                                number_format=integer_format,
                            ),
                        ),
                        rows=(
                            (
                                period_value,
                                data.tasks,
                                data.labels,
                                data.codes,
                            ),
                        ),
                        style=TableStyle(
                            outer_border=Border(width=1.0, style="solid"),
                            row_border=Border(width=0.5, style="solid"),
                            column_border=Border(width=0.5, style="solid"),
                            header_border=Border(width=1.0, style="double"),
                            cell_padding=8,
                            repeat_header=True,
                            striped_rows=True,
                            messenger_layout="vertical_table",
                        ),
                    ),
                ),
                break_after=False,
            ),
        ),
    )
