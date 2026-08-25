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
    return Report(
        title=REPORT_TITLE,
        metadata={"report": "tasks", "date": data.day.isoformat()},
        pages=(
            Page(
                blocks=(
                    Table(
                        columns=(
                            TableColumn("Дата", align="left", width="22%"),
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
                                data.day.strftime("%d.%m.%Y"),
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
                            messenger_layout="table",
                        ),
                    ),
                ),
                break_after=False,
            ),
        ),
    )
