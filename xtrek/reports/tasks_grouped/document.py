"""Build the neutral document for the article-grouped tasks report."""

from __future__ import annotations

from ..model import (
    Border,
    Heading,
    KeyValue,
    NumberFormat,
    Page,
    Report,
    Table,
    TableColumn,
    TableStyle,
)
from .data import TasksGroupedReportData


REPORT_TITLE = "Отчет о заданиях с группировкой по артикулам"


def build(data: TasksGroupedReportData, *, include_details: bool = True) -> Report:
    integer_format = NumberFormat(
        decimal_places=0,
        thousands_separator=" ",
        decimal_separator=",",
    )
    rows = tuple(
        (group.article, group.tasks, group.labels, group.codes)
        for group in data.groups
    ) + (("Итого", data.tasks, data.labels, data.codes),)
    if data.period is not None:
        metadata = {
            "report": "tasks-grouped",
            "date_from": data.period.start.isoformat(timespec="minutes"),
            "date_to": data.period.end.isoformat(timespec="minutes"),
        }
        selection = (
            KeyValue(
                "Дата и время от",
                data.period.start.strftime("%d.%m.%Y %H:%M"),
            ),
            KeyValue(
                "Дата и время до",
                data.period.end.strftime("%d.%m.%Y %H:%M"),
            ),
        )
        detail_time_title = "Дата и время"
        detail_time = lambda item: item.modified_at.strftime("%d.%m.%Y %H:%M")
        detail_time_width = "22%"
    else:
        if data.day is None:
            raise ValueError("Grouped tasks report must contain a day or a period")
        metadata = {"report": "tasks-grouped", "date": data.day.isoformat()}
        selection = (KeyValue("Дата", data.day.strftime("%d.%m.%Y")),)
        detail_time_title = "Время"
        detail_time = lambda item: item.time
        detail_time_width = "12%"
    pages = [
        Page(
            blocks=(
                Table(
                    columns=(
                        TableColumn("Артикул", align="left", width="36%"),
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
                    rows=rows,
                    style=TableStyle(
                        outer_border=Border(width=1.0, style="solid"),
                        row_border=Border(width=0.5, style="solid"),
                        column_border=Border(width=0.5, style="solid"),
                        header_border=Border(width=1.0, style="double"),
                        cell_padding=8,
                        repeat_header=True,
                        striped_rows=True,
                        messenger_layout="cards",
                    ),
                ),
            ),
            break_after=include_details,
        )
    ]
    if include_details:
        detail_blocks = [Heading("Расшифровка по файлам", level=2)]
        for group in data.groups:
            detail_blocks.extend(
                (
                    Heading(group.article, level=3),
                    Table(
                        columns=(
                            TableColumn(
                                detail_time_title,
                                align="left",
                                width=detail_time_width,
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
                            TableColumn("Файл", align="left", width="42%"),
                        ),
                        rows=tuple(
                            (detail_time(item), item.labels, item.codes, item.file)
                            for item in group.files
                        ),
                        style=TableStyle(
                            outer_border=Border(width=1.0, style="solid"),
                            row_border=Border(width=0.5, style="solid"),
                            column_border=Border(width=0.5, style="solid"),
                            header_border=Border(width=1.0, style="double"),
                            cell_padding=6,
                            repeat_header=True,
                            striped_rows=True,
                            messenger_layout="cards",
                        ),
                    ),
                )
            )
        pages.append(Page(blocks=tuple(detail_blocks), break_after=False))

    return Report(
        title=REPORT_TITLE,
        metadata=metadata,
        header=(
            KeyValue(
                "Дата и время создания отчёта",
                data.generated_at.strftime("%d.%m.%Y %H:%M:%S"),
                small=True,
            ),
        ) + selection,
        pages=tuple(pages),
    )
