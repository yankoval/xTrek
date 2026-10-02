"""Build the neutral document for finished equipment reports."""

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
from .data import EquipmentReportData


REPORT_TITLE = "Отчёт оборудования по операторам и артикулам"


def _summary_table(rows):
    integer_format = NumberFormat(
        decimal_places=0,
        thousands_separator=" ",
        decimal_separator=",",
    )
    return Table(
        columns=(
            TableColumn("Артикул", align="left", width="34%"),
            TableColumn("Отчётов", align="right", number_format=integer_format),
            TableColumn("Коробов", align="right", number_format=integer_format),
            TableColumn("Кодов", align="right", number_format=integer_format),
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
    )


def build(data: EquipmentReportData, *, include_details: bool = True) -> Report:
    if data.period is not None:
        metadata = {
            "report": "equipment-grouped",
            "date_from": data.period.start.isoformat(timespec="minutes"),
            "date_to": data.period.end.isoformat(timespec="minutes"),
        }
        selection = (
            KeyValue("Дата и время от", data.period.start.strftime("%d.%m.%Y %H:%M")),
            KeyValue("Дата и время до", data.period.end.strftime("%d.%m.%Y %H:%M")),
        )
    else:
        if data.day is None:
            raise ValueError("Equipment report must contain a day or a period")
        metadata = {"report": "equipment-grouped", "date": data.day.isoformat()}
        selection = (KeyValue("Дата", data.day.strftime("%d.%m.%Y")),)

    blocks = []
    for operator in data.operators:
        rows = tuple(
            (article.article, article.reports, article.boxes, article.codes)
            for article in operator.articles
        ) + ((
            f"Итого по оператору: {operator.operator}",
            operator.reports,
            operator.boxes,
            operator.codes,
        ),)
        blocks.extend((Heading(operator.operator, level=2), _summary_table(rows)))
    blocks.append(
        _summary_table(
            (("Общий итог", data.reports, data.boxes, data.codes),)
        )
    )

    pages = [Page(blocks=tuple(blocks), break_after=include_details)]
    if include_details:
        integer_format = NumberFormat(
            decimal_places=0,
            thousands_separator=" ",
            decimal_separator=",",
        )
        detail_rows = tuple(
            (
                item.ended_at.strftime("%d.%m.%Y %H:%M"),
                item.operator,
                item.article,
                item.boxes,
                item.codes,
                item.report_id,
            )
            for item in data.details
        )
        pages.append(
            Page(
                blocks=(
                    Heading("Расшифровка по отчётам", level=2),
                    Table(
                        columns=(
                            TableColumn("Окончание", align="left", width="17%"),
                            TableColumn("Оператор", align="left", width="18%"),
                            TableColumn("Артикул", align="left", width="18%"),
                            TableColumn("Коробов", align="right", number_format=integer_format),
                            TableColumn("Кодов", align="right", number_format=integer_format),
                            TableColumn("ID отчёта", align="left", width="25%"),
                        ),
                        rows=detail_rows,
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
                ),
                break_after=False,
            )
        )

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
