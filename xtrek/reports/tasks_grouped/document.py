"""Build the neutral document for grouped task reports."""

from __future__ import annotations

from dataclasses import replace

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
from ..tasks.data import TasksReportData
from ..tasks.document import build as build_totals


REPORT_TITLE = "Отчет о заданиях с группировкой по артикулам"
OPERATOR_REPORT_TITLE = "Отчёт о заданиях с группировкой по операторам"
FIELD_TITLES = {"operator": "Оператор", "article": "Артикул"}
SUBTOTAL_TITLES = {"operator": "Итого по оператору", "article": "Итого по артикулу"}


def build(data: TasksGroupedReportData, *, include_details: bool = True) -> Report:
    if not data.group_by:
        # With no grouping the report is strictly totals-only, even with details=full.
        document = build_totals(TasksReportData(
            day=data.day, tasks=data.tasks, labels=data.labels,
            codes=data.codes, period=data.period,
        ))
        return replace(document, metadata={
            **document.metadata, "report": "tasks-grouped", "group_by": "",
        })
    first_field = data.group_by[0]
    integer_format = NumberFormat(
        decimal_places=0,
        thousands_separator=" ",
        decimal_separator=",",
    )
    rows = tuple(
        (group.value, group.tasks, group.labels, group.codes)
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
    metadata["group_by"] = " ".join(data.group_by)
    pages = [
        Page(
            blocks=(
                Table(
                    columns=(
                        TableColumn(FIELD_TITLES[first_field], align="left", width="36%"),
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
    if len(data.group_by) == 2:
        summary_template = pages[0].blocks[0]
        summary_template = replace(
            summary_template,
            columns=(TableColumn(FIELD_TITLES[data.group_by[1]], align="left", width="36%"),)
                    + summary_template.columns[1:],
        )
        summary_blocks = []
        for group in data.groups:
            child_rows = tuple(
                (child.value, child.tasks, child.labels, child.codes)
                for child in group.groups
            )
            # Subtotals are mandatory, independently of file-detail visibility.
            subtotal = (f"{SUBTOTAL_TITLES[first_field]}: {group.value}",
                        group.tasks, group.labels, group.codes)
            summary_blocks.extend((
                Heading(f"{FIELD_TITLES[first_field]}: {group.value}", level=2),
                replace(summary_template, rows=child_rows + (subtotal,)),
            ))
        summary_blocks.append(replace(
            summary_template,
            columns=(TableColumn("Общий итог", align="left", width="36%"),)
                    + summary_template.columns[1:],
            rows=(("Итого", data.tasks, data.labels, data.codes),),
        ))
        pages[0] = replace(pages[0], blocks=tuple(summary_blocks))

    if include_details:
        detail_blocks = [Heading("Расшифровка по файлам", level=2)]
        detail_groups = []
        for group in data.groups:
            if len(data.group_by) == 2:
                for index, child in enumerate(group.groups):
                    headings = ([Heading(f"{FIELD_TITLES[first_field]}: {group.value}", level=3)]
                                if index == 0 else [])
                    headings.append(Heading(child.value, level=4))
                    detail_groups.append((headings, child))
            else:
                detail_groups.append(([Heading(group.value, level=3)], group))
        for headings, group in detail_groups:
            detail_blocks.extend(headings)
            detail_blocks.extend(
                (
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
        title=(
            "Отчёт о заданиях: " + " → ".join(FIELD_TITLES[field].lower() for field in data.group_by)
            if len(data.group_by) == 2
            else OPERATOR_REPORT_TITLE if first_field == "operator" else REPORT_TITLE
        ),
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
