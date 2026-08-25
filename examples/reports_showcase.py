"""Generate HTML and Markdown examples for every report profile."""

from decimal import Decimal
from pathlib import Path

from xtrek.reports import (
    Border,
    Heading,
    KeyValue,
    Link,
    NumberFormat,
    Page,
    Paragraph,
    Report,
    Table,
    TableColumn,
    TableStyle,
    render_to_file,
)


INTEGER = NumberFormat(decimal_places=0, thousands_separator=" ")
MONEY = NumberFormat(decimal_places=2, suffix=" ₽")
PERCENT = NumberFormat(decimal_places=1, suffix=" %")


def build_showcase() -> Report:
    decorated = TableStyle(
        outer_border=Border(2, "double", "#34495e"),
        row_border=Border(0.7, "dashed", "#94a3b8"),
        column_border=Border(0.7, "solid", "#cbd5e1"),
        header_border=Border(1.5, "double", "#2563eb"),
        cell_padding=9,
        striped_rows=True,
        messenger_layout="cards",
    )
    summary = TableStyle(
        outer_border=Border(1.5, "solid", "#16a34a"),
        row_border=Border(0.8, "dotted", "#86efac"),
        column_border=Border(0.8, "dotted", "#86efac"),
        header_border=Border(1, "solid", "#16a34a"),
        cell_padding=10,
        messenger_layout="key_value",
    )
    return Report(
        title="Тест возможностей генератора отчётов",
        header=(
            Paragraph("Демонстрация таблиц, форматов чисел, ссылок и страниц."),
            KeyValue("Дата формирования", "25.08.2026"),
            KeyValue(
                "Документация",
                Link("Открыть описание xTrek", "https://github.com/yankoval/xTrek"),
            ),
        ),
        pages=(
            Page(
                header=(Heading("Детализация заданий", 2),),
                blocks=(
                    Table(
                        caption="Производственные задания",
                        columns=(
                            TableColumn("Задание", align="left", width="27%"),
                            TableColumn("Статус", align="center", width="14%"),
                            TableColumn(
                                "Количество", align="right", number_format=INTEGER
                            ),
                            TableColumn("Цена", align="decimal", number_format=MONEY),
                            TableColumn(
                                "Выполнение", align="right", number_format=PERCENT
                            ),
                            TableColumn("Документ", align="left", width="16%"),
                        ),
                        rows=(
                            (
                                "T-GB-8436-77-001",
                                "Готово",
                                7420,
                                Decimal("18.5"),
                                Decimal("100"),
                                Link("Открыть", "https://example.com/tasks/8436"),
                            ),
                            (
                                "T-7909-77-001",
                                "В работе",
                                1640,
                                Decimal("1234.567"),
                                Decimal("62.45"),
                                Link("Открыть", "https://example.com/tasks/7909"),
                            ),
                            (
                                "T-4826-77-002",
                                "Ожидает",
                                0,
                                Decimal(0),
                                Decimal(0),
                                Link("Открыть", "https://example.com/tasks/4826"),
                            ),
                        ),
                        style=decorated,
                    ),
                ),
                footer=(Paragraph("Конец детальной таблицы"),),
            ),
            Page(
                header=(Heading("Сводные показатели", 2),),
                blocks=(
                    Table(
                        columns=(
                            TableColumn("Показатель", align="left", width="55%"),
                            TableColumn(
                                "Значение", align="right", number_format=INTEGER
                            ),
                        ),
                        rows=(
                            ("Всего заданий", 57),
                            ("Паспортов (ярлыков)", 2505),
                            ("Кодов", 16760),
                        ),
                        style=summary,
                    ),
                ),
                break_after=False,
            ),
        ),
        footer=(
            KeyValue(
                "Поддержка",
                Link("Открыть портал", "https://example.com/support"),
            ),
        ),
    )


def main() -> None:
    output_dir = Path("outputs/report-style-showcase")
    report = build_showcase()
    for output_format, extension in (("html", "html"), ("md", "md")):
        for profile in ("browser", "printer", "messenger"):
            render_to_file(
                report,
                output_dir / f"showcase-{profile}.{extension}",
                output_format=output_format,
                profile=profile,
            )
    print(output_dir.resolve())


if __name__ == "__main__":
    main()
