import pytest

from xtrek.reports.model import (
    Border,
    Heading,
    Link,
    NumberFormat,
    Page,
    Report,
    Table,
    TableColumn,
)


def test_report_requires_a_page():
    with pytest.raises(ValueError, match="at least one page"):
        Report(title="Empty", pages=())


def test_heading_level_is_validated():
    with pytest.raises(ValueError, match="between 1 and 6"):
        Heading("Invalid", level=7)


def test_table_rows_match_headers():
    with pytest.raises(ValueError, match="match the number"):
        Table(headers=("One", "Two"), rows=(("only one",),))


def test_model_supports_page_header_and_footer():
    page = Page(
        header=(Heading("Page", level=2),),
        blocks=(Heading("Body", level=3),),
        footer=(Heading("Footer", level=4),),
    )
    report = Report(title="Structured", pages=(page,))

    assert report.pages[0].header[0].text == "Page"
    assert report.pages[0].footer[0].text == "Footer"


def test_table_columns_keep_alignment_and_number_format():
    number_format = NumberFormat(decimal_places=2, suffix=" ₽")
    table = Table(
        columns=(
            TableColumn("Название", align="left"),
            TableColumn("Сумма", align="right", number_format=number_format),
        ),
        rows=(("Товар", 1234.5),),
    )

    assert table.headers == ("Название", "Сумма")
    assert table.columns[1].align == "right"
    assert table.columns[1].number_format == number_format


def test_table_headers_remain_backward_compatible():
    table = Table(headers=("One", "Two"), rows=(("a", "b"),))

    assert tuple(column.title for column in table.columns) == ("One", "Two")


@pytest.mark.parametrize("style", ["groove", "inset", "unknown"])
def test_border_rejects_unsupported_style(style):
    with pytest.raises(ValueError, match="Unsupported border style"):
        Border(style=style)


def test_link_rejects_executable_scheme():
    with pytest.raises(ValueError, match="Unsupported link scheme"):
        Link("Опасная ссылка", "javascript:alert(1)")
