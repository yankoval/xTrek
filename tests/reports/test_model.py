import pytest

from xtrek.reports.model import Heading, Page, Report, Table


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
