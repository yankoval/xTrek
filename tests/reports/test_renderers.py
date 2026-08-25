import sys
from decimal import Decimal

import pytest

from xtrek.reports import (
    Border,
    KeyValue,
    Link,
    NumberFormat,
    Page,
    Report,
    Table,
    TableColumn,
    TableStyle,
    render,
)
from xtrek.reports.errors import OptionalDependencyError


@pytest.fixture
def sample_report():
    return Report(
        title="Тестовый отчёт",
        pages=(
            Page(
                blocks=(
                    KeyValue("Дата", "25.08.2026"),
                    Table(
                        headers=("Название", "Количество"),
                        rows=(("Задание <1>", "10"),),
                    ),
                ),
                break_after=False,
            ),
        ),
    )


@pytest.fixture
def styled_table_report():
    return Report(
        title="Таблица",
        pages=(
            Page(
                blocks=(
                    Table(
                        columns=(
                            TableColumn("Документ", align="left"),
                            TableColumn(
                                "Сумма",
                                align="right",
                                number_format=NumberFormat(
                                    decimal_places=2,
                                    thousands_separator=" ",
                                    decimal_separator=",",
                                    suffix=" ₽",
                                ),
                            ),
                        ),
                        rows=(
                            (
                                Link(
                                    "Открыть",
                                    "https://example.com/task?id=1&source=report",
                                    title="Карточка задания",
                                ),
                                Decimal("1234.5"),
                            ),
                        ),
                        style=TableStyle(
                            outer_border=Border(
                                width=2,
                                style="dashed",
                                color="#123456",
                            ),
                            striped_rows=True,
                        ),
                    ),
                ),
            ),
        ),
        footer=(KeyValue("Поддержка", Link("Открыть", "https://example.com")),),
    )


def test_browser_html_is_a_document_with_embedded_style(sample_report):
    result = render(sample_report, output_format="html", profile="browser")

    assert result.startswith("<!doctype html>")
    assert "profile-browser" in result
    assert "Тестовый отчёт" in result
    assert "Задание &lt;1&gt;" in result
    assert "box-shadow" in result


def test_printer_html_contains_paged_css(sample_report):
    result = render(sample_report, output_format="html", profile="printer")

    assert "@page { size: A4 portrait" in result
    assert "profile-printer" in result


def test_messenger_html_is_a_compact_fragment(sample_report):
    result = render(sample_report, output_format="html", profile="messenger")

    assert result.startswith("<b>Тестовый отчёт</b>")
    assert "<b>Дата:</b> 25.08.2026" in result
    assert "<!doctype" not in result
    assert "<style>" not in result


def test_markdown_renderer_supports_key_values_and_tables(sample_report):
    result = render(sample_report, output_format="md", profile="messenger")

    assert result.startswith("**Тестовый отчёт**")
    assert "**Дата:** 25\\.08\\.2026" in result
    assert "| Название | Количество |" in result
    assert "\n\n" not in result


def test_pdf_dependency_is_loaded_only_when_requested(sample_report, monkeypatch):
    monkeypatch.setitem(sys.modules, "weasyprint", None)

    with pytest.raises(OptionalDependencyError, match="reports-pdf"):
        render(sample_report, output_format="pdf", profile="printer")


def test_pdf_renderer_returns_weasyprint_bytes(sample_report, monkeypatch):
    class FakeHTML:
        def __init__(self, *, string):
            assert "profile-printer" in string

        def write_pdf(self):
            return b"%PDF-fake"

    fake_module = type("FakeWeasyPrint", (), {"HTML": FakeHTML})
    monkeypatch.setitem(sys.modules, "weasyprint", fake_module)

    result = render(sample_report, output_format="pdf", profile="printer")

    assert result == b"%PDF-fake"


def test_unknown_theme_is_rejected(sample_report):
    with pytest.raises(ValueError, match="Unknown report profile or theme"):
        render(sample_report, output_format="html", theme="missing")


def test_html_table_applies_borders_alignment_numbers_and_links(styled_table_report):
    result = render(styled_table_report, output_format="html", profile="browser")

    assert "--outer-border: 2pt dashed #123456" in result
    assert "text-align: right" in result
    assert "1 234,50 ₽" in result
    assert "https://example.com/task?id=1&amp;source=report" in result
    assert 'title="Карточка задания"' in result
    assert 'class="striped repeat-header"' in result
    assert '<th style="text-align: right; vertical-align: middle">Сумма</th>' in result


def test_markdown_table_applies_alignment_numbers_and_links(styled_table_report):
    result = render(styled_table_report, output_format="md", profile="browser")

    assert "| :--- | ---: |" in result
    assert "1 234,50 ₽" in result
    assert "[Открыть](https://example.com/task?id=1&source=report)" in result


def test_messenger_table_ends_with_break_before_footer(styled_table_report):
    result = render(styled_table_report, output_format="html", profile="messenger")

    assert "1 234,50 ₽<br>" in result
    assert "<b>Поддержка:</b>" in result
    assert "<table" not in result
    assert "border:" not in result
    assert "<style" not in result
