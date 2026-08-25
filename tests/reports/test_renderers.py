import sys

import pytest

from xtrek.reports import KeyValue, Page, Report, Table, render
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
