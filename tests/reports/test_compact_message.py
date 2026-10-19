from html import unescape
import re

import pytest

from xtrek.reports import render
from xtrek.reports.model import KeyValue, Link, Page, Paragraph, Report
from xtrek.reports.tasks_grouped import build, collect, collect_range
from test_tasks_grouped import FakeSource, ref


def visible(content):
    return unescape(re.sub(r"<[^>]+>", "", content))


def source():
    return FakeSource([
        (ref("productionOrders/one.json", "2026-10-19T10:00:00+03:00"),
         {"Article": "1124-77-001", "Quantity": 2285,
          "PasportData": {"operator": "Иванов Иван Иванович", "Product_PackQty": 6}}),
        (ref("productionOrders/two.json", "2026-10-19T11:00:00+03:00"),
         {"Article": "1589-77-002", "Quantity": 1200,
          "PasportData": {"operator": "Иванов Иван Иванович", "Product_PackQty": 6}}),
        (ref("productionOrders/zero.json", "2026-10-19T12:00:00+03:00"),
         {"Article": "ZERO", "Quantity": 0,
          "PasportData": {"operator": "Другой", "Product_PackQty": 6}}),
    ])


@pytest.mark.parametrize("groups", [(), ("operator",), ("article",),
                                      ("operator", "article"), ("article", "operator")])
@pytest.mark.parametrize("period", [False, True])
def test_mobile_width_preserves_grouping_and_totals(groups, period):
    selection = ({"date_from": "2026-10-18T20:00", "date_to": "2026-10-19T20:00"}
                 if period else {"day": "2026-10-19"})
    data = (collect_range if period else collect)(source(), group_by=groups, **selection)
    report = build(data, include_details=False)
    text = visible(render(report, output_format="html", profile="messenger"))
    assert max(map(len, text.splitlines())) <= 20
    assert text.startswith("ЗАДАНИЯ\n")
    assert text.endswith("Кодов: 20 910\n")
    assert "Заданий: 2\nЯрлыков: 3 485\nКодов: 20 910" in text
    assert "ZERO" not in text and "Другой" not in text
    if groups:
        assert ("1124-77-001" in text) == ("article" in groups)
        if "operator" in groups:
            assert "Иванов Иван Иванович" in text
    if len(groups) == 2:
        assert "Итого по" in text
    if period:
        assert "С 18.10.2026 20:00\nПо 19.10.2026 20:00" in text
    else:
        assert "Дата: 19.10.2026" in text


def test_identifiers_links_and_html_are_not_damaged():
    identifier = "T-1124-77-001-2996-C-01-6739be97-8a06-4c56-8bd4-b760c6435232"
    report = Report(title="test", metadata={"report": "tasks"}, pages=(Page(blocks=(
        KeyValue("Отчёт", identifier),
        KeyValue("SSCC", "346070517900670502"),
        Paragraph('Иван <Пётр> & "Сергей"'),
        Paragraph(Link("Загрузить отчёт", "https://example.org/?a=1&b=2")),
    )),))
    content = render(report, output_format="html", profile="messenger")
    text = visible(content)
    assert identifier in text.splitlines()
    assert "346070517900670502" in text.splitlines()
    assert "&lt;Пётр&gt;" in content and "&amp;" in content
    assert '<a href="https://example.org/?a=1&amp;b=2">Загрузить отчёт</a>' in content


def test_file_profiles_and_document_are_unchanged():
    report = build(collect(source(), day="2026-10-19", group_by=("operator", "article")),
                   include_details=False)
    original_title = report.title
    browser_before = render(report, output_format="html", profile="browser")
    markdown_before = render(report, output_format="md", profile="printer")
    render(report, output_format="html", profile="messenger")
    assert report.title == original_title
    assert render(report, output_format="html", profile="browser") == browser_before
    assert render(report, output_format="md", profile="printer") == markdown_before
    assert "Паспортов (ярлыков)" in browser_before
    assert "Паспортов \\(ярлыков\\)" in markdown_before


@pytest.mark.parametrize("details", [False, True])
def test_equipment_report_uses_the_same_compact_cards(details):
    from xtrek.reports.equipment_grouped.data import collect as collect_equipment
    from xtrek.reports.equipment_grouped.document import build as build_equipment
    from test_equipment_grouped_data import FakeSource as EquipmentSource

    report = build_equipment(collect_equipment(EquipmentSource(), day="2026-10-02"),
                             include_details=details)
    text = visible(render(report, output_format="html", profile="messenger"))
    assert max(map(len, text.splitlines())) <= 20
    assert text.startswith("ОТЧЁТЫ ОБОРУДОВАНИЯ\n")
    assert "ARTICLE-1" in text
    assert "Отчётов: 1\nКоробов: 2\nКодов: 3" in text
    assert "Итого по оператору:" in text
    assert ("report-1" in text) == details


def test_long_name_is_wrapped_without_abbreviation_and_large_number_preserved():
    name = "Иванов Иван Александрович"
    report = Report(title="test", metadata={"report": "tasks"}, pages=(Page(blocks=(
        KeyValue("Оператор", name),
        KeyValue("Кодов", "123 456 789 012 345"),
    )),))
    text = visible(render(report, output_format="html", profile="messenger"))
    assert max(map(len, text.splitlines())) <= 20
    assert "Иванов Иван\nАлександрович" in text
    assert "Кодов:\n123 456 789 012 345" in text


def test_group_named_total_is_not_confused_with_the_final_total():
    from xtrek.reports.model import Table, TableColumn

    report = Report(title="test", metadata={"report": "tasks-grouped"}, pages=(Page(blocks=(
        Table(columns=(TableColumn("Оператор"), TableColumn("Заданий")),
              rows=(("Итого", 1), ("Итого", 1))),
    )),))
    text = visible(render(report, output_format="html", profile="messenger"))
    assert "Оператор: Итого\nЗаданий: 1" in text
    assert text.count("ИТОГО") == 1
