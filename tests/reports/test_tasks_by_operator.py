from decimal import Decimal

import pytest

from xtrek.reports import render
from xtrek.reports.tasks import TasksDataError
from xtrek.reports.tasks_grouped import OperatorTasksData, build, collect, collect_range
from test_tasks_grouped import FakeSource, ref


def task(name, operator, quantity=2, article="A", timestamp="2026-08-25T10:00:00+03:00"):
    return (
        ref(f"productionOrders/{name}.json", timestamp),
        {"Article": article, "Quantity": quantity,
         "PasportData": {"operator": operator, "Product_PackQty": 3}},
    )


@pytest.mark.parametrize("period", ["day", "range"])
def test_operator_totals_details_and_period_filters(period):
    source = FakeSource([
        task("one", "Иван", article="A"),
        task("two", "Иван", quantity="1,5", article="B"),
        task("three", "Анна", quantity=4, article="A"),
        task("zero", "Иван", quantity=0),
        task("zero-only", "Пётр", quantity="0"),
        task("outside", "Иван", timestamp="2026-08-24T10:00:00+03:00"),
    ])
    if period == "day":
        data = collect(source, day="2026-08-25", group_by=("operator", "article"))
    else:
        data = collect_range(source, date_from="2026-08-25T10:00",
                             date_to="2026-08-25T10:00", group_by=("operator", "article"))

    assert data.group_by == ("operator", "article")
    assert all(isinstance(g, OperatorTasksData) for g in data.groups)
    assert [g.operator for g in data.groups] == ["Анна", "Иван"]
    assert [g.tasks for g in data.groups] == [1, 2]
    assert [(a.article, a.tasks) for a in data.groups[1].groups] == [("A", 1), ("B", 1)]
    assert data.groups[1].labels == Decimal("3.5")
    assert data.groups[1].codes == Decimal("10.5")
    assert [f.file for f in data.groups[1].files] == ["one.json", "two.json"]
    assert (data.tasks, data.labels, data.codes) == (3, Decimal("7.5"), Decimal("22.5"))
    document = build(data)
    assert document.metadata["group_by"] == "operator article"
    summary = document.pages[0].blocks
    assert summary[0].text == "Оператор: Анна"
    assert summary[1].columns[0].title == "Артикул"
    assert summary[1].rows == (("A", 1, 4, 12), ("Итого по оператору: Анна", 1, 4, 12))
    assert summary[2].text == "Оператор: Иван"
    assert summary[3].rows == (
        ("A", 1, 2, 6), ("B", 1, Decimal("1.5"), Decimal("4.5")),
        ("Итого по оператору: Иван", 2, Decimal("3.5"), Decimal("10.5")),
    )
    assert summary[-1].rows == (("Итого", 3, Decimal("7.5"), Decimal("22.5")),)
    assert document.pages[1].blocks[1].text == "Оператор: Анна"
    assert document.pages[1].blocks[2].text == "A"
    html = render(document, output_format="html", profile="messenger")
    assert "оператор → артикул" in html
    assert "Оператор: Иван" in html
    assert "zero" not in html
    assert "outside" not in html
    assert "Пётр" not in html


def test_missing_operator_and_surrounding_whitespace():
    missing = task("missing", None)
    del missing[1]["PasportData"]["operator"]
    data = collect(FakeSource([
        missing, task("null", None), task("empty", ""), task("spaces", "  "),
        task("trimmed", "  Иван  "), task("plain", "Иван"),
    ]), day="2026-08-25", group_by="operator")
    assert [(g.operator, g.tasks) for g in data.groups] == [("Без оператора", 4), ("Иван", 2)]


def test_top_level_operator_is_not_confused_with_passport_operator():
    item = task("task", "Иван")
    item[1]["operator"] = "Другой оператор"
    data = collect(FakeSource([item]), day="2026-08-25", group_by="operator")
    assert data.groups[0].operator == "Иван"


@pytest.mark.parametrize("operator", [123, [], {}])
def test_malformed_operator_fails_explicitly(operator):
    with pytest.raises(TasksDataError, match="PasportData.operator must be a string"):
        collect(FakeSource([task("bad", operator)]), day="2026-08-25", group_by="operator")


def test_unknown_grouping_is_rejected_even_for_empty_source():
    with pytest.raises(TasksDataError, match="group_by must be"):
        collect(FakeSource([]), day="2026-08-25", group_by="unknown")


def test_only_zero_orders_leave_no_operator_groups():
    data = collect(FakeSource([task("zero", "Иван", quantity=0)]),
                   day="2026-08-25", group_by="operator")
    assert data.groups == ()
    assert (data.tasks, data.labels, data.codes) == (0, 0, 0)
    assert build(data).pages[0].blocks[0].rows == (("Итого", 0, 0, 0),)


@pytest.mark.parametrize("include_details", [False, True])
@pytest.mark.parametrize("output_format", ["html", "md"])
def test_operator_subtotals_are_mandatory_in_all_detail_modes(include_details, output_format):
    data = collect(FakeSource([
        task("one", "Иван", quantity=2, article="A"),
        task("two", "Иван", quantity=3, article="B"),
        task("three", None, quantity=4, article="A"),
    ]), day="2026-08-25", group_by=("operator", "article"))
    document = build(data, include_details=include_details)
    assert document.pages[0].blocks[1].rows[-1] == ("Итого по оператору: Без оператора", 1, 4, 12)
    assert document.pages[0].blocks[3].rows[-1] == ("Итого по оператору: Иван", 2, 5, 15)
    assert document.pages[0].blocks[-1].rows == (("Итого", 3, 9, 27),)
    content = render(document, output_format=output_format, profile="messenger")
    assert "Итого по оператору: Иван" in content
    assert "Итого по оператору: Без оператора" in content
    assert ("Расшифровка по файлам" in content) == include_details


def test_missing_article_keeps_its_subgroup_under_operator():
    item = task("missing-article", "Иван")
    del item[1]["Article"]
    data = collect(FakeSource([item]), day="2026-08-25", group_by=("operator", "article"))
    assert data.groups[0].groups[0].article == "Без артикула"
    assert build(data).pages[0].blocks[1].rows[-1] == ("Итого по оператору: Иван", 1, 2, 6)


@pytest.mark.parametrize("period", ["day", "range"])
def test_default_grouping_is_totals_only(period):
    source = FakeSource([
        task("one", "Иван", quantity=2, article="A"),
        task("two", "Иван", quantity=3, article="B"),
        task("three", "Анна", quantity=4, article="A"),
        task("zero", "Пётр", quantity=0),
    ])
    if period == "day":
        collector, selection = collect, {"day": "2026-08-25"}
    else:
        collector, selection = collect_range, {
            "date_from": "2026-08-25T10:00", "date_to": "2026-08-25T10:00",
        }
    default = collector(source, **selection)
    explicit = collector(source, **selection, group_by=[])
    assert default.group_by == ()
    assert default.groups == explicit.groups
    assert default.groups == ()
    assert (default.tasks, default.labels, default.codes) == (3, 9, 27)
    # No file details are included, even when requested explicitly.
    document = build(default, include_details=True)
    assert len(document.pages) == 1
    assert len(document.pages[0].blocks) == 1
    assert document.pages[0].blocks[0].rows[0][1:] == (3, 9, 27)

    article = collector(source, **selection, group_by="article")
    assert [(g.article, g.tasks) for g in article.groups] == [("A", 2), ("B", 1)]
    assert (article.tasks, article.labels, article.codes) == (3, 9, 27)
