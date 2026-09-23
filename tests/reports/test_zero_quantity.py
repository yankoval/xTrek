from datetime import datetime
from decimal import Decimal

import pytest

from xtrek.reports import render
from xtrek.reports import tasks, tasks_grouped
from xtrek.reports.tasks import TaskObjectRef


class FakeSource:
    def __init__(self, payloads):
        self.payloads = payloads

    def list_objects(self):
        return [
            TaskObjectRef(key, datetime.fromisoformat("2026-08-25T10:00:00+03:00"))
            for key in self.payloads
        ]

    def read_object(self, ref):
        return self.payloads[ref.key]


@pytest.fixture(params=[tasks, tasks_grouped], ids=["tasks", "tasks-grouped"])
def report(request):
    return request.param


@pytest.fixture(params=["day", "range"])
def collect_report(request, report):
    def collect(payloads):
        source = FakeSource(payloads)
        grouping = {"group_by": "article"} if report is tasks_grouped else {}
        if request.param == "day":
            return report.collect(source, day="2026-08-25", **grouping)
        return report.collect_range(
            source,
            date_from="2026-08-25T09:00",
            date_to="2026-08-25T11:00",
            **grouping,
        )

    return collect


@pytest.mark.parametrize("quantity_fields", [
    {"Quantity": 0},
    {"Quantity": "0"},
    {"Quantity": "0.0"},
    {"Quantity": "0,00"},
    {"Quantity": ""},
    {"Quantity": None},
    {},
])
def test_zero_quantity_is_excluded_from_totals_and_details(
    report, collect_report, quantity_fields
):
    zero = {**quantity_fields, "PasportData": {"Product_PackQty": 6}}
    result = collect_report({
        "productionOrders/positive.json": {
            "Article": "A-100", "Quantity": 10,
            "PasportData": {"Product_PackQty": 6},
        },
        "productionOrders/zero-same-article.json": {**zero, "Article": "A-100"},
        "productionOrders/zero-only-article.json": {**zero, "Article": "ZERO-ONLY"},
    })

    assert result.tasks == 1
    assert result.labels == Decimal(10)
    assert result.codes == Decimal(60)
    if report is tasks_grouped:
        assert [group.article for group in result.groups] == ["A-100"]
        assert result.groups[0].tasks == 1
        assert [item.file for item in result.groups[0].files] == ["positive.json"]
        message = render(report.build(result), output_format="html", profile="messenger")
        assert "positive.json" in message
        assert "zero-same-article.json" not in message
        assert "zero-only-article.json" not in message
        assert "ZERO-ONLY" not in message


def test_only_zero_quantity_orders_produce_empty_report(report, collect_report):
    result = collect_report({
        "productionOrders/zero.json": {
            "Article": "A-100", "Quantity": 0,
            "PasportData": {"Product_PackQty": 6},
        },
    })

    assert result.tasks == 0
    assert result.labels == Decimal(0)
    assert result.codes == Decimal(0)
    if report is tasks_grouped:
        assert result.groups == ()


def test_positive_quantity_with_zero_pack_size_is_still_counted(collect_report):
    result = collect_report({
        "productionOrders/positive.json": {
            "Article": "A-100", "Quantity": 10,
            "PasportData": {"Product_PackQty": 0},
        },
    })

    assert result.tasks == 1
    assert result.labels == Decimal(10)
    assert result.codes == Decimal(0)
