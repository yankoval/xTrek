from datetime import datetime, timezone
from decimal import Decimal

import pytest

from xtrek.reports import render
from xtrek.reports.tasks import TaskObjectRef, TasksDataError, build, collect


class FakeSource:
    def __init__(self, objects):
        self.objects = objects
        self.read_keys = []

    def list_objects(self):
        return [item[0] for item in self.objects]

    def read_object(self, ref):
        self.read_keys.append(ref.key)
        return dict(next(payload for item_ref, payload in self.objects if item_ref == ref))


def ref(key, timestamp):
    return TaskObjectRef(key, datetime.fromisoformat(timestamp))


def test_collects_only_requested_moscow_calendar_day():
    source = FakeSource(
        [
            (
                ref("Задания/first.json", "2026-08-24T20:59:59+00:00"),
                {"Quantity": 2, "PasportData": {"Product_PackQty": 6}},
            ),
            (
                ref("Задания/second.json", "2026-08-24T21:00:00+00:00"),
                {"Quantity": "3", "PasportData": {"Product_PackQty": "4"}},
            ),
            (
                ref("Задания/empty.json", "2026-08-25T10:00:00+00:00"),
                {"Quantity": "", "PasportData": {"Product_PackQty": 10}},
            ),
        ]
    )

    result = collect(source, day="2026-08-25")

    assert result.tasks == 2
    assert result.labels == Decimal(3)
    assert result.codes == Decimal(12)
    assert source.read_keys == ["Задания/second.json", "Задания/empty.json"]


def test_comma_decimal_values_are_supported():
    source = FakeSource(
        [
            (
                ref("Задания/task.json", "2026-08-25T10:00:00+00:00"),
                {"Quantity": "2,5", "PasportData": {"Product_PackQty": 4}},
            )
        ]
    )

    result = collect(source, day="2026-08-25")

    assert result.labels == Decimal("2.5")
    assert result.codes == Decimal(10)


def test_invalid_nonempty_number_fails_instead_of_silently_changing_totals():
    source = FakeSource(
        [
            (
                ref("Задания/task.json", "2026-08-25T10:00:00+00:00"),
                {"Quantity": "wrong", "PasportData": {"Product_PackQty": 4}},
            )
        ]
    )

    with pytest.raises(TasksDataError, match="Invalid Quantity"):
        collect(source, day="2026-08-25")


def test_build_and_render_approved_tasks_report():
    source = FakeSource(
        [
            (
                ref("Задания/task.json", "2026-08-25T10:00:00+00:00"),
                {"Quantity": 1640, "PasportData": {"Product_PackQty": 6}},
            ),
            (
                ref("Задания/task2.json", "2026-08-25T11:00:00+00:00"),
                {"Quantity": 0, "PasportData": {"Product_PackQty": 1}},
            ),
        ]
    )

    document = build(collect(source, day="2026-08-25"))
    message = render(document, output_format="html", profile="messenger")

    assert document.title == "Отчёт о полученных заданиях"
    assert "<b>Дата:</b> 25.08.2026" in message
    assert "<b>Заданий:</b> 2" in message
    assert "<b>Паспортов (ярлыков):</b> 1 640" in message
    assert "<b>Кодов:</b> 9 840" in message
    assert "Quantity" not in message
