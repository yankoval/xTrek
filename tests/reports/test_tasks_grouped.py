from datetime import datetime
from decimal import Decimal

from xtrek.reports import Table, render
from xtrek.reports.tasks import TaskObjectRef
from xtrek.reports.tasks_grouped import (
    TasksGroupedReportData,
    build,
    collect,
    collect_range,
)
from xtrek.reports.tasks_goupped import TasksGouppedReportData


class FakeSource:
    def __init__(self, objects):
        self.objects = objects

    def list_objects(self):
        return [item[0] for item in self.objects]

    def read_object(self, ref):
        return dict(next(payload for item_ref, payload in self.objects if item_ref == ref))


def ref(key, timestamp):
    return TaskObjectRef(key, datetime.fromisoformat(timestamp))


def test_misspelled_data_class_name_remains_a_compatibility_alias():
    assert TasksGouppedReportData is TasksGroupedReportData


def test_collects_requested_day_and_groups_by_article():
    source = FakeSource(
        [
            (
                ref("Задания/one.json", "2026-08-25T08:00:00+03:00"),
                {"Article": "B-200", "Quantity": 10, "PasportData": {"Product_PackQty": 6}},
            ),
            (
                ref("Задания/two.json", "2026-08-25T09:00:00+03:00"),
                {"Article": "A-100", "Quantity": "20", "PasportData": {"Product_PackQty": 4}},
            ),
            (
                ref("Задания/three.json", "2026-08-25T10:00:00+03:00"),
                {"Article": "B-200", "Quantity": "5,5", "PasportData": {"Product_PackQty": 6}},
            ),
            (
                ref("Задания/other-day.json", "2026-08-24T10:00:00+03:00"),
                {"Article": "A-100", "Quantity": 99, "PasportData": {"Product_PackQty": 1}},
            ),
        ]
    )

    result = collect(
        source,
        day="2026-08-25",
        generated_at=datetime.fromisoformat("2026-08-25T16:30:45+00:00"),
    )

    assert [group.article for group in result.groups] == ["A-100", "B-200"]
    assert result.groups[1].tasks == 2
    assert result.groups[1].labels == Decimal("15.5")
    assert result.groups[1].codes == Decimal(93)
    assert [item.time for item in result.groups[1].files] == ["08:00", "10:00"]
    assert result.groups[1].files[0].file == "one.json"
    assert result.tasks == 3
    assert result.labels == Decimal("35.5")
    assert result.codes == Decimal(173)
    assert result.generated_at.isoformat() == "2026-08-25T19:30:45+03:00"


def test_missing_article_is_grouped_explicitly():
    source = FakeSource(
        [
            (
                ref("Задания/task.json", "2026-08-25T10:00:00+03:00"),
                {"Quantity": 2, "PasportData": {"Product_PackQty": 3}},
            )
        ]
    )

    result = collect(source, day="2026-08-25")

    assert result.groups[0].article == "Без артикула"


def test_builds_aligned_table_with_totals_and_messenger_cards():
    source = FakeSource(
        [
            (
                ref("Задания/task.json", "2026-08-25T10:00:00+03:00"),
                {"Article": "A-100", "Quantity": 1640, "PasportData": {"Product_PackQty": 6}},
            )
        ]
    )

    document = build(
        collect(
            source,
            day="2026-08-25",
            generated_at=datetime.fromisoformat("2026-08-25T16:30:45+00:00"),
        )
    )
    table = document.pages[0].blocks[0]
    message = render(document, output_format="html", profile="messenger")
    browser = render(document, output_format="html", profile="browser")

    assert document.title == "Отчет о заданиях с группировкой по артикулам"
    assert document.metadata["report"] == "tasks-grouped"
    assert document.header[0].label == "Дата и время создания отчёта"
    assert document.header[0].small is True
    assert document.header[1].label == "Дата"
    assert '<div class="key-value small">' in browser
    assert isinstance(table, Table)
    assert [column.align for column in table.columns] == ["left", "right", "right", "right"]
    assert table.rows[-1] == ("Итого", 1, Decimal(1640), Decimal(9840))
    assert document.pages[1].blocks[0].text == "Расшифровка по файлам"
    detail_table = document.pages[1].blocks[2]
    assert detail_table.rows == (("10:00", Decimal(1640), Decimal(9840), "task.json"),)
    assert message.startswith("<b>Отчет о заданиях с группировкой по артикулам</b>")
    assert "<b>Дата и время создания отчёта:</b> 25.08.2026 19:30:45\n" in message
    assert message.index("Дата и время создания отчёта") < message.index("<b>Дата:</b>")
    assert "<b>Дата:</b> 25.08.2026\n" in message
    assert "<b>Артикул:</b> A-100\n" in message
    assert "<b>Кодов:</b> 9 840\n" in message
    assert "<table" not in message
    assert "<br" not in message


def test_details_can_be_omitted_for_compact_messenger_output():
    source = FakeSource(
        [
            (
                ref("Задания/task.json", "2026-08-25T10:00:00+03:00"),
                {"Article": "A-100", "Quantity": 1, "PasportData": {"Product_PackQty": 2}},
            )
        ]
    )

    document = build(collect(source, day="2026-08-25"), include_details=False)
    message = render(document, output_format="html", profile="messenger")

    assert len(document.pages) == 1
    assert "Расшифровка по файлам" not in message
    assert "task.json" not in message


def test_grouped_range_shows_dates_in_file_details():
    source = FakeSource(
        [
            (
                ref("Задания/late.json", "2026-08-25T23:59:30+03:00"),
                {"Article": "A-100", "Quantity": 2, "PasportData": {"Product_PackQty": 3}},
            ),
            (
                ref("Задания/early.json", "2026-08-26T00:00:30+03:00"),
                {"Article": "A-100", "Quantity": 4, "PasportData": {"Product_PackQty": 5}},
            ),
        ]
    )

    data = collect_range(
        source,
        date_from="2026-08-25T23:59",
        date_to="2026-08-26T00:00",
        generated_at=datetime.fromisoformat("2026-08-26T01:00:00+03:00"),
    )
    document = build(data)
    detail_table = document.pages[1].blocks[2]

    assert data.day is None
    assert data.tasks == 2
    assert [item.time for item in data.groups[0].files] == ["23:59", "00:00"]
    assert document.header[1].label == "Дата и время от"
    assert document.header[2].label == "Дата и время до"
    assert detail_table.columns[0].title == "Дата и время"
    assert detail_table.rows[0][0] == "25.08.2026 23:59"
    assert detail_table.rows[1][0] == "26.08.2026 00:00"
