from datetime import datetime, timezone

from xtrek.reports.equipment_grouped.data import collect
from xtrek.reports.equipment_grouped.source import EquipmentReportObjectRef


class FakeSource:
    def __init__(self):
        self.ref = EquipmentReportObjectRef(
            "equipment-reports/report-1.json",
            datetime(2026, 10, 2, 14, 0, tzinfo=timezone.utc),
        )

    def list_finished_reports(self, **kwargs):
        assert kwargs["date_from"].isoformat() == "2026-10-02T00:00:00+03:00"
        return [self.ref]

    def read_report(self, ref):
        return {
            "id": "report-1",
            "operator": "Оператор из отчёта",
            "endTime": "2026-10-02T16:50:00+03:00",
            "readyBox": [
                {"boxNumber": "BOX-1", "productNumbersFull": ["a", "b"]},
                {"boxNumber": "BOX-2", "productNumbersFull": ["c"]},
            ],
        }

    def read_production_order(self, report_id):
        assert report_id == "report-1"
        return {
            "Article": "ARTICLE-1",
            "PasportData": {"operator": "Оператор из задания"},
        }


def test_legacy_flat_boxes_and_production_order_operator_are_used():
    result = collect(FakeSource(), day="2026-10-02")

    assert result.reports == 1
    assert result.boxes == 2
    assert result.codes == 3
    assert len(result.operators) == 1
    operator = result.operators[0]
    assert operator.operator == "Оператор из задания"
    assert operator.articles[0].article == "ARTICLE-1"
    assert operator.articles[0].boxes == 2
    assert operator.articles[0].codes == 3
    assert result.details[0].ended_at.isoformat() == "2026-10-02T16:50:00+03:00"
