from datetime import datetime, timezone

from xtrek.reports.equipment_grouped.source import S3EquipmentReportSource


class FakePaginator:
    def paginate(self, **kwargs):
        assert kwargs == {
            "Bucket": "bucket",
            "Prefix": "equipment-reports/",
        }
        return [
            {
                "Contents": [
                    {
                        "Key": "equipment-reports/before.json",
                        "LastModified": datetime(
                            2026, 10, 1, 15, 20, 59, tzinfo=timezone.utc
                        ),
                    },
                    {
                        "Key": "equipment-reports/finished.json",
                        "LastModified": datetime(
                            2026, 10, 1, 15, 21, tzinfo=timezone.utc
                        ),
                    },
                    {
                        "Key": "equipment-reports/processing.json",
                        "LastModified": datetime(
                            2026, 10, 2, 15, 20, 59, tzinfo=timezone.utc
                        ),
                    },
                    {
                        "Key": "equipment-reports/after.json",
                        "LastModified": datetime(
                            2026, 10, 2, 15, 21, tzinfo=timezone.utc
                        ),
                    },
                ]
            }
        ]


class FakeS3Client:
    def __init__(self):
        self.tag_requests = []

    def get_paginator(self, name):
        assert name == "list_objects_v2"
        return FakePaginator()

    def get_object_tagging(self, **kwargs):
        self.tag_requests.append(kwargs["Key"])
        state = "processing" if kwargs["Key"].endswith("processing.json") else "finished"
        return {"TagSet": [{"Key": "check", "Value": state}]}


def test_filters_s3_listing_by_date_before_requesting_status_tags():
    client = FakeS3Client()
    source = S3EquipmentReportSource(bucket="bucket", client=client)

    refs = list(
        source.list_finished_reports(
            date_from=datetime.fromisoformat("2026-10-01T18:21:00+03:00"),
            date_to_exclusive=datetime.fromisoformat(
                "2026-10-02T18:21:00+03:00"
            ),
            timezone_name="Europe/Moscow",
        )
    )

    assert client.tag_requests == [
        "equipment-reports/finished.json",
        "equipment-reports/processing.json",
    ]
    assert [ref.key for ref in refs] == ["equipment-reports/finished.json"]


def test_check_tag_takes_precedence_over_lifecycle_status():
    class FinishedAndFailedClient(FakeS3Client):
        def get_object_tagging(self, **kwargs):
            self.tag_requests.append(kwargs["Key"])
            return {
                "TagSet": [
                    {"Key": "status", "Value": "finished"},
                    {"Key": "check", "Value": "error"},
                ]
            }

    client = FinishedAndFailedClient()
    source = S3EquipmentReportSource(bucket="bucket", client=client)
    refs = list(
        source.list_finished_reports(
            date_from=datetime.fromisoformat("2026-10-01T18:21:00+03:00"),
            date_to_exclusive=datetime.fromisoformat(
                "2026-10-02T18:21:00+03:00"
            ),
            timezone_name="Europe/Moscow",
        )
    )

    assert refs == []
