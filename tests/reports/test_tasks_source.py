import io
from datetime import datetime, timezone

from xtrek.reports.tasks.source import S3TaskSource


class FakePaginator:
    def paginate(self, **kwargs):
        assert kwargs == {"Bucket": "bucket", "Prefix": "Задания/"}
        return [
            {
                "Contents": [
                    {
                        "Key": "Задания/one.json",
                        "LastModified": datetime(2026, 8, 25, tzinfo=timezone.utc),
                    },
                    {
                        "Key": "Задания/readme.txt",
                        "LastModified": datetime(2026, 8, 25, tzinfo=timezone.utc),
                    },
                ]
            },
            {
                "Contents": [
                    {
                        "Key": "Задания/two.JSON",
                        "LastModified": datetime(2026, 8, 26, tzinfo=timezone.utc),
                    }
                ]
            },
        ]


class FakeS3Client:
    def get_paginator(self, name):
        assert name == "list_objects_v2"
        return FakePaginator()

    def get_object(self, **kwargs):
        assert kwargs == {"Bucket": "bucket", "Key": "Задания/one.json"}
        return {"Body": io.BytesIO(b'{"Quantity": 5}')}


def test_s3_source_paginates_filters_and_reads_json():
    source = S3TaskSource(bucket="bucket", client=FakeS3Client())

    refs = list(source.list_objects())

    assert [item.key for item in refs] == ["Задания/one.json", "Задания/two.JSON"]
    assert source.read_object(refs[0]) == {"Quantity": 5}
