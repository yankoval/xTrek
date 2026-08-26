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
    source = S3TaskSource(
        bucket="bucket",
        prefix="Задания/",
        client=FakeS3Client(),
    )

    refs = list(source.list_objects())

    assert [item.key for item in refs] == ["Задания/one.json", "Задания/two.JSON"]
    assert source.read_object(refs[0]) == {"Quantity": 5}


class ProductionOrdersPaginator:
    def paginate(self, **kwargs):
        assert kwargs == {"Bucket": "bucket", "Prefix": "productionOrders/"}
        return [
            {
                "Contents": [
                    {
                        "Key": (
                            "productionOrders/"
                            "T-4830-77-001-0686-C-01-"
                            "03346bc9-c07e-4ce6-92d5-2c150d520998.json"
                        ),
                        "LastModified": datetime(
                            2026, 8, 25, 8, 0, tzinfo=timezone.utc
                        ),
                    },
                    {
                        "Key": (
                            "productionOrders/"
                            "V-T-4830-77-001-0686-C-01-"
                            "03346bc9-c07e-4ce6-92d5-2c150d520998-"
                            "04670017922428.json"
                        ),
                        "LastModified": datetime(
                            2026, 8, 25, 8, 5, tzinfo=timezone.utc
                        ),
                    },
                    {
                        "Key": "productionOrders/CODEX-REPAIR-UNIT.json",
                        "LastModified": datetime(
                            2026, 8, 25, 8, 10, tzinfo=timezone.utc
                        ),
                    },
                ]
            },
            {
                "Contents": [
                    {
                        "Key": (
                            "productionOrders/T-OTHER-"
                            "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee.json"
                        ),
                        "LastModified": datetime(
                            2026, 8, 25, 7, 0, tzinfo=timezone.utc
                        ),
                    }
                ]
            },
        ]


class ProductionOrdersClient:
    def get_paginator(self, name):
        assert name == "list_objects_v2"
        return ProductionOrdersPaginator()


def test_production_orders_are_deduplicated_by_original_task_uuid():
    source = S3TaskSource(bucket="bucket", client=ProductionOrdersClient())

    refs = list(source.list_objects())

    assert [item.key for item in refs] == [
        (
            "productionOrders/T-OTHER-"
            "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee.json"
        ),
        (
            "productionOrders/T-4830-77-001-0686-C-01-"
            "03346bc9-c07e-4ce6-92d5-2c150d520998.json"
        ),
    ]
