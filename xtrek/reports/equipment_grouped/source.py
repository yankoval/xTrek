"""S3 sources for finished equipment reports and their production orders."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Iterable, Mapping, Optional, Protocol

import boto3


DEFAULT_BUCKET = "20ab2a0c-2726-4ba1-9c7c-7deae82941ff"
DEFAULT_REPORTS_PREFIX = "equipment-reports/"
DEFAULT_PRODUCTION_ORDERS_PREFIX = "productionOrders/"
DEFAULT_ENDPOINT_URL = "https://storage.yandexcloud.net"


def _local_datetime(value: datetime, timezone_name: str) -> datetime:
    from datetime import timezone
    from zoneinfo import ZoneInfo

    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(ZoneInfo(timezone_name))


@dataclass(frozen=True)
class EquipmentReportObjectRef:
    key: str
    last_modified: datetime


class EquipmentReportSource(Protocol):
    def list_finished_reports(
        self,
        *,
        date_from: datetime,
        date_to_exclusive: datetime,
        timezone_name: str,
    ) -> Iterable[EquipmentReportObjectRef]:
        ...

    def read_report(self, ref: EquipmentReportObjectRef) -> Mapping[str, Any]:
        ...

    def read_production_order(self, report_id: str) -> Mapping[str, Any]:
        ...


class S3EquipmentReportSource:
    """Read only finished reports and their matching production orders."""

    def __init__(
        self,
        *,
        bucket: str = DEFAULT_BUCKET,
        reports_prefix: str = DEFAULT_REPORTS_PREFIX,
        production_orders_prefix: str = DEFAULT_PRODUCTION_ORDERS_PREFIX,
        endpoint_url: str = DEFAULT_ENDPOINT_URL,
        region_name: str = "ru-central1",
        client: Optional[Any] = None,
    ) -> None:
        self.bucket = bucket
        self.reports_prefix = reports_prefix
        self.production_orders_prefix = production_orders_prefix
        self.client = client or boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            region_name=region_name,
        )

    def list_finished_reports(
        self,
        *,
        date_from: datetime,
        date_to_exclusive: datetime,
        timezone_name: str,
    ) -> Iterable[EquipmentReportObjectRef]:
        """Date-filter S3 listing metadata before requesting any object tags."""
        paginator = self.client.get_paginator("list_objects_v2")
        refs = []
        for page in paginator.paginate(
            Bucket=self.bucket,
            Prefix=self.reports_prefix,
        ):
            for item in page.get("Contents", []):
                key = str(item.get("Key", ""))
                modified = item.get("LastModified")
                if not key.lower().endswith(".json") or not isinstance(
                    modified, datetime
                ):
                    continue
                local_modified = _local_datetime(modified, timezone_name)
                if not date_from <= local_modified < date_to_exclusive:
                    continue
                tags = self.client.get_object_tagging(
                    Bucket=self.bucket,
                    Key=key,
                ).get("TagSet", [])
                tag_values = {tag.get("Key"): tag.get("Value") for tag in tags}
                # Equipment report validation writes check=finished. Some
                # installations use the generic lifecycle status tag instead.
                # If a check tag exists, it is authoritative over lifecycle status.
                is_finished = (
                    tag_values.get("check") == "finished"
                    if "check" in tag_values
                    else tag_values.get("status") == "finished"
                )
                if is_finished:
                    refs.append(EquipmentReportObjectRef(key, modified))
        yield from sorted(refs, key=lambda ref: (ref.last_modified, ref.key))

    def _read_json(self, key: str) -> Mapping[str, Any]:
        response = self.client.get_object(Bucket=self.bucket, Key=key)
        raw = response["Body"].read()
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8-sig")
        value = json.loads(raw)
        if not isinstance(value, dict):
            raise ValueError(f"Expected a JSON object: s3://{self.bucket}/{key}")
        return value

    def read_report(self, ref: EquipmentReportObjectRef) -> Mapping[str, Any]:
        return self._read_json(ref.key)

    def read_production_order(self, report_id: str) -> Mapping[str, Any]:
        filename = (
            report_id
            if report_id.lower().endswith(".json")
            else f"{report_id}.json"
        )
        key = f"{self.production_orders_prefix.rstrip('/')}/{filename}"
        return self._read_json(key)
