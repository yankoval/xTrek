"""Sources of received xTrek task objects."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Iterable, Mapping, Optional, Protocol

import boto3


DEFAULT_BUCKET = "20ab2a0c-2726-4ba1-9c7c-7deae82941ff"
DEFAULT_PREFIX = "productionOrders/"
DEFAULT_ENDPOINT_URL = "https://storage.yandexcloud.net"

_TASK_UUID = re.compile(
    r"(?<![0-9a-f])"
    r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
    r"(?![0-9a-f])",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class TaskObjectRef:
    key: str
    last_modified: datetime


class TaskSource(Protocol):
    def list_objects(self) -> Iterable[TaskObjectRef]:
        ...

    def read_object(self, ref: TaskObjectRef) -> Mapping[str, Any]:
        ...


class S3TaskSource:
    """Read received-task data from an S3-compatible bucket.

    ``productionOrders/`` may contain several derived files for one received
    task.  For that prefix the source keeps the earliest object containing the
    original task UUID and ignores untraceable manual/repair files.
    """

    def __init__(
        self,
        *,
        bucket: str = DEFAULT_BUCKET,
        prefix: str = DEFAULT_PREFIX,
        endpoint_url: str = DEFAULT_ENDPOINT_URL,
        region_name: str = "ru-central1",
        client: Optional[Any] = None,
        deduplicate_by_uuid: Optional[bool] = None,
    ) -> None:
        self.bucket = bucket
        self.prefix = prefix
        self.deduplicate_by_uuid = (
            prefix.rstrip("/") == DEFAULT_PREFIX.rstrip("/")
            if deduplicate_by_uuid is None
            else deduplicate_by_uuid
        )
        self.client = client or boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            region_name=region_name,
        )

    def list_objects(self) -> Iterable[TaskObjectRef]:
        paginator = self.client.get_paginator("list_objects_v2")
        canonical: Dict[str, TaskObjectRef] = {}
        for page in paginator.paginate(Bucket=self.bucket, Prefix=self.prefix):
            for item in page.get("Contents", []):
                key = str(item.get("Key", ""))
                modified = item.get("LastModified")
                if key.lower().endswith(".json") and isinstance(modified, datetime):
                    ref = TaskObjectRef(key=key, last_modified=modified)
                    if not self.deduplicate_by_uuid:
                        yield ref
                        continue

                    matches = _TASK_UUID.findall(key)
                    if not matches:
                        continue
                    task_uuid = matches[-1].lower()
                    previous = canonical.get(task_uuid)
                    if previous is None or (ref.last_modified, ref.key) < (
                        previous.last_modified,
                        previous.key,
                    ):
                        canonical[task_uuid] = ref

        if self.deduplicate_by_uuid:
            yield from sorted(
                canonical.values(),
                key=lambda ref: (ref.last_modified, ref.key),
            )

    def read_object(self, ref: TaskObjectRef) -> Mapping[str, Any]:
        response = self.client.get_object(Bucket=self.bucket, Key=ref.key)
        raw = response["Body"].read()
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8-sig")
        value = json.loads(raw)
        if not isinstance(value, dict):
            raise ValueError(f"Task object must contain a JSON object: {ref.key}")
        return value
