"""Sources of received xTrek task objects."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Iterable, Mapping, Optional, Protocol

import boto3


DEFAULT_BUCKET = "1bf11148-3595-4a07-a089-d460153b7c7a"
DEFAULT_PREFIX = "Задания/"
DEFAULT_ENDPOINT_URL = "https://storage.yandexcloud.net"


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
    """Read task JSON files and metadata from an S3-compatible bucket."""

    def __init__(
        self,
        *,
        bucket: str = DEFAULT_BUCKET,
        prefix: str = DEFAULT_PREFIX,
        endpoint_url: str = DEFAULT_ENDPOINT_URL,
        region_name: str = "ru-central1",
        client: Optional[Any] = None,
    ) -> None:
        self.bucket = bucket
        self.prefix = prefix
        self.client = client or boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            region_name=region_name,
        )

    def list_objects(self) -> Iterable[TaskObjectRef]:
        paginator = self.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=self.prefix):
            for item in page.get("Contents", []):
                key = str(item.get("Key", ""))
                modified = item.get("LastModified")
                if key.lower().endswith(".json") and isinstance(modified, datetime):
                    yield TaskObjectRef(key=key, last_modified=modified)

    def read_object(self, ref: TaskObjectRef) -> Mapping[str, Any]:
        response = self.client.get_object(Bucket=self.bucket, Key=ref.key)
        raw = response["Body"].read()
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8-sig")
        value = json.loads(raw)
        if not isinstance(value, dict):
            raise ValueError(f"Task object must contain a JSON object: {ref.key}")
        return value
