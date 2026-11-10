"""Opt-in route for new equipment tasks; no MAX history or delivery ledger."""
import hashlib
import json
import logging
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from html import escape
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlsplit

LOG = logging.getLogger(__name__)
BUCKET = "1bf11148-3595-4a07-a089-d460153b7c7a"
CHAT = "-72448994566325"
PREFIX = "equipment-tasks/"
HOST = "storage.yandexcloud.net"
TTL = 48 * 3600
MAX_ATTEMPTS = 5
REQUEUE_DELAY = 60
# JSON dates come from producer clocks; S3 HEAD also loses subsecond precision.
# This tolerance applies only to their ordering, never to the admission cutoff.


def retry_pause(seconds):
    """Keep the original delivery unacknowledged; no replacement task."""
    time.sleep(seconds)


class PilotInvalid(ValueError):
    """Nonretryable, sanitized event/configuration failure."""


class PilotRetry(RuntimeError):
    """POST may have succeeded; retrying can yield a duplicate."""


def utc(value):
    if not isinstance(value, str):
        raise PilotInvalid("invalid_timestamp")
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise PilotInvalid("invalid_timestamp") from None
    if dt.tzinfo is None:
        raise PilotInvalid("timestamp_without_timezone")
    return dt.astimezone(timezone.utc)


def task_key(key):
    if not isinstance(key, str) or not re.fullmatch(
        r"equipment-tasks/T-[^/\\\x00-\x1f\x7f]{1,240}\.json", key
    ):
        raise PilotInvalid("invalid_task_key")
    return key


@dataclass(frozen=True)
class Cohort:
    not_before: datetime | None = None
    keys: frozenset = frozenset()
    excluded_keys: frozenset = frozenset()
    automatic: bool = False

    @property
    def enabled(self):
        return self.automatic or bool(self.keys)

    def admits_key(self, key):
        try:
            task_key(key)
        except PilotInvalid:
            return False
        return key not in self.excluded_keys and (self.automatic or key in self.keys)

    def admits_object(self, key, modified):
        if not self.admits_key(key):
            return False
        if not self.automatic:
            return True
        if isinstance(modified, str):
            modified = utc(modified)
        if not isinstance(modified, datetime) or modified.tzinfo is None:
            raise PilotInvalid("invalid_object_timestamp")
        return modified.astimezone(timezone.utc) >= self.not_before


def cohort():
    path = os.environ.get("XTREK_MAX_QUEUE_PILOT_COHORT")
    if not path:
        return Cohort()
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise PilotInvalid("invalid_cohort")
    if set(raw) == {"not_before", "keys"} and isinstance(raw["keys"], list):
        return Cohort(utc(raw["not_before"]), frozenset(task_key(k) for k in raw["keys"]))
    if (set(raw) == {"not_before", "mode", "excluded_keys"}
            and raw["mode"] == "new_after_cutoff" and isinstance(raw["excluded_keys"], list)):
        return Cohort(utc(raw["not_before"]), excluded_keys=frozenset(task_key(k) for k in raw["excluded_keys"]), automatic=True)
    raise PilotInvalid("invalid_cohort")


def identity(key):
    return hashlib.sha256(f"max-pilot-v1\0{BUCKET}\0{key}\0{CHAT}".encode()).hexdigest()


def new_task_times(cutoff, created, ready):
    # The producer and S3 use separate clocks. Only S3 records readiness.
    return cutoff is not None and created >= cutoff and ready >= cutoff


def validate(event, now):
    if not isinstance(event, dict) or set(event) != {"event_id", "key", "created_at", "ready_at", "url"}:
        raise PilotInvalid("invalid_event")
    key = task_key(event["key"])
    policy = cohort()
    cutoff = policy.not_before
    created, ready = utc(event["created_at"]), utc(event["ready_at"])
    if (not policy.admits_key(key) or not new_task_times(cutoff, created, ready)
            or ready > now or event["event_id"] != identity(key)):
        raise PilotInvalid("outside_cohort")
    if not isinstance(event["url"], str) or len(event["url"]) > 2048 or any(
        ord(c) < 33 or ord(c) == 127 for c in event["url"]
    ):
        raise PilotInvalid("invalid_url")
    try:
        url = urlsplit(event["url"])
    except ValueError:
        raise PilotInvalid("invalid_url") from None
    if (url.scheme != "https" or url.netloc != HOST or url.fragment
            or unquote(url.path) != f"/{BUCKET}/{key}"):
        raise PilotInvalid("unexpected_url_destination")
    q = parse_qs(url.query)
    try:
        signed = datetime.strptime(q["X-Amz-Date"][0], "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
        expires = int(q["X-Amz-Expires"][0])
        if not re.fullmatch(r"[0-9a-fA-F]{64}", q["X-Amz-Signature"][0]):
            raise ValueError("invalid signature format")
    except (KeyError, ValueError):
        raise PilotInvalid("invalid_signature_metadata") from None
    if not 0 < expires <= TTL or signed + timedelta(seconds=expires) <= now + timedelta(minutes=5):
        raise PilotInvalid("link_expired_or_expiring")
    return key, ready


def publisher_storage(config):
    import boto3
    from botocore.config import Config

    return boto3.client(
        "s3", endpoint_url=f"https://{HOST}",
        aws_access_key_id=config["aws_access_key_id"],
        aws_secret_access_key=config["aws_secret_access_key"],
        region_name=config.get("region_name", "ru-central1"),
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"},
                      connect_timeout=4, read_timeout=10),
    )


def prepare(storage, key):
    """Validate the existing immutable task and issue its download URL."""
    key = task_key(key)
    policy = cohort()
    cutoff = policy.not_before
    if not policy.admits_key(key):
        raise PilotInvalid("outside_cohort")
    try:
        head = storage.head_object(Bucket=BUCKET, Key=key)
        args = {"Bucket": BUCKET, "Key": key, "IfMatch": head["ETag"]}
        if head.get("VersionId"):
            args["VersionId"] = head["VersionId"]
        if not 0 < head["ContentLength"] <= 2 * 1024 * 1024:
            raise PilotInvalid("invalid_json_size")
        response = storage.get_object(**args)
        try:
            payload = json.loads(response["Body"].read(2 * 1024 * 1024 + 1))
        finally:
            response["Body"].close()
        if not isinstance(payload, dict) or payload.get("id") != Path(key).stem:
            raise PilotInvalid("object_id_mismatch")
        created = utc(payload.get("date"))
        ready = head["LastModified"].astimezone(timezone.utc)
        if not new_task_times(cutoff, created, ready):
            raise PilotInvalid("not_a_new_task")
        args.pop("IfMatch")
        args["ResponseContentDisposition"] = "attachment"
        url = storage.generate_presigned_url("get_object", Params=args, ExpiresIn=TTL)
        event = dict(event_id=identity(key), key=key, created_at=created.isoformat(),
                     ready_at=ready.isoformat(), url=url)
        validate(event, datetime.now(timezone.utc))
        return event
    except PilotInvalid:
        raise
    except (ValueError, UnicodeError):
        raise PilotInvalid("invalid_json") from None
    except Exception:
        raise PilotRetry("s3_prepare_failed") from None


def send(event, post, token, ca, attempt):
    now = datetime.now(timezone.utc)
    key, ready = validate(event, now)
    if not token or not ca:
        raise PilotInvalid("missing_max_configuration")
    caption = f'<a href="{escape(event["url"], quote=True)}">Задание: {escape(Path(key).name)}</a>'
    try:
        response = post(
            "https://platform-api2.max.ru/messages",
            params={"chat_id": CHAT, "disable_link_preview": "true"},
            headers={"Authorization": token},
            json={"text": caption, "format": "html", "notify": True},
            timeout=(4, 15), verify=ca, allow_redirects=False,
        )
    except Exception:
        raise PilotRetry("max_response_unknown") from None
    if response.status_code in (408, 425, 429) or response.status_code >= 500:
        raise PilotRetry(f"max_http_{response.status_code}")
    if not 200 <= response.status_code < 300:
        raise PilotInvalid(f"max_http_{response.status_code}")
    try:
        mid = response.json()["message"]["body"]["mid"]
    except (ValueError, KeyError, TypeError):
        raise PilotRetry("max_missing_mid") from None
    if not isinstance(mid, str) or not re.fullmatch(r"mid\.[A-Za-z0-9_.-]{1,150}", mid):
        raise PilotRetry("max_invalid_mid")
    LOG.info("max_pilot accepted event=%s task=%s attempt=%d mid=%s latency_ms=%d",
             event["event_id"], Path(key).stem, attempt, mid,
             (datetime.now(timezone.utc) - ready).total_seconds() * 1000)
    return mid
