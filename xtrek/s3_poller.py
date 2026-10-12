"""Durable S3 LIST-to-Celery adapter for the temporary single-host worker mode.

The poller only discovers events. Existing ``tasks.process_s3_event`` remains
the sole business router, and its operation-state guards remain authoritative.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import boto3

LOG = logging.getLogger("xtrek.s3_poller")
INPUT_BUCKET = "1bf11148-3595-4a07-a089-d460153b7c7a"
INTERNAL_BUCKET = "20ab2a0c-2726-4ba1-9c7c-7deae82941ff"
ROUTED_PREFIXES = {
    INPUT_BUCKET: ("Задания/",),
    INTERNAL_BUCKET: (
        "emissionOrders/", "emissionReceipts/", "emissions/", "kodes/",
        "utilisationReceipts/", "introduceReceipts/", "equipment-reports/",
        "equipment-reports-disaggregation/", "disaggregationTasks/",
        "disaggregationReceipts/", "equipment-reports-reaggregation-removing/",
        "reaggregationTasks/", "reaggregationReceipts/", "productionOrders/",
        "aggReceipts/", "cisInformationChangeTasks/",
        "cisInformationChangeReceipts/", "aggSetReceipts/",
    ),
}


def _utc(value) -> str:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()
    return str(value or "")


def event_identity(bucket: str, key: str, obj: dict, tags: Iterable[dict]) -> str:
    """Stable identity for an object create/overwrite; tag changes are not events."""
    stable = {
        "bucket": bucket, "key": key, "etag": str(obj.get("ETag", "")).strip('"'),
        "version": obj.get("VersionId"), "size": obj.get("Size"),
        "last_modified": _utc(obj.get("LastModified")),
    }
    raw = json.dumps(stable, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


class EventLedger:
    """SQLite outbox; publishing is at-least-once and uses stable Celery IDs."""

    def __init__(self, path: str):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(self.path, timeout=30)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.execute("PRAGMA busy_timeout=30000")
        self.db.execute("""CREATE TABLE IF NOT EXISTS s3_outbox (
            event_id TEXT PRIMARY KEY, bucket TEXT NOT NULL, object_key TEXT NOT NULL,
            payload TEXT NOT NULL, state TEXT NOT NULL DEFAULT 'pending',
            attempts INTEGER NOT NULL DEFAULT 0, next_attempt REAL NOT NULL DEFAULT 0,
            lease_until REAL NOT NULL DEFAULT 0, last_error TEXT,
            created_at REAL NOT NULL, published_at REAL
        )""")
        self.db.execute("""CREATE TABLE IF NOT EXISTS task_attempts (
            event_id TEXT NOT NULL, state TEXT NOT NULL, updated_at REAL NOT NULL,
            detail TEXT, PRIMARY KEY(event_id, state, updated_at)
        )""")
        self.db.execute("""CREATE TABLE IF NOT EXISTS prefix_scan_state (
            bucket TEXT NOT NULL, prefix TEXT NOT NULL, continuation_token TEXT,
            PRIMARY KEY(bucket,prefix)
        )""")
        self.db.execute("""CREATE TABLE IF NOT EXISTS scan_control (
            singleton INTEGER PRIMARY KEY CHECK(singleton=1), prefix_index INTEGER NOT NULL
        )""")
        self.db.execute("INSERT OR IGNORE INTO scan_control VALUES(1,0)")
        self.db.commit()

    def enqueue(self, event_id: str, bucket: str, key: str) -> bool:
        payload = json.dumps({"bucket": bucket, "key": key}, ensure_ascii=False)
        cur = self.db.execute(
            "INSERT OR IGNORE INTO s3_outbox(event_id,bucket,object_key,payload,created_at) VALUES(?,?,?,?,?)",
            (event_id, bucket, key, payload, time.time()),
        )
        self.db.commit()
        return cur.rowcount == 1

    def claim(self, now: float | None = None, lease_seconds: int = 120):
        now = time.time() if now is None else now
        self.db.execute("BEGIN IMMEDIATE")
        row = self.db.execute(
            "SELECT event_id,payload FROM s3_outbox WHERE state='pending' AND next_attempt<=? AND lease_until<=? ORDER BY created_at LIMIT 1",
            (now, now),
        ).fetchone()
        if row:
            self.db.execute("UPDATE s3_outbox SET lease_until=?,attempts=attempts+1 WHERE event_id=?", (now + lease_seconds, row[0]))
        self.db.commit()
        return row

    def published(self, event_id: str):
        self.db.execute("UPDATE s3_outbox SET state='published',published_at=?,lease_until=0,last_error=NULL WHERE event_id=?", (time.time(), event_id))
        self.db.commit()

    def failed(self, event_id: str, error: Exception):
        row = self.db.execute("SELECT attempts FROM s3_outbox WHERE event_id=?", (event_id,)).fetchone()
        attempts = row[0] if row else 1
        delay = min(300, 2 ** min(attempts, 8))
        self.db.execute("UPDATE s3_outbox SET lease_until=0,next_attempt=?,last_error=? WHERE event_id=?", (time.time() + delay, type(error).__name__, event_id))
        self.db.commit()

    def task_state(self, event_id: str, state: str, detail: str | None = None):
        now = time.time()
        self.db.execute("INSERT INTO task_attempts VALUES(?,?,?,?)", (event_id, state, now, detail))
        if state == "succeeded":
            self.db.execute("UPDATE s3_outbox SET state='completed',lease_until=0 WHERE event_id=?", (event_id,))
        self.db.commit()

    def scan_cursor(self):
        row = self.db.execute("SELECT prefix_index FROM scan_control WHERE singleton=1").fetchone()
        return int(row[0])

    def page_token(self, bucket: str, prefix: str):
        row = self.db.execute("SELECT continuation_token FROM prefix_scan_state WHERE bucket=? AND prefix=?",
                              (bucket, prefix)).fetchone()
        return row[0] if row else None

    def save_scan_cursor(self, next_index: int, bucket: str, prefix: str, token: str | None):
        self.db.execute("INSERT INTO prefix_scan_state VALUES(?,?,?) ON CONFLICT(bucket,prefix) "
                        "DO UPDATE SET continuation_token=excluded.continuation_token",
                        (bucket, prefix, token))
        self.db.execute("UPDATE scan_control SET prefix_index=? WHERE singleton=1", (next_index,))
        self.db.commit()

    def close(self):
        self.db.close()


@dataclass
class S3EventPoller:
    s3: object
    ledger: EventLedger
    cutoff_utc: datetime
    allowlist: frozenset[tuple[str, str]] = frozenset()
    allowed_inns: frozenset[str] = frozenset()

    def _source_inn_allowed(self, bucket: str, key: str) -> bool:
        if bucket != INPUT_BUCKET or not key.startswith("Задания/") or not self.allowed_inns:
            return True
        response = self.s3.get_object(Bucket=bucket, Key=key)
        try:
            source = json.loads(response["Body"].read())
        finally:
            response["Body"].close()
        quantity = source.get("Quantity")
        if quantity is None or (isinstance(quantity, str) and not quantity.strip()):
            # Equipment-only tasks must reach the router even when their INN is
            # outside the local signer allowlist. The router skips all signing
            # and code issuance for this source state.
            return True
        inn = str((source.get("PasportData") or {}).get("Manufacturer_inn") or "")
        if inn not in self.allowed_inns:
            LOG.info("Skipping source outside local signer allowlist: key=%s", key)
            return False
        return True

    def scan(self, max_pages: int = 20) -> int:
        """Scan prefixes fairly with a durable independent token per prefix."""
        if self.cutoff_utc.tzinfo is None:
            raise ValueError("S3 cutoff must include a timezone")
        flattened = [(bucket, prefix) for bucket, prefixes in ROUTED_PREFIXES.items() for prefix in prefixes]
        discovered = 0
        prefix_index = self.ledger.scan_cursor()
        for _ in range(max_pages):
            bucket, prefix = flattened[prefix_index]
            request = {"Bucket": bucket, "Prefix": prefix, "MaxKeys": 1000}
            token = self.ledger.page_token(bucket, prefix)
            if token:
                request["ContinuationToken"] = token
            try:
                page = self.s3.list_objects_v2(**request)
            except Exception as exc:
                code = getattr(exc, "response", {}).get("Error", {}).get("Code")
                if not token or code not in {"InvalidToken", "InvalidArgument", "ExpiredToken"}:
                    raise
                LOG.warning("S3 continuation token expired for %s/%s; restarting prefix", bucket, prefix)
                next_index = (prefix_index + 1) % len(flattened)
                self.ledger.save_scan_cursor(next_index, bucket, prefix, None)
                prefix_index = next_index
                continue
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if key.endswith("/") or not key.startswith(prefix):
                    continue
                modified = obj.get("LastModified")
                if isinstance(modified, str):
                    modified = datetime.fromisoformat(modified.replace("Z", "+00:00"))
                is_allowlisted = (bucket, key) in self.allowlist
                if not is_allowlisted and (modified is None or modified.astimezone(timezone.utc) < self.cutoff_utc):
                    continue
                if not self._source_inn_allowed(bucket, key):
                    continue
                event_id = event_identity(bucket, key, obj, ())
                if self.ledger.enqueue(event_id, bucket, key):
                    discovered += 1
            token = page.get("NextContinuationToken") if page.get("IsTruncated") else None
            next_index = (prefix_index + 1) % len(flattened)
            self.ledger.save_scan_cursor(next_index, bucket, prefix, token)
            prefix_index = next_index
        return discovered


def publish_pending(ledger: EventLedger, celery_app, limit: int = 100) -> int:
    published = 0
    for _ in range(limit):
        row = ledger.claim()
        if row is None:
            break
        event_id, payload = row
        try:
            celery_app.send_task("tasks.process_s3_event", args=[json.loads(payload)], task_id=event_id)
        except Exception as exc:
            ledger.failed(event_id, exc)
            raise
        ledger.published(event_id)
        published += 1
    return published


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--once", action="store_true", help="scan and publish once")
    args = parser.parse_args()
    if os.environ.get("XTREK_LOCAL_S3_POLLING") != "1":
        raise SystemExit("Set XTREK_LOCAL_S3_POLLING=1 to enable this temporary mode")
    db_path = os.environ.get("XTREK_S3_POLL_LEDGER", "/var/lib/xtrek/s3-poller/outbox.sqlite3")
    interval = max(5, int(os.environ.get("XTREK_S3_POLL_SECONDS", "20")))
    cutoff_raw = os.environ.get("XTREK_S3_CUTOFF_UTC")
    if not cutoff_raw:
        raise SystemExit("Set XTREK_S3_CUTOFF_UTC explicitly; empty-ledger historical replay is disabled")
    cutoff = datetime.fromisoformat(cutoff_raw.replace("Z", "+00:00"))
    allowlist_path = os.environ.get("XTREK_S3_BACKFILL_ALLOWLIST")
    allowlist = set()
    if allowlist_path:
        with open(allowlist_path, encoding="utf-8") as allowlist_file:
            allowlist = {(row["bucket"], row["key"]) for row in map(json.loads, allowlist_file) if row.get("bucket") and row.get("key")}
    allowed_inns = frozenset(value.strip() for value in os.environ.get("XTREK_LOCAL_ALLOWED_INNS", "").split(",") if value.strip())
    if not allowed_inns:
        raise SystemExit("Set XTREK_LOCAL_ALLOWED_INNS explicitly; unvalidated business signers are not admitted")
    # Import after environment guard so ordinary tools do not load the worker.
    from xtrek.tasks import app
    ledger = EventLedger(db_path)
    endpoint = os.environ.get("XTREK_S3_ENDPOINT_URL", "https://storage.yandexcloud.net")
    access_key = os.environ.get("XTREK_S3_ACCESS_KEY")
    secret_key = os.environ.get("XTREK_S3_SECRET_KEY")
    if not access_key or not secret_key:
        raise SystemExit("Set XTREK_S3_ACCESS_KEY and XTREK_S3_SECRET_KEY; ambient IAM credentials are disabled")
    s3 = boto3.client("s3", endpoint_url=endpoint, region_name=os.environ.get("XTREK_S3_REGION", "ru-central1"),
                      aws_access_key_id=access_key, aws_secret_access_key=secret_key)
    poller = S3EventPoller(s3, ledger, cutoff, frozenset(allowlist), allowed_inns)
    try:
        while True:
            count = poller.scan()
            sent = publish_pending(ledger, app)
            LOG.info("S3 poll complete: discovered=%d published=%d", count, sent)
            if args.once:
                return
            time.sleep(interval)
    finally:
        ledger.close()


if __name__ == "__main__":
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
    main()
