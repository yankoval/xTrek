"""Durable S3 LIST-to-Celery adapter for the temporary single-host worker mode.

Separate scanner/publisher roles deliver events. ``tasks.process_s3_event`` remains
the sole business router, and its operation-state guards remain authoritative.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import random
from contextlib import contextmanager
import sqlite3
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable
from .direct_mode import classify_input, validate_direct_task


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


def configured_routes(input_bucket, internal_bucket, prefix=''):
    if prefix and (prefix.startswith('/') or not prefix.endswith('/') or '..' in prefix.split('/')):
        raise ValueError('S3 event prefix must be relative and end with /')
    routes = {}
    for bucket, prefixes in ((input_bucket, ('Задания/',)),
                             (internal_bucket, ROUTED_PREFIXES[INTERNAL_BUCKET])):
        routes.setdefault(bucket, []).extend(prefix + p for p in prefixes)
    return {bucket: tuple(dict.fromkeys(prefixes)) for bucket, prefixes in routes.items()}


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
        self.db.execute("""CREATE TABLE IF NOT EXISTS discovery_retry (
            event_id TEXT PRIMARY KEY, bucket TEXT NOT NULL, object_key TEXT NOT NULL,
            object_meta TEXT NOT NULL, reason TEXT NOT NULL, attempts INTEGER NOT NULL,
            next_retry REAL NOT NULL, created_at REAL NOT NULL,
            state TEXT NOT NULL DEFAULT 'pending'
        )""")
        self.db.execute("CREATE INDEX IF NOT EXISTS discovery_retry_due ON discovery_retry(state,next_retry)")
        self.db.execute("""CREATE TABLE IF NOT EXISTS discovery_prefix_retry (
            bucket TEXT NOT NULL, prefix TEXT NOT NULL, reason TEXT NOT NULL,
            attempts INTEGER NOT NULL, next_retry REAL NOT NULL,
            PRIMARY KEY(bucket,prefix)
        )""")
        self.db.execute("""CREATE TABLE IF NOT EXISTS poller_health (
            role TEXT PRIMARY KEY, updated_at REAL NOT NULL, metrics TEXT NOT NULL
        )""")
        self.db.execute("CREATE INDEX IF NOT EXISTS outbox_pending_due ON s3_outbox(next_attempt,lease_until,created_at) WHERE state='pending'")
        self.db.execute("CREATE INDEX IF NOT EXISTS outbox_confirmed ON s3_outbox(published_at) WHERE published_at IS NOT NULL")
        self.db.commit()
        self._locks = {}

    @contextmanager
    def role_lock(self, role):
        """Host-local flock, shared by all entry points; never unlink lock files."""
        import fcntl
        if role in self._locks:
            yield
            return
        # resolve aliases to the same database path before choosing the lock.
        lock_path = str(self.path.resolve()) + '.' + role + '.lock'
        with open(lock_path, 'a') as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise RuntimeError('Poller role already active: ' + role) from None
            self._locks[role] = lock
            try:
                yield
            finally:
                del self._locks[role]
                fcntl.flock(lock, fcntl.LOCK_UN)

    def known(self, event_id):
        return self.db.execute("SELECT 1 FROM s3_outbox WHERE event_id=?", (event_id,)).fetchone() is not None

    def retry_record(self, event_id):
        return self.db.execute("SELECT state,next_retry FROM discovery_retry WHERE event_id=?", (event_id,)).fetchone()

    def defer(self, event_id, bucket, key, obj, reason, *, attempted=True):
        old = self.db.execute("SELECT attempts FROM discovery_retry WHERE event_id=?", (event_id,)).fetchone()
        attempts = (old[0] if old else 0) + int(attempted)
        now = time.time()
        meta = json.dumps(obj, default=_utc, ensure_ascii=False)
        with self.db:
            self.db.execute("""INSERT INTO discovery_retry
                (event_id,bucket,object_key,object_meta,reason,attempts,next_retry,created_at)
                VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(event_id) DO UPDATE SET
                reason=excluded.reason, attempts=excluded.attempts, next_retry=excluded.next_retry, state='pending'""",
                (event_id, bucket, key, meta, reason, attempts, now + retry_delay(attempts, reason), now))

    def resolve_retry(self, event_id, state='resolved'):
        with self.db:
            self.db.execute("UPDATE discovery_retry SET state=? WHERE event_id=?", (state, event_id))

    def due_retries(self, limit):
        return self.db.execute("""SELECT event_id,bucket,object_key,object_meta FROM discovery_retry
            WHERE state='pending' AND next_retry<=? ORDER BY next_retry,event_id LIMIT ?""",
            (time.time(), limit)).fetchall()

    def prefix_ready(self, bucket, prefix):
        row = self.db.execute("SELECT next_retry FROM discovery_prefix_retry WHERE bucket=? AND prefix=?",
                              (bucket, prefix)).fetchone()
        return not row or row[0] <= time.time()

    def prefix_failed(self, bucket, prefix, reason):
        row = self.db.execute("SELECT attempts FROM discovery_prefix_retry WHERE bucket=? AND prefix=?",
                              (bucket, prefix)).fetchone()
        attempts = (row[0] if row else 0) + 1
        with self.db:
            self.db.execute("INSERT OR REPLACE INTO discovery_prefix_retry VALUES(?,?,?,?,?)",
                            (bucket, prefix, reason, attempts, time.time() + retry_delay(attempts, reason)))

    def prefix_succeeded(self, bucket, prefix):
        with self.db:
            self.db.execute("DELETE FROM discovery_prefix_retry WHERE bucket=? AND prefix=?", (bucket, prefix))

    def health(self):
        count, oldest = self.db.execute("SELECT COUNT(*),MIN(created_at) FROM s3_outbox WHERE state='pending'").fetchone()
        return {
            'pending': count, 'oldest_pending_age_seconds': max(0, time.time() - oldest) if oldest else 0,
            'last_confirmed_publish': self.db.execute("SELECT MAX(published_at) FROM s3_outbox WHERE published_at IS NOT NULL").fetchone()[0],
            'retry_pending': self.db.execute("SELECT COUNT(*) FROM discovery_retry WHERE state='pending'").fetchone()[0],
            'prefix_backoffs': self.db.execute("SELECT COUNT(*) FROM discovery_prefix_retry").fetchone()[0],
        }

    def record_health(self, role, metrics):
        with self.db:
            self.db.execute("INSERT OR REPLACE INTO poller_health VALUES(?,?,?)",
                            (role, time.time(), json.dumps(metrics, sort_keys=True)))

    def enqueue(self, event_id: str, bucket: str, key: str) -> bool:
        payload = json.dumps({"bucket": bucket, "key": key}, ensure_ascii=False)
        with self.db:
            cur = self.db.execute(
                "INSERT OR IGNORE INTO s3_outbox(event_id,bucket,object_key,payload,created_at) VALUES(?,?,?,?,?)",
                (event_id, bucket, key, payload, time.time()),
            )
            return cur.rowcount == 1

    def claim(self, now: float | None = None, lease_seconds: int = 120):
        now = time.time() if now is None else now
        with self.db:
            self.db.execute("BEGIN IMMEDIATE")
            row = self.db.execute(
                "SELECT event_id,payload FROM s3_outbox WHERE state='pending' AND next_attempt<=? AND lease_until<=? ORDER BY created_at LIMIT 1",
                (now, now),
            ).fetchone()
            if row:
                self.db.execute("UPDATE s3_outbox SET lease_until=?,attempts=attempts+1 WHERE event_id=?", (now + lease_seconds, row[0]))
            return row

    def published(self, event_id: str):
        with self.db:
            self.db.execute("UPDATE s3_outbox SET state=CASE WHEN state='completed' THEN state ELSE 'published' END,published_at=?,lease_until=0,last_error=NULL WHERE event_id=?", (time.time(), event_id))

    def failed(self, event_id: str, error: Exception):
        row = self.db.execute("SELECT attempts FROM s3_outbox WHERE event_id=?", (event_id,)).fetchone()
        attempts = row[0] if row else 1
        delay = min(300, 2 ** min(attempts, 8))
        with self.db:
            self.db.execute("UPDATE s3_outbox SET lease_until=0,next_attempt=?,last_error=? WHERE event_id=? AND state='pending'", (time.time() + delay, type(error).__name__, event_id))

    def task_state(self, event_id: str, state: str, detail: str | None = None):
        now = time.time()
        with self.db:
            self.db.execute("INSERT INTO task_attempts VALUES(?,?,?,?)", (event_id, state, now, detail))
            if state == "succeeded":
                self.db.execute("UPDATE s3_outbox SET state='completed',lease_until=0 WHERE event_id=?", (event_id,))

    def scan_cursor(self):
        row = self.db.execute("SELECT prefix_index FROM scan_control WHERE singleton=1").fetchone()
        return int(row[0])

    def page_token(self, bucket: str, prefix: str):
        row = self.db.execute("SELECT continuation_token FROM prefix_scan_state WHERE bucket=? AND prefix=?",
                              (bucket, prefix)).fetchone()
        return row[0] if row else None

    def save_scan_cursor(self, next_index: int, bucket: str, prefix: str, token: str | None):
        with self.db:
            self.db.execute("INSERT INTO prefix_scan_state VALUES(?,?,?) ON CONFLICT(bucket,prefix) "
                            "DO UPDATE SET continuation_token=excluded.continuation_token",
                            (bucket, prefix, token))
            self.db.execute("UPDATE scan_control SET prefix_index=? WHERE singleton=1", (next_index,))

    def close(self):
        self.db.close()


def retry_delay(attempts, reason):
    base = 300 if reason in {'auth', 'invalid_json', 'missing'} else 5
    cap = 3600 if base == 300 else 300
    return min(cap, base * 2 ** min(max(attempts - 1, 0), 10)) * random.uniform(0.8, 1.0)


def error_kind(exc):
    code = getattr(exc, 'response', {}).get('Error', {}).get('Code', '')
    if code in {'AccessDenied', 'SignatureDoesNotMatch', 'InvalidAccessKeyId', 'ExpiredToken',
                'InvalidToken', 'TokenRefreshRequired', 'Unauthorized', '403', '401'}:
        return 'auth'
    if code in {'NoSuchKey', 'NoSuchVersion', '404'}:
        return 'missing'
    if code in {'PreconditionFailed', '412'} or isinstance(exc, ObjectReplaced):
        return 'replaced'
    if isinstance(exc, (ValueError, UnicodeError)):
        return 'invalid_json'
    return 'transient'


class ObjectReplaced(Exception):
    pass


@dataclass
class S3EventPoller:
    s3: object
    ledger: EventLedger
    cutoff_utc: datetime
    allowlist: frozenset[tuple[str, str]] = frozenset()
    allowed_inns: frozenset[str] = frozenset()
    max_gets: int = 100
    retry_limit: int = 20
    cycle_seconds: float = 30
    input_bucket: str = INPUT_BUCKET
    event_prefix: str = ''
    routed_prefixes: dict | None = None
    metrics: dict = field(default_factory=dict, init=False)

    def __post_init__(self):
        if self.max_gets < 1 or self.retry_limit < 0 or self.cycle_seconds <= 0:
            raise ValueError('Invalid discovery budgets')
        self.last_list = None
        row = self.ledger.db.execute("SELECT metrics FROM poller_health WHERE role='scan'").fetchone()
        if row:
            self.last_list = json.loads(row[0]).get('last_successful_list')
        # Count wire attempts separately from API calls, including SDK retries.
        events = getattr(getattr(self.s3, 'meta', None), 'events', None)
        if events is not None:
            for operation, counter in [('ListObjectsV2', 'list_sdk_attempts'), ('GetObject', 'get_sdk_attempts')]:
                def count_attempt(counter=counter, **kwargs):
                    self.metrics[counter] = self.metrics.get(counter, 0) + 1
                events.register('before-send.s3.' + operation, count_attempt)
        self.sdk_measurable = events is not None

    def _source_inn_allowed(self, bucket: str, key: str, obj: dict) -> bool:
        if bucket != self.input_bucket or not key.startswith(self.event_prefix + 'Задания/') or not self.allowed_inns:
            return True
        request = {'Bucket': bucket, 'Key': key}
        if obj.get('ETag'):
            request['IfMatch'] = obj['ETag']
        if obj.get('VersionId'):
            request['VersionId'] = obj['VersionId']
        self.metrics['get_calls'] += 1
        response = self.s3.get_object(**request)
        try:
            # Never authorize a LIST identity using bytes belonging to an overwrite.
            actual = {'ETag': response.get('ETag'), 'Size': response.get('ContentLength'),
                      'LastModified': response.get('LastModified'),
                      'VersionId': response.get('VersionId') if obj.get('VersionId') else None}
            if any(actual[field] is None for field in ('ETag', 'Size', 'LastModified')):
                raise RuntimeError('Incomplete GET metadata')
            # LIST may preserve milliseconds, whereas GET's HTTP-date has only
            # whole seconds. Normalize only this comparison, never the LIST
            # metadata used by the durable event identity or retry record.
            expected = dict(obj)
            listed_modified = obj.get('LastModified')
            returned_modified = actual['LastModified']
            if isinstance(listed_modified, str):
                listed_modified = datetime.fromisoformat(listed_modified.replace('Z', '+00:00'))
            if isinstance(returned_modified, str):
                returned_modified = datetime.fromisoformat(returned_modified.replace('Z', '+00:00'))
            if (not isinstance(listed_modified, datetime) or listed_modified.tzinfo is None
                    or not isinstance(returned_modified, datetime) or returned_modified.tzinfo is None):
                raise RuntimeError('Invalid GET or LIST modification time')
            expected['LastModified'] = (listed_modified.replace(microsecond=0)
                                        if returned_modified.microsecond == 0 else listed_modified)
            actual['LastModified'] = returned_modified
            if event_identity(bucket, key, actual, ()) != event_identity(bucket, key, expected, ()):
                raise ObjectReplaced()
            source = json.loads(response['Body'].read())
        finally:
            response['Body'].close()
        if not isinstance(source, dict) or (source.get('PasportData') is not None
                                           and not isinstance(source['PasportData'], dict)):
            raise ValueError('Invalid source shape')
        if 'codes' in source:
            if classify_input(source) == 'codes':
                return True  # Inert input; only the paired task authorizes processing.
        if source.get('directMode') is not None:
            classify_input(source)
            if source.get('directMode'):
                validate_direct_task(source)
                return str(source['PasportData']['Manufacturer_inn']).strip() in self.allowed_inns
        quantity = source.get('Quantity')
        if quantity is None or (isinstance(quantity, str) and not quantity.strip()):
            return True
        inn = str((source.get('PasportData') or {}).get('Manufacturer_inn') or '')
        return inn in self.allowed_inns

    def _discover(self, event_id, bucket, key, obj, *, retry=False):
        if self.ledger.known(event_id):
            self.metrics['known_skips'] += 1
            if retry:
                self.ledger.resolve_retry(event_id)
            return
        record = self.ledger.retry_record(event_id) if not retry else None
        if record and record[0] != 'resolved':
            # All retries (including terminal replaced identities) are owned by
            # the retry queue. A repeated LIST must not bypass its backoff.
            self.metrics['retry_skips'] += 1
            return
        needs_get = bucket == self.input_bucket and key.startswith(self.event_prefix + 'Задания/') and self.allowed_inns
        if needs_get and (self.metrics['get_calls'] >= self.max_gets or time.monotonic() >= self.deadline):
            if not retry:
                self.ledger.defer(event_id, bucket, key, obj, 'budget', attempted=False)
                self.metrics['deferred'] += 1
            return
        if retry:
            self.metrics['object_retries'] += 1
        try:
            allowed = self._source_inn_allowed(bucket, key, obj)
        except Exception as exc:
            # SQLite writes are outside this handler: a disk error must abort
            # the page, never be reclassified as a recoverable S3 error.
            reason = error_kind(exc)
            self.metrics['object_errors'] += 1
            self.metrics['errors_' + reason] = self.metrics.get('errors_' + reason, 0) + 1
            self.ledger.defer(event_id, bucket, key, obj, reason)
            if reason == 'replaced':
                self.ledger.resolve_retry(event_id, 'replaced')
            return
        if allowed:
            self.metrics['new_events'] += int(self.ledger.enqueue(event_id, bucket, key))
        else:
            self.metrics['inn_denied'] += 1
        if retry:
            self.ledger.resolve_retry(event_id)

    def scan(self, max_pages: int = 20) -> int:
        """Each page advances only after all identities are durable or checked."""
        with self.ledger.role_lock('scan'):
            return self._scan(max_pages)

    def _scan(self, max_pages):
        started = time.monotonic()
        self.deadline = started + self.cycle_seconds
        self.metrics = dict.fromkeys(('list_calls', 'get_calls', 'records', 'known_skips', 'new_events',
                                    'object_retries', 'object_errors', 'list_errors', 'retry_skips',
                                    'deferred', 'inn_denied', 'checkpoints', 'prefix_skips'), 0)
        self.metrics.update(list_sdk_attempts=0 if self.sdk_measurable else None,
                            get_sdk_attempts=0 if self.sdk_measurable else None, status='failed')
        try:
            if self.cutoff_utc.tzinfo is None:
                raise ValueError('S3 cutoff must include a timezone')
            # Reserve most time and GET capacity for fresh discovery.
            retry_deadline = started + self.cycle_seconds / 4
            for event_id, bucket, key, meta in self.ledger.due_retries(min(self.retry_limit, max(1, self.max_gets // 4))):
                if time.monotonic() >= retry_deadline:
                    break
                self._discover(event_id, bucket, key, json.loads(meta), retry=True)
            flattened = [(b, p) for b, prefixes in (self.routed_prefixes if self.routed_prefixes is not None else ROUTED_PREFIXES).items() for p in prefixes]
            prefix_index = self.ledger.scan_cursor() % len(flattened)
            for page_number in range(max_pages):
                # One page slot is guaranteed even if a retry overran the soft
                # deadline; otherwise slow retries could starve all LIST sources.
                if page_number and time.monotonic() >= self.deadline:
                    break
                bucket, prefix = flattened[prefix_index]
                next_index = (prefix_index + 1) % len(flattened)
                token = self.ledger.page_token(bucket, prefix)
                if not self.ledger.prefix_ready(bucket, prefix):
                    self.metrics['prefix_skips'] += 1
                    self.ledger.save_scan_cursor(next_index, bucket, prefix, token)
                    prefix_index = next_index
                    continue
                request = {'Bucket': bucket, 'Prefix': prefix, 'MaxKeys': 1000}
                if token:
                    request['ContinuationToken'] = token
                self.metrics['list_calls'] += 1
                try:
                    page = self.s3.list_objects_v2(**request)
                except Exception as exc:
                    self.metrics['list_errors'] += 1
                    reason = error_kind(exc)
                    self.metrics['errors_' + reason] = self.metrics.get('errors_' + reason, 0) + 1
                    code = getattr(exc, 'response', {}).get('Error', {}).get('Code')
                    # ExpiredToken is an authentication failure, not a page token.
                    # InvalidArgument is ambiguous: preserve the token and alert.
                    argument = getattr(exc, 'response', {}).get('Error', {}).get('ArgumentName')
                    invalid_cursor = code == 'InvalidContinuationToken' or (
                        code == 'InvalidArgument' and argument in {'continuation-token', 'ContinuationToken'})
                    if token and invalid_cursor:
                        token = None
                    self.ledger.prefix_failed(bucket, prefix, reason)
                    self.ledger.save_scan_cursor(next_index, bucket, prefix, token)
                    prefix_index = next_index
                    continue
                self.last_list = time.time()
                self.ledger.prefix_succeeded(bucket, prefix)
                records = page.get('Contents', [])
                self.metrics['records'] += len(records)
                for obj in records:
                    key = obj['Key']
                    if key.endswith('/') or not key.startswith(prefix):
                        continue
                    modified = obj.get('LastModified')
                    if isinstance(modified, str):
                        modified = datetime.fromisoformat(modified.replace('Z', '+00:00'))
                    if (bucket, key) not in self.allowlist and (modified is None or modified < self.cutoff_utc):
                        continue
                    self._discover(event_identity(bucket, key, obj, ()), bucket, key, obj)
                token = page.get('NextContinuationToken') if page.get('IsTruncated') else None
                if page.get('IsTruncated') and not token:
                    raise ValueError('Truncated LIST without continuation token')
                self.ledger.save_scan_cursor(next_index, bucket, prefix, token)
                self.metrics['checkpoints'] += 1
                prefix_index = next_index
            self.metrics.update(self.ledger.health())
            self.metrics['status'] = 'degraded' if (self.metrics['object_errors'] or self.metrics['list_errors']
                                                    or self.metrics['retry_pending'] or self.metrics['prefix_backoffs']) else 'ok'
            return self.metrics['new_events']
        except BaseException:
            self.ledger.db.rollback()
            raise
        finally:
            self.metrics.update(duration_seconds=time.monotonic() - started, last_successful_list=self.last_list)
            try:
                self.metrics.update(self.ledger.health())
                self.ledger.record_health('scan', self.metrics)
            except sqlite3.Error:
                self.ledger.db.rollback()
                self.metrics['status'] = 'failed'
                self.metrics['health_persisted'] = False
                raise
            finally:
                LOG.info('S3 discovery cycle %s', json.dumps(self.metrics, sort_keys=True))


def publish_pending(ledger: EventLedger, celery_app, limit: int = 100) -> int:
    """One active publisher per database, including manual invocations."""
    with ledger.role_lock('publish'):
        started = time.monotonic()
        metrics = {'published': 0, 'publish_attempts': 0, 'status': 'failed'}
        try:
            for _ in range(limit):
                row = ledger.claim()
                if row is None:
                    break
                event_id, payload = row
                metrics['publish_attempts'] += 1
                try:
                    # Runtime app uses AMQP confirms. A timeout is NOT success;
                    # the durable outbox owns retries, not hidden Celery retries.
                    celery_app.send_task('tasks.process_s3_event', args=[json.loads(payload)],
                                         task_id=event_id, retry=False, timeout=10, confirm_timeout=10)
                except Exception as exc:
                    ledger.failed(event_id, exc)
                    raise
                ledger.published(event_id)
                metrics['published'] += 1
            metrics['status'] = 'ok'
            return metrics['published']
        except BaseException:
            ledger.db.rollback()
            raise
        finally:
            metrics['duration_seconds'] = time.monotonic() - started
            try:
                metrics.update(ledger.health())
                ledger.record_health('publish', metrics)
            except sqlite3.Error:
                ledger.db.rollback()
                metrics['status'] = 'failed'
                metrics['health_persisted'] = False
                raise
            finally:
                LOG.info('S3 publish cycle %s', json.dumps(metrics, sort_keys=True))


def create_publisher_app():
    """Small producer; do not import worker/signing/S3 business dependencies."""
    from celery import Celery
    from kombu import Queue
    from urllib.parse import urlparse

    broker = os.environ.get('XTREK_LOCAL_BROKER_URL', '')
    parsed = urlparse(broker)
    if (parsed.scheme not in {'amqp', 'amqps'}
            or parsed.hostname not in {'localhost', '127.0.0.1', '::1'}
            or not parsed.username or parsed.username == 'guest'
            or not parsed.password or not parsed.path.strip('/')):
        raise ValueError('Local RabbitMQ requires a dedicated credential and non-default vhost on loopback')
    queue = os.environ.get('XTREK_LOCAL_QUEUE_NAME', 'xtrek_s3_events')
    app = Celery('s3_outbox_publisher', broker=broker)
    app.conf.update(
        broker_transport_options={'confirm_publish': True}, broker_connection_timeout=10,
        task_default_queue=queue, task_queues=(Queue(queue, durable=True),),
        task_default_delivery_mode='persistent', task_serializer='json',
        accept_content=['json'], task_publish_retry=False,
    )
    return app


def configure_poller_logging():
    """Imports may change logger levels; suppress SDK headers/bodies afterwards."""
    logging.getLogger().setLevel(logging.INFO)
    namespaces = ('boto3', 'botocore', 'urllib3', 's3transfer')
    for name in list(logging.Logger.manager.loggerDict) + list(namespaces):
        if any(name == n or name.startswith(n + '.') for n in namespaces):
            logging.getLogger(name).setLevel(logging.WARNING)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--role', choices=('scan', 'publish'), required=True,
                        help='run scanner and publisher as separate processes')
    parser.add_argument('--once', action='store_true', help='run one cycle of the selected role')
    args = parser.parse_args()
    if os.environ.get('XTREK_LOCAL_S3_POLLING') != '1':
        raise SystemExit('Set XTREK_LOCAL_S3_POLLING=1 to enable this temporary mode')
    configure_poller_logging()
    db_path = os.environ.get('XTREK_S3_POLL_LEDGER', '/var/lib/xtrek/s3-poller/outbox.sqlite3')
    interval = max(5, int(os.environ.get('XTREK_S3_POLL_SECONDS', '20')))
    ledger = EventLedger(db_path)
    try:
        with ledger.role_lock(args.role):
            if args.role == 'publish':
                app = create_publisher_app()
                interval = max(1, int(os.environ.get('XTREK_S3_PUBLISH_SECONDS', '2')))
                cycle = lambda: publish_pending(ledger, app)
            else:
                import boto3
                from botocore.config import Config

                cutoff_raw = os.environ.get('XTREK_S3_CUTOFF_UTC')
                if not cutoff_raw:
                    raise SystemExit('Set XTREK_S3_CUTOFF_UTC explicitly; empty-ledger historical replay is disabled')
                cutoff = datetime.fromisoformat(cutoff_raw.replace('Z', '+00:00'))
                if cutoff.tzinfo is None:
                    raise SystemExit('S3 cutoff must include a timezone')
                allowlist_path = os.environ.get('XTREK_S3_BACKFILL_ALLOWLIST')
                allowlist = set()
                if allowlist_path:
                    with open(allowlist_path, encoding='utf-8') as source:
                        allowlist = {(row['bucket'], row['key']) for row in map(json.loads, source)
                                     if row.get('bucket') and row.get('key')}
                allowed_inns = frozenset(v.strip() for v in os.environ.get('XTREK_LOCAL_ALLOWED_INNS', '').split(',') if v.strip())
                if not allowed_inns:
                    raise SystemExit('Set XTREK_LOCAL_ALLOWED_INNS explicitly; unvalidated business signers are not admitted')
                access_key, secret_key = os.environ.get('XTREK_S3_ACCESS_KEY'), os.environ.get('XTREK_S3_SECRET_KEY')
                if not access_key or not secret_key:
                    raise SystemExit('Set XTREK_S3_ACCESS_KEY and XTREK_S3_SECRET_KEY; ambient IAM credentials are disabled')
                s3 = boto3.client('s3', endpoint_url=os.environ.get('XTREK_S3_ENDPOINT_URL', 'https://storage.yandexcloud.net'),
                                  region_name=os.environ.get('XTREK_S3_REGION', 'ru-central1'),
                                  aws_access_key_id=access_key, aws_secret_access_key=secret_key,
                                  config=Config(connect_timeout=5, read_timeout=10,
                                                retries={'mode': 'standard', 'total_max_attempts': 1}))
                poller = S3EventPoller(s3, ledger, cutoff, frozenset(allowlist), allowed_inns,
                                     input_bucket=os.environ.get('XTREK_S3_INPUT_BUCKET', INPUT_BUCKET),
                                     event_prefix=os.environ.get('XTREK_S3_EVENT_PREFIX', ''),
                                     routed_prefixes=configured_routes(
                                         os.environ.get('XTREK_S3_INPUT_BUCKET', INPUT_BUCKET),
                                         os.environ.get('XTREK_S3_INTERNAL_BUCKET', INTERNAL_BUCKET),
                                         os.environ.get('XTREK_S3_EVENT_PREFIX', '')),
                                     max_gets=int(os.environ.get('XTREK_S3_MAX_GETS', '100')),
                                     retry_limit=int(os.environ.get('XTREK_S3_RETRY_LIMIT', '20')),
                                     cycle_seconds=float(os.environ.get('XTREK_S3_CYCLE_SECONDS', '30')))
                cycle = poller.scan
            failures = 0
            while True:
                try:
                    cycle()
                    failures = 0
                except sqlite3.Error:
                    # Fail closed on storage faults; systemd restarts with delay.
                    LOG.error('S3 role storage failure role=%s', args.role)
                    raise SystemExit(1) from None
                except Exception as exc:
                    failures += 1
                    LOG.error('S3 role cycle failed role=%s error_type=%s', args.role, type(exc).__name__)
                    if args.once:
                        raise SystemExit(1) from None
                if args.once:
                    return
                time.sleep(max(interval, retry_delay(failures, 'transient')) if failures else interval)
    finally:
        ledger.close()


if __name__ == "__main__":
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
    try:
        main()
    except Exception as exc:
        LOG.error('S3 role stopped error_type=%s', type(exc).__name__)
        raise SystemExit(1) from None
