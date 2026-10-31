"""Idempotent MAX PNG delivery for validated equipment-report pallets.

The caller must validate report/task identity and render the PNG first. This
module enforces bot/chat identity, complete-history preflight, and no retry of an
ambiguous message POST. It is not run automatically by the PRNSRV generator.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
from contextlib import contextmanager
import sqlite3
import time
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any
from urllib.parse import urlencode


from .max_retry import MAX_MID_BATCH_SIZE, MaxRetryLater, RetrySchedule, api_scope, observe_response


MAX_API = 'https://platform-api2.max.ru'


class DeliveryConflict(RuntimeError):
    pass


class DeliveryUncertain(RuntimeError):
    pass


class DeliveryRecheck(DeliveryUncertain, MaxRetryLater):
    def __init__(self, delay, message):
        MaxRetryLater.__init__(self, delay, message)


class HistoryIncomplete(RuntimeError):
    pass


@dataclass(frozen=True)
class LabelMessage:
    report_id: str
    sscc: str
    file_name: str
    text: str
    png_bytes: bytes
    source_hash: str


class DeliveryLedger:
    """Explicitly initialized SQLite ledger; production startup never creates it."""

    def __init__(self, path: str):
        if not Path(path).is_file():
            raise RuntimeError('MAX pallet-delivery ledger is missing; explicit initialization required')
        self.path = path
        with sqlite3.connect(path) as db:
            db.execute('''CREATE TABLE IF NOT EXISTS pallet_delivery_proofs (
                report_id TEXT, sscc TEXT, chat_id TEXT, bot_id TEXT NOT NULL, caption TEXT NOT NULL,
                PRIMARY KEY(report_id,sscc,chat_id))''')

            db.execute('''CREATE TABLE IF NOT EXISTS pallet_history_conflicts (
                report_id TEXT, sscc TEXT, chat_id TEXT, message_ids TEXT NOT NULL,
                PRIMARY KEY(report_id,sscc,chat_id))''')

    def check_history_conflict(self, report_id, sscc, chat_id):
        with sqlite3.connect(self.path) as db:
            if db.execute('SELECT 1 FROM pallet_history_conflicts WHERE report_id=? AND sscc=? AND chat_id=?',
                          (report_id, sscc, chat_id)).fetchone():
                raise DeliveryUncertain('Multiple GET matches require manual reconciliation')

    def saved_caption(self, item, chat_id):
        with sqlite3.connect(self.path) as db:
            row = db.execute('SELECT caption FROM pallet_delivery_proofs WHERE report_id=? AND sscc=? AND chat_id=?',
                             (item.report_id, item.sscc, chat_id)).fetchone()
            return row[0] if row else item.text

    def record_history_conflict(self, row, mids):
        with sqlite3.connect(self.path) as db:
            db.execute('BEGIN IMMEDIATE')
            key = (row['report_id'], row['sscc'], row['chat_id'])
            old = db.execute('SELECT message_ids FROM pallet_history_conflicts WHERE report_id=? AND sscc=? AND chat_id=?', key).fetchone()
            all_mids = set(mids) | (set(json.loads(old[0])) if old else set())
            db.execute('INSERT OR REPLACE INTO pallet_history_conflicts VALUES (?,?,?,?)',
                       (*key, json.dumps(sorted(all_mids))))
            # Positive delivery evidence is retained in message_id, but a newly
            # discovered conflict must override the normal CONFIRMED fast path.
            db.execute("UPDATE deliveries SET state='POST_UNCERTAIN',updated_at=? WHERE report_id=? AND sscc=? AND chat_id=?",
                       (time.time(), *key))

    @contextmanager
    def delivery_lock(self):
        import fcntl  # Delivery runs on the VPS; keep label rendering importable on Windows.
        # Serialize the complete read/GET/POST sequence across local callers.
        with open(self.path + '.lock', 'a') as lock:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(lock.fileno(), fcntl.LOCK_UN)

    def remember(self, item, chat_id, bot_id):
        self.check_history_conflict(item.report_id, item.sscc, chat_id)
        row = self.reserve(item, chat_id)
        if (row['source_hash'] not in (item.source_hash, 'legacy-uncertain')
                or row['file_name'] != item.file_name):
            raise DeliveryConflict('Report/SSCC delivery identity changed')
        with sqlite3.connect(self.path) as db:
            proof = db.execute('SELECT bot_id FROM pallet_delivery_proofs WHERE report_id=? AND sscc=? AND chat_id=?',
                               (item.report_id, item.sscc, chat_id)).fetchone()
            if proof and proof[0] != bot_id:
                raise DeliveryConflict('Delivery bot identity changed')
            # An old ambiguous attempt has no trustworthy saved caption.
            # Do not replace it with a caption from a newer renderer.
            if not proof and row['state'] in {'POST_STARTED', 'POST_UNCERTAIN', 'POSTED_UNCONFIRMED'}:
                return row
            # Freeze the reserved caption; any later pre-POST retry sends this text.
            db.execute('INSERT OR IGNORE INTO pallet_delivery_proofs VALUES (?,?,?,?,?)',
                       (item.report_id, item.sscc, chat_id, bot_id, item.text))
        return row

    def pending(self, chat_id, bot_id, *, include_confirmed=False):
        with sqlite3.connect(self.path) as db:
            db.row_factory = sqlite3.Row
            return [dict(row) for row in db.execute('''SELECT d.*,p.caption,p.bot_id FROM deliveries d
                LEFT JOIN pallet_delivery_proofs p USING(report_id,sscc,chat_id)
                WHERE d.chat_id=? AND (? OR d.state!='CONFIRMED') AND (p.bot_id=? OR p.bot_id IS NULL)''',
                (chat_id, include_confirmed, bot_id))]

    def confirm(self, row, mid):
        if not mid:
            return False
        with sqlite3.connect(self.path) as db:
            cur = db.execute('''UPDATE deliveries SET state='CONFIRMED',message_id=?,updated_at=?
                WHERE report_id=? AND sscc=? AND chat_id=? AND state=? AND source_hash=?
                AND file_sha256=? AND (message_id IS NULL OR message_id='' OR message_id=?)
                AND NOT EXISTS (SELECT 1 FROM pallet_history_conflicts c WHERE c.report_id=deliveries.report_id
                    AND c.sscc=deliveries.sscc AND c.chat_id=deliveries.chat_id)''',
                (mid, time.time(), row['report_id'], row['sscc'], row['chat_id'], row['state'],
                 row['source_hash'], row['file_sha256'], mid))
            return cur.rowcount == 1

    @classmethod
    def initialize(cls, path: str) -> 'DeliveryLedger':
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        os.close(fd)
        try:
            with sqlite3.connect(path) as db:
                db.execute('PRAGMA journal_mode=DELETE')
                db.execute('PRAGMA synchronous=EXTRA')
                db.execute('''CREATE TABLE deliveries (
                    report_id TEXT NOT NULL, sscc TEXT NOT NULL, chat_id TEXT NOT NULL,
                    source_hash TEXT NOT NULL, file_name TEXT NOT NULL, file_sha256 TEXT NOT NULL,
                    state TEXT NOT NULL, message_id TEXT, updated_at REAL NOT NULL,
                    PRIMARY KEY(report_id, sscc, chat_id))''')
                db.commit()
        except BaseException:
            os.unlink(path)
            raise
        os.chmod(path, 0o600)
        return cls(path)

    def get(self, report_id: str, sscc: str, chat_id: str) -> dict[str, Any] | None:
        with sqlite3.connect(self.path) as db:
            db.row_factory = sqlite3.Row
            row = db.execute('SELECT * FROM deliveries WHERE report_id=? AND sscc=? AND chat_id=?',
                             (report_id, sscc, chat_id)).fetchone()
            return dict(row) if row else None

    def block_legacy_attempt(self, report_id: str, sscc: str, chat_id: str,
                             file_name: str) -> None:
        """Import a prior ambiguous POST before enabling this ledger for use."""
        with sqlite3.connect(self.path) as db:
            db.execute('''INSERT OR IGNORE INTO deliveries
                (report_id,sscc,chat_id,source_hash,file_name,file_sha256,state,message_id,updated_at)
                VALUES (?,?,?,?,?,?,?,NULL,?)''',
                (report_id, sscc, chat_id, 'legacy-uncertain', file_name,
                 'legacy-uncertain', 'POST_UNCERTAIN', time.time()))
            db.commit()

    def reserve(self, message: LabelMessage, chat_id: str) -> dict[str, Any]:
        file_hash = hashlib.sha256(message.png_bytes).hexdigest()
        with sqlite3.connect(self.path, isolation_level=None) as db:
            db.execute('BEGIN IMMEDIATE')
            row = db.execute('SELECT * FROM deliveries WHERE report_id=? AND sscc=? AND chat_id=?',
                             (message.report_id, message.sscc, chat_id)).fetchone()
            if row:
                existing = dict(zip(
                    ('report_id', 'sscc', 'chat_id', 'source_hash', 'file_name',
                     'file_sha256', 'state', 'message_id', 'updated_at'), row,
                ))
                if existing['state'] in {'POST_STARTED', 'POST_UNCERTAIN', 'POSTED_UNCONFIRMED'}:
                    db.execute('COMMIT')
                    return existing
                if (existing['state'] == 'CONFIRMED' and existing['source_hash'] == message.source_hash
                        and existing['file_name'] == message.file_name):
                    db.execute('COMMIT')
                    return existing
                if (existing['source_hash'] != message.source_hash
                        or existing['file_name'] != message.file_name
                        or existing['file_sha256'] != file_hash):
                    db.execute('ROLLBACK')
                    raise DeliveryConflict('Report/SSCC delivery identity changed')
                db.execute('COMMIT')
                return existing
            now = time.time()
            db.execute('INSERT INTO deliveries VALUES (?, ?, ?, ?, ?, ?, ?, NULL, ?)',
                       (message.report_id, message.sscc, chat_id, message.source_hash,
                        message.file_name, file_hash, 'UPLOAD_PENDING', now))
            db.execute('COMMIT')
        return self.get(message.report_id, message.sscc, chat_id)

    def set_state(self, message: LabelMessage, chat_id: str, state: str, message_id: str | None = None) -> None:
        with sqlite3.connect(self.path) as db:
            db.execute('UPDATE deliveries SET state=?, message_id=COALESCE(?,message_id), updated_at=? '
                       "WHERE report_id=? AND sscc=? AND chat_id=? AND state!='CONFIRMED' "
                       "AND NOT EXISTS (SELECT 1 FROM pallet_history_conflicts c WHERE c.report_id=deliveries.report_id "
                       "AND c.sscc=deliveries.sscc AND c.chat_id=deliveries.chat_id)",
                       (state, message_id, time.time(), message.report_id, message.sscc, chat_id))
            db.commit()


class MaxPalletLabelNotifier:
    def __init__(self, *, token: str, chat_id: str, expected_bot_user_id: str,
                 ledger: DeliveryLedger, session: Any, history_start_ms: int,
                 sleep=time.sleep, history_seed=None, max_history_pages: int = 100, allow_truncated_history: bool = False,
                 retry_state_path=None, clock=time.time):
        self.clock = clock
        self.retries = RetrySchedule(ledger.path, clock=clock)
        self.api_retries = RetrySchedule(retry_state_path or ledger.path, clock=clock)
        self.api_scope = api_scope(token, chat_id)
        self._bot_verified = False
        self._history_cache = None
        self._history_cached_at = 0
        self._history_cursor = None
        self._history_pages = 0
        self._history_partial = []
        self.token = token
        self.chat_id = str(chat_id)
        self.expected_bot_user_id = str(expected_bot_user_id)
        self.ledger = ledger
        self.session = session
        self.history_start_ms = int(history_start_ms)
        self.sleep = sleep
        self.history_seed = list(history_seed or [])
        self.max_history_pages = int(max_history_pages)
        self.allow_truncated_history = allow_truncated_history is True
        if not 1 <= self.max_history_pages <= 10000:
            raise ValueError("Invalid MAX history page limit")

    def _get(self, path: str, params: dict | None = None):
        self.api_retries.check(self.api_scope)
        try:
            response = self.session.get(
                MAX_API + path, params=params,
                headers={'Authorization': self.token}, timeout=20)
        except (OSError, TimeoutError) as exc:
            raise MaxRetryLater(self.api_retries.fail(self.api_scope)) from exc
        observe_response(self.api_retries, self.api_scope, response)
        response.raise_for_status()
        try:
            payload = response.json()
        except ValueError as exc:
            raise MaxRetryLater(self.api_retries.fail(self.api_scope), 'Invalid MAX JSON; retry GET later') from exc
        if (not isinstance(payload, dict)
                or (path == '/messages' and (not isinstance(payload.get('messages'), list)
                    or any(not isinstance(m, dict) for m in payload['messages'])))):
            raise MaxRetryLater(self.api_retries.fail(self.api_scope), 'Invalid MAX history; retry GET later')
        return payload

    @staticmethod
    def _check_scope(row):
        return 'pallet-check:' + json.dumps([row['report_id'], row['sscc'], row['chat_id']])

    def reconcile_history(self, messages):
        """Every successful page is evidence for ALL pending deliveries, even if a later GET fails."""
        confirmed = []
        by_sscc = {}
        for message in messages:
            for sscc in set(re.findall(r'(?<!\d)\d{18}(?!\d)', str((message.get('body') or {}).get('text') or ''))):
                by_sscc.setdefault(sscc, []).append(message)
        for row in self.ledger.pending(self.chat_id, self.expected_bot_user_id, include_confirmed=True):
            matches = {}
            for message in by_sscc.get(row['sscc'], []):
                body = message.get('body') or {}
                mid = body.get('mid')
                text = str(body.get('text') or '')
                if (not isinstance(mid, str) or not mid
                        or str((message.get('sender') or {}).get('user_id')) != self.expected_bot_user_id
                        or str((message.get('recipient') or {}).get('chat_id')) != self.chat_id):
                    continue
                # Avoid prefix matches between different report IDs or SSCCs.
                if not all(re.search(r'(?<![\w-])' + re.escape(value) + r'(?![\w-])', text)
                           for value in (row['report_id'], row['sscc'])):
                    continue
                attachments = body.get('attachments') or []
                image = any(a.get('type') == 'image' for a in attachments)
                file = any(a.get('type') == 'file' for a in attachments)
                if row['state'] == 'POSTED_UNCONFIRMED' and not image:
                    continue
                filename = row['file_name'] in self._attachment_names(message) or row['file_name'] in text
                caption = row.get('caption') and text == row['caption']
                # Legacy rows have no saved caption. A stored POST MID plus exact
                # identity and image is independent positive delivery evidence.
                known_mid = row['message_id'] == mid and image and not row.get('caption')
                if (image or file) and (filename or (image and caption) or known_mid):
                    matches[mid] = message
            mids = set(matches)
            if row['message_id']:
                mids.add(row['message_id'])
            if len(mids) > 1:
                self.ledger.record_history_conflict(row, mids)
            elif len(matches) == 1 and row['state'] != 'CONFIRMED':
                mid = next(iter(matches))
                if self.ledger.confirm(row, mid):
                    self.retries.clear(self._check_scope(row))
                    confirmed.append(mid)
        return confirmed

    def reconcile_pending_mids(self):
        rows = [row for row in self.ledger.pending(self.chat_id, self.expected_bot_user_id)
                if row['message_id'] and not self.retries.remaining(self._check_scope(row))]
        mids = list(dict.fromkeys(row['message_id'] for row in rows))
        confirmed = []
        for offset in range(0, len(mids), MAX_MID_BATCH_SIZE):
            batch = mids[offset:offset + MAX_MID_BATCH_SIZE]
            payload = self._get('/messages', {'message_ids': ','.join(batch)})
            confirmed.extend(self.reconcile_history(payload.get('messages', [])))
            self.api_retries.success(self.api_scope)
            for row in rows:
                if row['message_id'] in batch:
                    current = self.ledger.get(row['report_id'], row['sscc'], self.chat_id)
                    if current['state'] != 'CONFIRMED':
                        self.retries.fail(self._check_scope(row))
        return confirmed

    def _history(self) -> list[dict]:
        if self._history_cache is not None and self.clock() - self._history_cached_at < 30:
            self.reconcile_history(self._history_cache)
            return self._history_cache
        self.reconcile_history(self.history_seed)
        if not self._history_partial:
            self._history_partial = list(self.history_seed)
        for _ in range(self._history_pages, self.max_history_pages):
            params = {'chat_id': self.chat_id, 'count': 100}
            if self._history_cursor is not None:
                params['from'] = self._history_cursor - 1
            payload = self._get('/messages', params)
            batch = payload.get('messages', [])
            self.reconcile_history(batch)
            self._history_partial.extend(m for m in batch if int(m.get('timestamp') or 0) >= self.history_start_ms)
            stamps = [int(m.get('timestamp') or 0) for m in batch]
            self._history_pages += 1
            if len(batch) < 100 or (stamps and min(stamps) < self.history_start_ms):
                break
            if not stamps or min(stamps) == self._history_cursor:
                raise HistoryIncomplete('MAX history pagination cursor did not advance')
            self._history_cursor = min(stamps)
        else:
            if not self.allow_truncated_history:
                raise HistoryIncomplete('MAX history page limit exceeded; delivery is held')
        self.api_retries.success(self.api_scope)
        self._history_cache = self._history_partial
        self._history_cached_at = self.clock()
        self._history_partial, self._history_cursor, self._history_pages = [], None, 0
        return self._history_cache

    @staticmethod
    def _attachment_names(message: dict) -> list[str]:
        names = []
        for attachment in (message.get('body') or {}).get('attachments', []):
            payload = attachment.get('payload') or {}
            names.extend(str(payload[key]) for key in ('name', 'filename') if payload.get(key))
        return names

    def _exact_message(self, history: list[dict], item: LabelMessage, *, image_only: bool = False) -> dict | None:
        matches = {}
        for message in history:
            sender = message.get('sender') or {}
            recipient = message.get('recipient') or {}
            body = message.get('body') or {}
            text = str(body.get('text') or '')
            if (str(sender.get('user_id')) != self.expected_bot_user_id
                    or str(recipient.get('chat_id')) != self.chat_id):
                continue
            attachments = body.get('attachments') or []
            allowed_types = {'image'} if image_only else {'image', 'file'}
            has_attachment = any(a.get('type') in allowed_types for a in attachments)
            filename_proven = (item.file_name in self._attachment_names(message)
                               or item.file_name in text)
            # MAX images may omit the upload filename. The complete caption
            # identifies the validated report/pallet without exposing that name.
            caption_proven = (text == item.text
                              and any(a.get('type') == 'image' for a in attachments))
            if (has_attachment and (filename_proven or caption_proven)
                    and all(re.search(r'(?<![\w-])' + re.escape(value) + r'(?![\w-])', text)
                            for value in (item.report_id, item.sscc))):
                mid = body.get('mid')
                if isinstance(mid, str) and mid:
                    matches[mid] = message
        return next(iter(matches.values())) if len(matches) == 1 else None

    def _verify_bot(self) -> None:
        if self._bot_verified:
            return
        me = self._get('/me')
        if str(me.get('user_id')) != self.expected_bot_user_id:
            raise DeliveryConflict('Configured MAX token belongs to a different bot')
        self._bot_verified = True

    def deliver(self, item: LabelMessage) -> dict[str, Any]:
        with self.ledger.delivery_lock():
            return self._deliver(item)

    def _deliver(self, item: LabelMessage) -> dict[str, Any]:
        if not item.png_bytes or not item.report_id or not item.sscc or not item.file_name.endswith('.png'):
            raise ValueError('Validated report ID, SSCC, filename, and PNG bytes are required')
        row = self.ledger.remember(item, self.chat_id, self.expected_bot_user_id)
        item = replace(item, text=self.ledger.saved_caption(item, self.chat_id))
        if row['state'] == 'CONFIRMED':
            return {'state': 'ALREADY_CONFIRMED_BY_LEDGER', 'message_id': row.get('message_id')}
        # Positive evidence does not expire with the preflight cache and must
        # be usable even while this row or the API is waiting for its deadline.
        evidence = list(self.history_seed) + list(self._history_cache or [])
        self.reconcile_history(evidence)
        self.ledger.check_history_conflict(item.report_id, item.sscc, self.chat_id)
        row = self.ledger.get(item.report_id, item.sscc, self.chat_id)
        if row['state'] == 'CONFIRMED':
            match = self._exact_message(evidence, item)
            is_file = match and not any(a.get('type') == 'image'
                                       for a in match['body'].get('attachments', []))
            return {'state': 'LEGACY_FILE_CONFIRMED_BY_GET' if is_file else 'ALREADY_CONFIRMED_BY_GET',
                    'message_id': row.get('message_id')}
        self._verify_bot()
        self.reconcile_pending_mids()
        self.ledger.check_history_conflict(item.report_id, item.sscc, self.chat_id)
        row = self.ledger.get(item.report_id, item.sscc, self.chat_id)
        if row['state'] == 'CONFIRMED':
            return {'state': 'ALREADY_CONFIRMED_BY_GET', 'message_id': row.get('message_id')}
        if row['message_id'] or self.retries.remaining(self._check_scope(row)):
            raise DeliveryRecheck(self.retries.remaining(self._check_scope(row)) or 5,
                                  'Prior message POST is ambiguous; GET only, never auto-repeat')
        history = self._history()
        self.ledger.check_history_conflict(item.report_id, item.sscc, self.chat_id)
        existing_message = self._exact_message(history, item, image_only=row['state'] == 'POSTED_UNCONFIRMED')
        if existing_message:
            self.ledger.reserve(item, self.chat_id)
            self.ledger.set_state(item, self.chat_id, 'CONFIRMED', str((existing_message.get('body') or {}).get('mid') or ''))
            is_image = any(a.get('type') == 'image'
                           for a in existing_message['body'].get('attachments', []))
            kind = 'ALREADY_CONFIRMED_BY_GET' if is_image else 'LEGACY_FILE_CONFIRMED_BY_GET'
            return {'state': kind, 'message_id': (existing_message.get('body') or {}).get('mid')}

        for candidate in history:
            sender = candidate.get('sender') or {}
            recipient = candidate.get('recipient') or {}
            body = candidate.get('body') or {}
            text = str(body.get('text') or '')
            if (str(sender.get('user_id')) == self.expected_bot_user_id
                    and str(recipient.get('chat_id')) == self.chat_id
                    and item.report_id in text and item.sscc in text
                    and body.get('attachments')):
                self.ledger.reserve(item, self.chat_id)
                self.ledger.set_state(item, self.chat_id, 'POST_UNCERTAIN')
                raise DeliveryUncertain('Prior attachment candidate lacks exact filename or caption proof; GET reconciliation required')

        row = self.ledger.reserve(item, self.chat_id)
        if row['state'] == 'CONFIRMED':
            return {'state': 'ALREADY_CONFIRMED_BY_LEDGER', 'message_id': row.get('message_id')}
        if row['state'] in {'POST_STARTED', 'POST_UNCERTAIN', 'POSTED_UNCONFIRMED'}:
            raise DeliveryRecheck(self.retries.fail(self._check_scope(row)),
                                  'Prior message POST is ambiguous; GET only, never auto-repeat')

        caption = item.text
        if len(caption) > 4000:
            raise ValueError('Complete pallet caption exceeds MAX 4000-character limit')

        self.api_retries.check(self.api_scope)
        upload = self.session.post(
            MAX_API + '/uploads', params={'type': 'image'},
            headers={'Authorization': self.token}, timeout=20,
        )
        observe_response(self.api_retries, self.api_scope, upload)
        upload.raise_for_status()
        try:
            upload_init = upload.json()
        except ValueError:
            raise DeliveryUncertain('MAX image upload init returned invalid JSON') from None
        if not isinstance(upload_init, dict) or upload_init.get('code'):
            raise DeliveryUncertain('MAX image upload init returned an error')
        upload_url = upload_init.get('url')
        if not isinstance(upload_url, str) or not upload_url.strip():
            raise DeliveryUncertain('MAX upload init returned no URL')
        uploaded = self.session.post(
            upload_url, headers={'Authorization': self.token},
            files={'data': (item.file_name, item.png_bytes, 'image/png')}, timeout=60,
        )
        observe_response(self.api_retries, self.api_scope, uploaded)
        uploaded.raise_for_status()
        try:
            upload_data = uploaded.json()
        except ValueError:
            raise DeliveryUncertain('MAX image upload returned invalid JSON') from None
        if not isinstance(upload_data, dict) or upload_data.get('code'):
            raise DeliveryUncertain('MAX image upload returned an error')
        photos = upload_data.get('photos')
        tokens = [photo.get('token') for photo in photos.values()
                  if isinstance(photo, dict) and isinstance(photo.get('token'), str)
                  and photo['token'].strip()] if isinstance(photos, dict) else []
        if len(tokens) != 1:
            raise DeliveryUncertain('MAX image upload must return exactly one image token')
        file_token = tokens[0]
        self.ledger.set_state(item, self.chat_id, 'FILE_UPLOADED')

        body = {
            'text': caption,
            'attachments': [{'type': 'image', 'payload': {'token': file_token}}],
            'notify': True,
        }
        self.api_retries.check(self.api_scope)
        self.ledger.set_state(item, self.chat_id, 'POST_STARTED')
        try:
            for attempt in range(10):
                response = self.session.post(
                    MAX_API + '/messages', params={'chat_id': self.chat_id},
                    json=body, headers={'Authorization': self.token, 'Content-Type': 'application/json'},
                    timeout=30,
                )
                if response.status_code == 200:
                    message_id = (response.json().get('message') or {}).get('body', {}).get('mid')
                    self.ledger.set_state(item, self.chat_id, 'POSTED_UNCONFIRMED', str(message_id or ''))
                    break
                try:
                    error = response.json()
                except ValueError:
                    error = {}
                if error.get('code') == 'attachment.not.ready':
                    self.sleep((attempt + 1) * 2)
                    continue
                self.ledger.set_state(item, self.chat_id, 'POST_UNCERTAIN')
                observe_response(self.api_retries, self.api_scope, response)
                raise DeliveryRecheck(5, f'MAX rejected message POST with HTTP {response.status_code}; reconcile by GET')
            else:
                self.ledger.set_state(item, self.chat_id, 'POST_UNCERTAIN')
                raise DeliveryUncertain('MAX attachment was never ready; reconcile by GET')
        except (DeliveryUncertain, MaxRetryLater):
            self._history_cache = None
            raise
        except Exception as exc:
            self._history_cache = None
            self.ledger.set_state(item, self.chat_id, 'POST_UNCERTAIN')
            raise DeliveryRecheck(5, 'Message POST outcome is ambiguous; reconcile by GET') from exc

        # Known MIDs never require a full history walk after POST.
        self.ledger.check_history_conflict(item.report_id, item.sscc, self.chat_id)
        row = self.ledger.get(item.report_id, item.sscc, self.chat_id)
        if row['message_id']:
            self.reconcile_pending_mids()
        else:
            self._history_cache = None
            self._history()
        self.ledger.check_history_conflict(item.report_id, item.sscc, self.chat_id)
        row = self.ledger.get(item.report_id, item.sscc, self.chat_id)
        if row['state'] != 'CONFIRMED':
            raise DeliveryRecheck(self.retries.remaining(self._check_scope(row)) or self.retries.fail(self._check_scope(row)),
                                  'MAX returned success but exact attachment is absent in GET history')
        return {'state': 'CONFIRMED', 'message_id': row['message_id']}
