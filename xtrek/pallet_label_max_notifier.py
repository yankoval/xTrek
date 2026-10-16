"""Idempotent MAX PNG delivery for validated equipment-report pallets.

The caller must validate report/task identity and render the PNG first. This
module enforces bot/chat identity, complete-history preflight, and no retry of an
ambiguous message POST. It is not run automatically by the PRNSRV generator.
"""
from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlencode


MAX_API = 'https://platform-api2.max.ru'


class DeliveryConflict(RuntimeError):
    pass


class DeliveryUncertain(RuntimeError):
    pass


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
                       'WHERE report_id=? AND sscc=? AND chat_id=?',
                       (state, message_id, time.time(), message.report_id, message.sscc, chat_id))
            db.commit()


class MaxPalletLabelNotifier:
    def __init__(self, *, token: str, chat_id: str, expected_bot_user_id: str,
                 ledger: DeliveryLedger, session: Any, history_start_ms: int,
                 sleep=time.sleep, history_seed=None, max_history_pages: int = 100, allow_truncated_history: bool = False):
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
        response = self.session.get(
            MAX_API + path,
            params=params,
            headers={'Authorization': self.token},
            timeout=20,
        )
        response.raise_for_status()
        return response.json()

    def _history(self) -> list[dict]:
        messages = list(self.history_seed)
        cursor = None
        for _ in range(self.max_history_pages):
            params = {'chat_id': self.chat_id, 'count': 100}
            if cursor is not None:
                params['from'] = cursor - 1
            payload = self._get('/messages', params)
            batch = payload.get('messages', [])
            messages.extend(m for m in batch if int(m.get('timestamp') or 0) >= self.history_start_ms)
            stamps = [int(m.get('timestamp') or 0) for m in batch]
            if len(batch) < 100 or (stamps and min(stamps) < self.history_start_ms):
                return messages
            if not stamps or min(stamps) == cursor:
                raise HistoryIncomplete('MAX history pagination cursor did not advance')
            cursor = min(stamps)
        if self.allow_truncated_history:
            return messages
        raise HistoryIncomplete('MAX history page limit exceeded; delivery is held')

    @staticmethod
    def _attachment_names(message: dict) -> list[str]:
        names = []
        for attachment in (message.get('body') or {}).get('attachments', []):
            payload = attachment.get('payload') or {}
            names.extend(str(payload[key]) for key in ('name', 'filename') if payload.get(key))
        return names

    def _exact_message(self, history: list[dict], item: LabelMessage, *, image_only: bool = False) -> dict | None:
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
            if (has_attachment and filename_proven and item.report_id in text and item.sscc in text):
                return message
        return None

    def _verify_bot(self) -> None:
        me = self._get('/me')
        if str(me.get('user_id')) != self.expected_bot_user_id:
            raise DeliveryConflict('Configured MAX token belongs to a different bot')

    def deliver(self, item: LabelMessage) -> dict[str, Any]:
        if not item.png_bytes or not item.report_id or not item.sscc or not item.file_name.endswith('.png'):
            raise ValueError('Validated report ID, SSCC, filename, and PNG bytes are required')
        self._verify_bot()
        history = self._history()
        existing_message = self._exact_message(history, item)
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
                raise DeliveryUncertain('Prior attachment candidate lacks exact filename proof; GET reconciliation required')

        row = self.ledger.reserve(item, self.chat_id)
        if row['state'] == 'CONFIRMED':
            return {'state': 'ALREADY_CONFIRMED_BY_LEDGER', 'message_id': row.get('message_id')}
        if row['state'] in {'POST_STARTED', 'POST_UNCERTAIN', 'POSTED_UNCONFIRMED'}:
            self.ledger.set_state(item, self.chat_id, 'POST_UNCERTAIN')
            raise DeliveryUncertain('Prior message POST is ambiguous; GET only, never auto-repeat')

        caption = f'{item.text}\nФайл: {item.file_name}'
        if len(caption) > 4000:
            raise ValueError('Complete pallet caption exceeds MAX 4000-character limit')

        upload = self.session.post(
            MAX_API + '/uploads', params={'type': 'image'},
            headers={'Authorization': self.token}, timeout=20,
        )
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
            # GET schemas do not promise that image attachments retain their
            # original upload filename, so include it in message text too.
            'text': caption,
            'attachments': [{'type': 'image', 'payload': {'token': file_token}}],
            'notify': True,
        }
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
                raise DeliveryUncertain(f'MAX rejected message POST with HTTP {response.status_code}; reconcile by GET')
            else:
                self.ledger.set_state(item, self.chat_id, 'POST_UNCERTAIN')
                raise DeliveryUncertain('MAX attachment was never ready; reconcile by GET')
        except DeliveryUncertain:
            raise
        except Exception as exc:
            self.ledger.set_state(item, self.chat_id, 'POST_UNCERTAIN')
            raise DeliveryUncertain('Message POST outcome is ambiguous; reconcile by GET') from exc

        # Confirm the exact sender, recipient, report, SSCC, and filename by GET.
        confirmed = self._exact_message(self._history(), item, image_only=True)
        if not confirmed:
            raise DeliveryUncertain('MAX returned success but exact attachment is absent in GET history')
        message_id = (confirmed.get('body') or {}).get('mid')
        self.ledger.set_state(item, self.chat_id, 'CONFIRMED', str(message_id or ''))
        return {'state': 'CONFIRMED', 'message_id': message_id}
