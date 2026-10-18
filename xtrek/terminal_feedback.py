"""Transport-independent terminal events and single-host, prefork-safe outbox.

No API payloads, exception strings or codes belong in this module's events.
The SQLite file is provisioned explicitly, on a persistent local filesystem.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
from pathlib import Path
import sqlite3
import time

from .operation_state import OperationConflict, ReconciliationRequired


STAGES = {
    'equipment-reports': 'report-validation',
    'equipment-reports-disaggregation': 'disaggregation-validation',
    'equipment-reports-reaggregation-removing': 'reaggregation-validation',
    'cisInformationChangeTasks': 'cis-submit',
    'cisInformationChangeReceipts': 'cis-status',
    'aggReceipts': 'aggregation-status',
    'aggSetReceipts': 'set-aggregation-status',
    'utilisationReceipts': 'utilisation-status',
    'introduceReceipts': 'introduce-status',
    'disaggregationTasks': 'disaggregation-submit',
    'disaggregationReceipts': 'disaggregation-status',
    'reaggregationTasks': 'reaggregation-submit',
    'reaggregationReceipts': 'reaggregation-status',
}
ACTIONS = {
    'suz_expiration_date_rejected': 'СУЗ отклонил нанесение: HTTP 400, код 7745. Срок годности (attributes.expirationDate) раньше текущей даты. Проверить дату в задании и согласовать исправление.',
    'cis_rejected': 'Проверить отклонённый документ корректировки в ЧЗ; согласовать исправление.',
    'aggregation_rejected': 'Проверить состав и статус агрегации в ЧЗ; согласовать исправление.',
    'invalid_report': 'Проверить исходный отчёт и сканирование; подготовить корректные исходные данные.',
    'retry_exhausted': 'Проверить доступность ресурсов и статус принятого документа; согласовать продолжение.',
    'reconciliation_required': 'Сверить исходный документ и журнал операций; согласовать дальнейшее действие.',
}


class TerminalBusinessError(ReconciliationRequired):
    category = 'reconciliation_required'


class TerminalRecorded(ReconciliationRequired):
    """The event is durable; never execute this business route again automatically."""


def classify(error, *, retries, retry_limit):
    from .suz_rejection import SuzUtilisationRejected
    if isinstance(error, (TerminalBusinessError, SuzUtilisationRejected)):
        return error.category
    if isinstance(error, (ReconciliationRequired, OperationConflict)):
        return 'reconciliation_required'
    if retries >= retry_limit:
        return 'retry_exhausted'
    return None


def event_identity(bucket, key):
    return hashlib.sha256(json.dumps(['equipment-terminal-v1', bucket, key],
                                    ensure_ascii=False, separators=(',', ':')).encode()).hexdigest()


def source_stage(key):
    parts = key.split('/')
    if len(parts) != 2 or not parts[1].endswith('.json') or parts[1] == '.json':
        return None
    return STAGES.get(parts[0])


def normalize_operator(value):
    # Exact Unicode/code-point identity. No trim, casefold, NFC, aliases or fuzzy matching.
    # Whitespace-only strings are invalid, but nonempty strings retain all whitespace.
    return value if isinstance(value, str) and value.strip() else None


class RouteConfigurationError(ValueError):
    """Safe operational diagnostic; the event must remain pending."""


def select_route(settings, operator):
    value = normalize_operator(operator)
    name = settings['operator_routes'].get(value, '$default') if value is not None else '$default'
    kind = 'operator'
    if name == '$default':
        name = settings.get('default_route')
        kind = 'default'
    route = settings['routes'].get(name) if isinstance(name, str) and name else None
    if (not isinstance(route, dict) or route.get('transport') != 'max'
            or not all(isinstance(route.get(k), str) and route[k].strip()
                       for k in ('chat_id', 'bot_user_id', 'token_env'))):
        raise RouteConfigurationError('Invalid terminal feedback ' + kind + ' route')
    return dict(route)


def message_text(row):
    # Untrusted identifiers/operators/errors never enter the outbound body.
    # Full SHA-256 references resolve unambiguously through the local audit ledger.
    po_ref = hashlib.sha256(row['production_order_id'].encode()).hexdigest()
    uuid = re.search(r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$',
                     row['production_order_id'])
    task_line = f"UUID задания: {uuid.group()}\n" if uuid else ''
    return (f"xTrek: обработка остановлена, требуется сверка.\n{task_line}"
            f"productionOrder ref: {po_ref}\n"
            f"Стадия: {row['stage']}\nКатегория: {row['category']}\n"
            f"Действие: {ACTIONS[row['category']]}\n"
            f"Автоматическая повторная отправка бизнес-документа запрещена.\n"
            f"Событие: {row['event_id']}")


class TerminalLedger:
    def __init__(self, path):
        if not Path(path).is_file():
            raise RuntimeError('Terminal ledger requires explicit initialization')
        self.path = str(Path(path).resolve())
        with self.connect() as db:
            if db.execute('PRAGMA user_version').fetchone()[0] != 1:
                raise RuntimeError('Unsupported terminal ledger schema')

    def connect(self):
        # mode=rw prevents silently replacing a missing ledger after rollback/mount loss.
        from urllib.parse import quote
        db = sqlite3.connect('file:' + quote(self.path) + '?mode=rw', uri=True, timeout=30)
        db.row_factory = sqlite3.Row
        db.execute('PRAGMA synchronous=EXTRA')
        return db

    @classmethod
    def initialize(cls, path):
        path = str(Path(path).resolve())
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        os.close(fd)
        with sqlite3.connect(path) as db:
            db.execute('PRAGMA synchronous=EXTRA')
            db.execute('''CREATE TABLE terminal_events (
                event_id TEXT PRIMARY KEY, bucket TEXT NOT NULL, source_key TEXT NOT NULL,
                stage TEXT NOT NULL, category TEXT NOT NULL, terminal_state TEXT NOT NULL,
                delivery_state TEXT NOT NULL, production_order_id TEXT,
                operator_raw TEXT, route_json TEXT, message_text TEXT,
                created_at REAL NOT NULL, message_id TEXT)''')
            db.execute('PRAGMA user_version=1')
        return cls(path)

    def get(self, event_id):
        with self.connect() as db:
            row = db.execute('SELECT * FROM terminal_events WHERE event_id=?', (event_id,)).fetchone()
            return dict(row) if row else None

    def record(self, bucket, key, stage, category):
        if category not in ACTIONS or stage not in STAGES.values():
            raise ValueError('Unknown terminal classification')
        event_id = event_identity(bucket, key)
        with self.connect() as db:
            db.execute('''INSERT OR IGNORE INTO terminal_events
                (event_id,bucket,source_key,stage,category,terminal_state,delivery_state,created_at)
                VALUES (?,?,?,?,?,'RECONCILIATION_REQUIRED','ROUTE_PENDING',?)''',
                       (event_id, bucket, key, stage, category, time.time()))
        return self.get(event_id)

    def pending(self):
        with self.connect() as db:
            return [dict(row) for row in db.execute(
                "SELECT * FROM terminal_events WHERE delivery_state != 'CONFIRMED' ORDER BY created_at")]

    def bind_route(self, event_id, production_order_id, operator, route):
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            row = dict(db.execute('SELECT * FROM terminal_events WHERE event_id=?', (event_id,)).fetchone())
            if row['route_json'] is not None:
                return row  # Frozen destination/body survive configuration/source changes.
            if (row['production_order_id'] is not None
                    and (row['production_order_id'] != production_order_id
                         or row['operator_raw'] != json.dumps(operator, ensure_ascii=False))):
                return row  # A concurrent resolver cannot replace the first audit snapshot.
            row.update(production_order_id=production_order_id)
            body = message_text(row) if route else None
            db.execute('''UPDATE terminal_events SET production_order_id=?,operator_raw=?,
                route_json=?,message_text=?,delivery_state=? WHERE event_id=?''',
                (production_order_id, json.dumps(operator, ensure_ascii=False),
                 json.dumps(route, sort_keys=True) if route else None, body,
                 'READY' if route else 'ROUTE_HELD', event_id))
        return self.get(event_id)

    def claim_post(self, event_id):
        # Commit reservation BEFORE POST; only one prefork process can win.
        with self.connect() as db:
            return db.execute("UPDATE terminal_events SET delivery_state='POST_STARTED' "
                              "WHERE event_id=? AND delivery_state='READY'", (event_id,)).rowcount == 1

    def confirmed(self, event_id, message_id):
        if not message_id:
            raise ValueError('GET must prove a message ID')
        with self.connect() as db:
            db.execute("UPDATE terminal_events SET delivery_state='CONFIRMED',message_id=? WHERE event_id=?",
                       (message_id, event_id))
