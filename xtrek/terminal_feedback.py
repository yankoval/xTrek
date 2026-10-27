"""Transport-independent terminal events and single-host, prefork-safe outbox.

Only allowlisted diagnostic facts belong in events; never raw API payloads.
The SQLite file is provisioned explicitly, on a persistent local filesystem.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import unicodedata
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
    'report_repeat_scan': 'Все SSCC и вложения уже в обороте; зарегистрированный состав совпадает. Повторное сканирование по ошибке.',
    'report_partial_repeat': 'Часть коробов и все их вложения уже в обороте; состав совпадает. Частичное повторное сканирование.',
    'report_scan_conflict': 'Состояния или состав SSCC и вложений не позволяют подтвердить безопасное повторное сканирование.',
    'suz_expiration_date_rejected': 'СУЗ отклонил нанесение: HTTP 400, код 7745. Срок годности (attributes.expirationDate) раньше текущей даты. Проверить дату в задании и согласовать исправление.',
    'suz_production_date_range_rejected': 'СУЗ отклонил нанесение: HTTP 400, код 7570. Дата производства (attributes.productionDate) вне разрешённого диапазона 1970-01-01 — 2100-01-01. Сверить фактическую дату производства и исправить её в исходном задании; проверить год и формат YYYY-MM-DD. Дата должна попадать в разрешённый диапазон и не быть позже текущей. Повторную отправку согласовать отдельно.',
    'suz_production_date_future_rejected': 'СУЗ отклонил нанесение: HTTP 400, код 7580. Дата производства (attributes.productionDate) позже текущей даты. Сверить фактическую дату производства и исправить её в исходном задании; проверить год, месяц и день. Не подставлять текущую дату вместо фактической. Повторную отправку согласовать отдельно.',
    'report_status_rejected': 'Коды находятся в недопустимом состоянии. Сверить их состояние в ЧЗ и фактический состав отчёта.',
    'report_aggregate_conflict': 'Короб или паллета уже зарегистрированы. Сверить SSCC и вложения с фактической упаковкой.',
    'report_access_denied': 'ЧЗ отказал в доступе к проверке отчёта (HTTP 403). Проверить полномочия организации.',
    'introduction_rejected': 'ЧЗ отклонил ввод в оборот. Проверить ошибки принятого документа и исправить исходные данные.',
    'utilisation_rejected': 'СУЗ отклонил принятый отчёт о нанесении. Проверить ошибки отчёта и исправить исходные данные.',
    'cis_rejected': 'Проверить отклонённый документ корректировки в ЧЗ; согласовать исправление.',
    'aggregation_rejected': 'Проверить состав и статус агрегации в ЧЗ; согласовать исправление.',
    'invalid_report': 'Проверить исходный отчёт и сканирование; подготовить корректные исходные данные.',
    'empty_report': 'Отчёт пуст или содержит неполные данные сканирования: отсутствуют считанные короба или коды маркировки. Повторно выгрузите задание и отсканируйте продукцию заново.',
    'retry_exhausted': 'Проверить доступность ресурсов и статус принятого документа; согласовать продолжение.',
    'reconciliation_required': 'Сверить исходный документ и журнал операций; согласовать дальнейшее действие.',
}


class TerminalBusinessError(ReconciliationRequired):
    category = 'reconciliation_required'


class WorkflowPending(RuntimeError):
    """Unconfirmed progress; continue durable polling without a terminal fence."""


class TerminalRecorded(ReconciliationRequired):
    """The event is durable; never execute this business route again automatically."""


def classify(error, *, retries, retry_limit):
    from .suz_rejection import SuzUtilisationRejected
    if isinstance(error, (TerminalBusinessError, SuzUtilisationRejected)):
        return error.category
    if isinstance(error, (ReconciliationRequired, OperationConflict)):
        return 'reconciliation_required'
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


STAGE_LABELS = {
    'report-validation': 'Проверка отчёта оборудования',
    'disaggregation-validation': 'Проверка отчёта расформирования',
    'reaggregation-validation': 'Проверка отчёта изъятия',
    'cis-submit': 'Отправка корректировки кодов', 'cis-status': 'Проверка корректировки кодов',
    'aggregation-status': 'Транспортная агрегация', 'set-aggregation-status': 'Агрегация в набор',
    'utilisation-status': 'Нанесение кодов', 'introduce-status': 'Ввод в оборот',
    'disaggregation-submit': 'Отправка расформирования', 'disaggregation-status': 'Расформирование',
    'reaggregation-submit': 'Отправка изъятия', 'reaggregation-status': 'Изъятие из агрегата',
}


def display_value(value, limit=256):
    # MAX receives plain text (no format/HTML). Preserve Unicode and slashes;
    # escape line/control characters so source data cannot forge message fields.
    if not isinstance(value, (str, int)) or isinstance(value, bool) or not str(value).strip():
        return 'не указан'
    value = str(value)
    if (re.search(r'01\d{14}21', value) or '\x1d' in value
            or re.search(r'(?i)(token|bearer|secret|password)[\s:=_-]', value)):
        return '[скрыто: чувствительные данные]'
    escaped = ''.join(c if unicodedata.category(c)[0] != 'C' and c not in '\r\n'
                      else '\\u%04x' % ord(c) for c in value)
    # Report IDs use the S3 object-key bound and are never silently shortened.
    if len(escaped) > limit:
        if limit == 1500:
            raise ValueError('Feedback report ID exceeds safe message limit')
        escaped = escaped[:limit - 1] + '…'
    return escaped


def message_text(row):
    from .terminal_feedback_messages import compact_message
    return compact_message(row)


class TerminalLedger:
    def __init__(self, path):
        if not Path(path).is_file():
            raise RuntimeError('Terminal ledger requires explicit initialization')
        self.path = str(Path(path).resolve())
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            if db.execute('PRAGMA user_version').fetchone()[0] != 1:
                raise RuntimeError('Unsupported terminal ledger schema')
            if 'context_json' not in {r[1] for r in db.execute('PRAGMA table_info(terminal_events)')}:
                db.execute('ALTER TABLE terminal_events ADD COLUMN context_json TEXT')
            if 'facts_json' not in {r[1] for r in db.execute('PRAGMA table_info(terminal_events)')}:
                db.execute('ALTER TABLE terminal_events ADD COLUMN facts_json TEXT')
            if 'attachment_json' not in {r[1] for r in db.execute('PRAGMA table_info(terminal_events)')}:
                db.execute('ALTER TABLE terminal_events ADD COLUMN attachment_json TEXT')

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

    def record(self, bucket, key, stage, category, *, facts=None):
        if category not in ACTIONS or stage not in STAGES.values():
            raise ValueError('Unknown terminal classification')
        from .terminal_feedback_facts import validated_facts
        facts = validated_facts(facts)
        event_id = event_identity(bucket, key)
        with self.connect() as db:
            db.execute('''INSERT OR IGNORE INTO terminal_events
                (event_id,bucket,source_key,stage,category,terminal_state,delivery_state,created_at,facts_json)
                VALUES (?,?,?,?,?,'RECONCILIATION_REQUIRED','ROUTE_PENDING',?,?)''',
                       (event_id, bucket, key, stage, category, time.time(), json.dumps(facts) if facts else None))
        return self.get(event_id)

    def pending(self):
        with self.connect() as db:
            return [dict(row) for row in db.execute(
                "SELECT * FROM terminal_events WHERE delivery_state != 'CONFIRMED' ORDER BY created_at")]

    def bind_route(self, event_id, production_order_id, operator, route, context=None):
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
            row['context_json'] = row.get('context_json') or json.dumps(context or {}, ensure_ascii=False)
            from .terminal_feedback_messages import make_attachment
            attachment = make_attachment(row) if route else None
            body = message_text(row) if route else None
            db.execute('''UPDATE terminal_events SET production_order_id=?,operator_raw=?,
                route_json=?,message_text=?,delivery_state=?,context_json=?,attachment_json=? WHERE event_id=?''',
                (production_order_id, json.dumps(operator, ensure_ascii=False),
                 json.dumps(route, sort_keys=True) if route else None, body,
                 'READY' if route else 'ROUTE_HELD', row['context_json'],
                 json.dumps(attachment, ensure_ascii=False) if attachment else None, event_id))
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
