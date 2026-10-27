"""Shared terminal fence and outbox, using the existing storage conditional PUT.

Every worker uses the same root. POST reservation never expires; even sender
failover may only reconcile an ambiguous attempt through GET, never resend it.
No business object or operation guard is modified by this ledger.
"""
import json
import re
import time
from urllib.parse import urlparse

from .operation_state import OperationBusy
from .terminal_feedback import ACTIONS, STAGES, event_identity, message_text


class S3TerminalLedger:
    def __init__(self, storage, root):
        parsed = urlparse(root)
        if (parsed.scheme != 's3' or not parsed.netloc or not parsed.path.strip('/')
                or parsed.query or parsed.fragment):
            raise ValueError('Shared feedback ledger requires an explicit S3 prefix')
        self.storage = storage
        self.root = root.rstrip('/')

    def _path(self, event_id):
        if not re.fullmatch(r'[0-9a-f]{64}', event_id):
            raise ValueError('Invalid terminal event identity')
        return self.root + '/' + event_id + '.json'

    def _read(self, event_id):
        stored = self.storage.read_lock_object(self._path(event_id))
        if stored is None:
            return None, None
        row = json.loads(stored[0])
        if (row.get('version') != 1 or row.get('event_id') != event_id
                or event_identity(row['bucket'], row['source_key']) != event_id
                or row.get('terminal_state') != 'RECONCILIATION_REQUIRED'
                or row.get('delivery_state') not in
                {'ROUTE_PENDING', 'ROUTE_HELD', 'READY', 'POST_STARTED', 'CONFIRMED'}):
            raise ValueError('Invalid shared terminal event; processing held')
        return row, stored[1]

    def get(self, event_id):
        return self._read(event_id)[0]

    def _update(self, event_id, change):
        for _ in range(8):
            row, etag = self._read(event_id)
            if row is None:
                raise ValueError('Shared terminal event missing')
            updated = change(dict(row))
            if updated == row:
                return row
            if self.storage.write_lock_object(self._path(event_id), json.dumps(updated), etag) is not None:
                return updated
        raise OperationBusy('Shared terminal event changed during CAS')

    def record(self, bucket, key, stage, category, *, facts=None):
        if category not in ACTIONS or stage not in STAGES.values():
            raise ValueError('Unknown terminal classification')
        from .terminal_feedback_facts import validated_facts
        facts = validated_facts(facts)
        event_id = event_identity(bucket, key)
        row = dict(version=1, event_id=event_id, bucket=bucket, source_key=key,
                   stage=stage, category=category, terminal_state='RECONCILIATION_REQUIRED',
                   delivery_state='ROUTE_PENDING', production_order_id=None, operator_raw=None,
                   route_json=None, message_text=None, created_at=time.time(), message_id=None,
                   facts_json=json.dumps(facts) if facts else None)
        self.storage.write_lock_object(self._path(event_id), json.dumps(row), None)
        return self.get(event_id)

    def pending(self):
        rows = []
        for path in self.storage.list_files(self.root, '*.json', include_processed=True, required=True):
            event_id = path.removeprefix(self.root + '/').removesuffix('.json')
            row = self.get(event_id)
            if row is None:
                raise ValueError('Shared terminal event disappeared')
            if row['delivery_state'] != 'CONFIRMED':
                rows.append(row)
        return sorted(rows, key=lambda row: row['created_at'])

    def bind_route(self, event_id, production_order_id, operator, route, context=None):
        def change(row):
            if row['route_json'] is not None:
                return row
            raw = json.dumps(operator, ensure_ascii=False)
            if row['production_order_id'] is not None and (
                    row['production_order_id'] != production_order_id or row['operator_raw'] != raw):
                return row
            row['context_json'] = row.get('context_json') or json.dumps(context or {}, ensure_ascii=False)
            row.update(production_order_id=production_order_id, operator_raw=raw,
                       route_json=json.dumps(route, sort_keys=True) if route else None,
                       delivery_state='READY' if route else 'ROUTE_HELD')
            from .terminal_feedback_messages import make_attachment
            attachment = make_attachment(row) if route else None
            row['attachment_json'] = json.dumps(attachment, ensure_ascii=False) if attachment else None
            row['message_text'] = message_text(row) if route else None
            return row
        return self._update(event_id, change)

    def claim_post(self, event_id):
        row, etag = self._read(event_id)
        if row is None or row['delivery_state'] != 'READY':
            return False
        row['delivery_state'] = 'POST_STARTED'
        return self.storage.write_lock_object(self._path(event_id), json.dumps(row), etag) is not None

    def confirmed(self, event_id, message_id):
        if not message_id:
            raise ValueError('GET must prove a message ID')
        def change(row):
            if row['delivery_state'] == 'CONFIRMED':
                if row['message_id'] != message_id:
                    raise ValueError('Conflicting GET confirmation')
                return row
            row.update(delivery_state='CONFIRMED', message_id=message_id)
            return row
        self._update(event_id, change)
