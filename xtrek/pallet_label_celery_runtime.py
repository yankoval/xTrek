"""VPS Celery entrypoint for validated pallet PNG delivery, with no business POST."""
from __future__ import annotations

import hashlib
import json
import os
import re
import shlex
from pathlib import Path
from datetime import datetime

from .pallet_label_pipeline import prepare_report_label_messages
from .pallet_label_max_notifier import DeliveryLedger, MaxPalletLabelNotifier

PROTECTED_UUID = '623c20fa-f90c-4478-aeae-9c6635cedc9f'


class SnapshotStorage:
    def __init__(self, objects):
        self.objects = objects

    def exists(self, key):
        return key in self.objects

    def read_text(self, key):
        return json.dumps(self.objects[key], ensure_ascii=False)


def read_snapshot(client, bucket, key):
    head = client.head_object(Bucket=bucket, Key=key)
    response = client.get_object(Bucket=bucket, Key=key, IfMatch=head['ETag'])
    return json.loads(response['Body'].read()), head


def validate_source(report_id, source, source_head, settings):
    uid = report_id[-36:]
    if not re.fullmatch(r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}', uid):
        raise ValueError('Report ID has no exact source UUID')
    if uid == PROTECTED_UUID:
        raise ValueError('Protected uncertain UUID')
    if report_id not in settings['backlog_report_ids'] and uid not in settings['backlog_source_uuids']:
        if source_head['LastModified'] <= datetime.fromisoformat(settings['source_cutoff_utc']):
            raise ValueError('Historical source outside approved backlog')
    inn = str((source.get('PasportData') or {}).get('Manufacturer_inn') or '')
    if inn not in settings['allowed_inns']:
        raise ValueError('Unconfigured INN')
    raw_quantity = source.get('Quantity')
    from decimal import Decimal, InvalidOperation
    try:
        quantity = Decimal(str(raw_quantity).strip()) if not isinstance(raw_quantity, bool) else Decimal(0)
        positive = quantity.is_finite() and quantity > 0 and quantity == quantity.to_integral_value()
    except InvalidOperation:
        positive = False
    if not positive:
        raise ValueError('Blank/nonpositive quantity is notification only')


def deliver_pallet_report(*, report_key, runtime_config, delivery_config_path=None):
    """Execute only within the released Celery worker; delivery ledger holds ambiguous POSTs."""
    import boto3
    import requests
    config_path = delivery_config_path or os.environ.get('XTREK_PALLET_DELIVERY_CONFIG')
    if not config_path:
        return {'state': 'DISABLED'}
    settings = json.loads(Path(config_path).read_text())
    if settings.get('enabled') is not True:
        return {'state': 'DISABLED'}
    if (str(settings['chat_id']) != '-72448994566325'
            or str(settings['bot_user_id']) != '259171708'):
        raise ValueError('Pallet recipient differs from the approved identity')
    if not re.fullmatch(r'equipment-reports/[^/]+\.json', report_key):
        raise ValueError('Report key outside exact report route')
    report_id = report_key.rsplit('/', 1)[-1][:-5]
    uid = report_id[-36:]
    if uid == PROTECTED_UUID:
        raise ValueError('Protected uncertain UUID')
    client = boto3.client('s3', **runtime_config['s3_config'])
    task_bucket = settings['task_bucket']
    if settings.get('all_valid_assigned_reports') is not True:
        source, source_head = read_snapshot(client, task_bucket, f'Задания/{uid}.json')
        validate_source(report_id, source, source_head, settings)
    elif report_id not in settings['backlog_report_ids']:
        # Future sources remain bounded by the explicit cutoff.
        source, source_head = read_snapshot(client, task_bucket, f'Задания/{uid}.json')
        validate_source(report_id, source, source_head, settings)
    report, _ = read_snapshot(client, settings['report_bucket'], report_key)
    task_key = f'equipment-tasks/{report_id}.json'
    task, _ = read_snapshot(client, task_bucket, task_key)
    labels = prepare_report_label_messages(
        report_storage=SnapshotStorage({report_key: report}),
        task_storage=SnapshotStorage({task_key: task}), report_key=report_key)
    for label in labels:
        if label.sscc[1:10] != '460705179':
            raise ValueError('Assigned pallet SSCC must retain shared Fito prefix')
    env = {}
    for line in Path(settings['max_environment_file']).read_text().splitlines():
        if line.strip() and not line.lstrip().startswith('#'):
            key, value = line.split('=', 1)
            env[key] = shlex.split(value)[0] if value else ''
    if env['MAX_CHAT_ID'] != str(settings['chat_id']):
        raise ValueError('MAX environment recipient mismatch')
    session = requests.Session()
    session.verify = env['MAX_CA_FILE']
    ledger = DeliveryLedger(settings['ledger_path'])
    notifier = MaxPalletLabelNotifier(
        token=env['MAX_BOT_TOKEN'], chat_id=str(settings['chat_id']),
        expected_bot_user_id=str(settings['bot_user_id']), ledger=ledger,
        session=session, history_start_ms=settings['history_start_ms'])
    notifier._verify_bot()
    # Old exact attachments must be visible before any new message POST.
    history = notifier._history()
    for label in labels:
        if report_id in settings['previously_delivered_report_ids']:
            found = notifier._exact_message(history, label)
            if not found:
                raise ValueError('Prior PNG not GET confirmed; never repeat its POST')
            row = ledger.get(label.report_id, label.sscc, str(settings['chat_id']))
            if row is None:
                ledger.reserve(label, str(settings['chat_id']))
                ledger.set_state(label, str(settings['chat_id']), 'CONFIRMED', str(found['body']['mid']))
            return {'state': 'ALREADY_CONFIRMED_BY_GET', 'report_id': report_id}
    results = []
    for label in labels:
        output = Path(settings['png_root']) / label.report_id / label.file_name
        output.parent.mkdir(parents=True, exist_ok=True)
        if output.exists() and output.read_bytes() != label.png_bytes:
            raise ValueError('Immutable PNG cache changed')
        if not output.exists():
            temp = output.with_suffix('.tmp')
            temp.write_bytes(label.png_bytes)
            os.replace(temp, output)
        result = notifier.deliver(label)
        results.append({'sscc': label.sscc, 'png_sha256': hashlib.sha256(label.png_bytes).hexdigest(), **result})
    return {'state': 'DELIVERED', 'report_id': report_id, 'labels': results}
