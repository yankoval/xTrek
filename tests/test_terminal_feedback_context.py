import json
from unittest.mock import MagicMock

import pytest

from xtrek import terminal_feedback as feedback, terminal_feedback_runtime as runtime
from xtrek.terminal_feedback_s3 import S3TerminalLedger
from test_terminal_feedback import settings
from test_terminal_feedback_s3 import Store


@pytest.mark.parametrize('backend', ['sqlite', 's3'])
@pytest.mark.parametrize('category', [c for c in feedback.ACTIONS if c != 'retry_exhausted'])
def test_every_terminal_message_has_frozen_source_context(tmp_path, monkeypatch, settings, backend, category):
    if backend == 's3':
        store = Store(str(tmp_path / 's3.db'))
        monkeypatch.setattr('xtrek.storage.get_storage', lambda *args: store)
        settings.update(ledger_backend='s3', ledger_path='s3://shared/events')
    ledger = runtime.open_ledger(settings, {})
    rid = 'T-4841-77-002-06Х6-С-03-b19dcec9-7b8a-4a1f-bf5f-da3c99f5dc26'
    event = ledger.record('bucket', 'equipment-reports/' + rid + '.json', 'report-validation', category)
    read = MagicMock(return_value={'Article': 'АРТ/4841', 'PasportData': {'Batch_number': '06Х6/С-03'}, 'operator': 'Unknown'})
    factory = MagicMock()
    factory.return_value.find.return_value = 'mid'
    assert runtime.drain(settings, {}, read_json=read, transport_factory=factory) == {'CONFIRMED': 1}
    row = ledger.get(event['event_id'])
    body = row['message_text']
    assert 'Артикул: АРТ/4841' in body and 'Партия: 06Х6/С-03' in body
    assert 'ID отчёта: ' + rid in body
    assert 'Категория:' not in body and 'productionOrder ref' not in body and 'report-validation' not in body
    assert len(body) < 4000
    read.side_effect = AssertionError('Do not reread or resend delivered notification')
    assert runtime.drain(settings, {}, read_json=read, transport_factory=factory) == {}


def test_missing_fields_controls_and_long_values_are_safe():
    row = dict(event_id='e' * 64, production_order_id='T-Х-С', stage='report-validation', category='invalid_report')
    text = feedback.message_text(row)
    assert 'Артикул: не указан' in text and 'Партия: не указан' in text
    row['context_json'] = json.dumps({'article': 'АРТ/1\nСтадия: ложь', 'batch': 'П' * 10000})
    text = feedback.message_text(row)
    assert 'АРТ/1\\u000aСтадия: ложь' in text
    assert text.count('\nСтадия:') == 0 and len(text) < 2000


def test_virtual_receipt_resolves_verified_parent_context(settings):
    parent = 'T-4841-06Х6-С-03'
    virtual = 'V-' + parent + '-04670017929630'
    row = {'bucket': 'bucket', 'source_key': 'introduceReceipts/uuid.json'}
    read = MagicMock(side_effect=[{'productionOrderId': virtual},
        {'Gtin': '04670017929630', 'operator': None, 'Article': 'component'},
        {'Article': '4841-77-002', 'PasportData': {'Batch_number': '06Х6-С-03'}}])
    order_id, operator, context = runtime.resolve_order(row, settings, {}, read, with_context=True)
    assert order_id == virtual and operator is None
    assert context == {'report_id': parent, 'article': '4841-77-002', 'batch': '06Х6-С-03'}


def test_context_survives_broken_route_and_concurrent_resolver(settings):
    ledger = runtime.open_ledger(settings, {})
    row = ledger.record('bucket', 'equipment-reports/T.json', 'report-validation', 'invalid_report')
    settings['default_route'] = None
    read = MagicMock(return_value={'Article': 'FIRST', 'PasportData': {'Batch_number': 'LOT/1'}})
    factory = MagicMock()
    runtime.drain(settings, {}, read_json=read, transport_factory=factory)
    frozen = ledger.get(row['event_id'])['context_json']
    ledger.bind_route(row['event_id'], 'T', None, None, context={'article': 'CHANGED'})
    assert ledger.get(row['event_id'])['context_json'] == frozen
    settings['default_route'] = 'line-a'
    read.side_effect = AssertionError('Source snapshot is frozen')
    factory.return_value.find.return_value = 'mid'
    runtime.drain(settings, {}, read_json=read, transport_factory=factory)
    assert 'Артикул: FIRST' in ledger.get(row['event_id'])['message_text']
