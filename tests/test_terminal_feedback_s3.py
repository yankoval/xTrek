"""Cross-worker CAS and restart tests: synthetic storage and fake MAX only."""
import json
import hashlib
import multiprocessing
import sqlite3
from concurrent.futures import ProcessPoolExecutor
from unittest.mock import MagicMock

import pytest
from token_lock_store import ProcessLockStore
from test_terminal_feedback import settings
from test_equipment_report_prerequisites import router
from xtrek import terminal_feedback as feedback
from xtrek import terminal_feedback_runtime as runtime
from xtrek.terminal_feedback_s3 import S3TerminalLedger
from xtrek.terminal_feedback_max import deliver

ROOT = 's3://coordination/terminal-feedback/v1'


class Store(ProcessLockStore):
    def list_files(self, path, pattern, **kwargs):
        assert kwargs == {'include_processed': True, 'required': True}
        with sqlite3.connect(self.path) as db:
            return [r[0] for r in db.execute('SELECT path FROM locks') if r[0].startswith(path + '/')]


def worker(path, route):
    ledger = S3TerminalLedger(Store(path), ROOT)
    row = ledger.record('bucket', 'equipment-reports/order.json', 'report-validation', 'invalid_report')
    row = ledger.bind_route(row['event_id'], 'order', None, route)
    return ledger.claim_post(row['event_id'])


def test_independent_processes_record_and_reserve_only_once(tmp_path, settings):
    path = str(tmp_path / 'fake-s3.db')
    Store(path)
    with ProcessPoolExecutor(4, mp_context=multiprocessing.get_context('spawn')) as pool:
        futures = [pool.submit(worker, path, settings['routes']['line-a']) for _ in range(12)]
        assert sum(f.result() for f in futures) == 1
    ledger = S3TerminalLedger(Store(path), ROOT)
    assert len(ledger.pending()) == 1
    row = ledger.pending()[0]
    transport = MagicMock()
    transport.find.return_value = None
    assert deliver(ledger, row, transport) == 'GET_RECONCILIATION_REQUIRED'
    transport.post.assert_not_called()
    transport.find.return_value = 'confirmed-mid'
    assert deliver(ledger, row, transport) == 'CONFIRMED'
    assert not S3TerminalLedger(Store(path), ROOT).pending()
    assert ledger.get(row['event_id'])['terminal_state'] == 'RECONCILIATION_REQUIRED'


@pytest.mark.parametrize('outcome', ['timeout', 'http-error', 'accepted-not-visible', 'crash-before-post'])
def test_shared_ambiguous_delivery_survives_host_loss(tmp_path, settings, outcome):
    path = str(tmp_path / 'fake-s3.db')
    first = S3TerminalLedger(Store(path), ROOT)
    row = first.record('bucket', 'equipment-reports/order.json', 'report-validation', 'invalid_report')
    row = first.bind_route(row['event_id'], 'order', None, settings['routes']['line-a'])
    fake = MagicMock()
    fake.find.return_value = None
    if outcome in {'timeout', 'http-error'}:
        fake.post.side_effect = TimeoutError()
    if outcome == 'crash-before-post':
        assert first.claim_post(row['event_id'])
    assert deliver(first, row, fake) == 'GET_RECONCILIATION_REQUIRED'
    second = S3TerminalLedger(Store(path), ROOT)
    assert deliver(second, row, fake) == 'GET_RECONCILIATION_REQUIRED'
    assert fake.post.call_count == (0 if outcome == 'crash-before-post' else 1)
    fake.find.return_value = 'mid'
    assert deliver(second, row, fake) == 'CONFIRMED'


@pytest.mark.parametrize('bad', [None, '', [], {}, 'missing'])
def test_bad_default_preserves_pending_and_raw_snapshot(tmp_path, settings, bad, monkeypatch, caplog):
    store = Store(str(tmp_path / 'fake-s3.db'))
    settings.update(ledger_backend='s3', ledger_path=ROOT, default_route=bad)
    monkeypatch.setattr('xtrek.storage.get_storage', lambda *args: store)
    ledger = runtime.open_ledger(settings, {})
    row = ledger.record('bucket', 'equipment-reports/order.json', 'report-validation', 'invalid_report')
    read = MagicMock(return_value={'PasportData': {'operator': 'Bob'}, 'operator': None})
    factory = MagicMock()
    factory.return_value.find.return_value = 'mid'
    assert runtime.drain(settings, {}, read_json=read, transport_factory=factory) == {'ROUTE_CONFIGURATION_ERROR': 1}
    factory.assert_not_called()
    assert 'Invalid terminal feedback default route' in caplog.text
    saved = ledger.get(row['event_id'])
    assert saved['delivery_state'] == 'ROUTE_HELD' and json.loads(saved['operator_raw']) is None
    settings['default_route'] = 'line-a'
    read.side_effect = AssertionError('Frozen source must not be reread')
    assert runtime.drain(settings, {}, read_json=read, transport_factory=factory) == {'CONFIRMED': 1}
    assert factory.call_args.args[0]['chat_id'] == 'chat-a'


def test_two_worker_instances_share_stop_before_business(tmp_path, settings, monkeypatch, router):
    from test_terminal_feedback import enable
    tasks, task, _ = router
    path = str(tmp_path / 'fake-s3.db')
    settings.update(ledger_backend='s3', ledger_path=ROOT)
    enable(tasks, settings, tmp_path)
    monkeypatch.setattr('xtrek.storage.get_storage', lambda *args: Store(path))
    business = MagicMock(side_effect=feedback.TerminalBusinessError())
    monkeypatch.setattr(tasks, 'logic_start_equipment_reports', business)
    data = {'bucket': 'internal-bucket', 'key': 'equipment-reports/order.json'}
    assert isinstance(task.apply(args=[data]).result, feedback.TerminalRecorded)
    assert isinstance(task.apply(args=[data]).result, feedback.TerminalRecorded)
    business.assert_called_once()
    assert len(runtime.open_ledger(settings, {}).pending()) == 1


def test_lost_cas_response_never_grants_another_post(tmp_path, settings):
    store = Store(str(tmp_path / 'fake-s3.db'))
    ledger = S3TerminalLedger(store, ROOT)
    row = ledger.record('bucket', 'equipment-reports/order.json', 'report-validation', 'invalid_report')
    row = ledger.bind_route(row['event_id'], 'order', 'Bob', settings['routes']['line-b'])
    original = store.write_lock_object
    def uncertain(*args):
        original(*args)
        raise TimeoutError()
    store.write_lock_object = uncertain
    fake = MagicMock()
    fake.find.return_value = None
    with pytest.raises(TimeoutError):
        deliver(ledger, row, fake)
    assert deliver(S3TerminalLedger(Store(store.path), ROOT), row, fake) == 'GET_RECONCILIATION_REQUIRED'
    fake.post.assert_not_called()


def _approved_utilisation(store, ledger, *, status='SENT'):
    bucket = 'internal-bucket'
    report = 'T-GB-6937-test'
    key = f'utilisationReceipts/{report}.json'
    row = ledger.record(bucket, key, 'utilisation-status', 'utilisation_rejected')
    row = ledger.bind_route(row['event_id'], report, None, {'transport': 'max',
                                                           'chat_id': 'chat',
                                                           'bot_user_id': 'bot',
                                                           'token_env': 'TEST_TOKEN'})
    assert ledger.claim_post(row['event_id'])
    ledger.confirmed(row['event_id'], 'old-notice-id')
    receipt_path = f's3://{bucket}/{key}'
    receipt = {'reportId': '12345678-1234-1234-1234-123456789abc',
               'productionOrderId': report, 'orderId': report}
    receipt_etag = store.write_lock_object(receipt_path, json.dumps(receipt), None)
    status_path = f's3://{bucket}/utilisationReports/{report}.json'
    store.write_lock_object(status_path, json.dumps({
        'reportId': receipt['reportId'], 'productionOrderId': report,
        'reportStatus': status}), None)
    task_path = f's3://{bucket}/utilisationTasks/{report}.json'
    task_text = json.dumps({'productionOrderId': report, 'sntins': ['code']})
    store.write_lock_object(task_path, task_text, None)
    terminal_etag = store.read_lock_object(ledger._path(row['event_id']))[1]
    resolution = {'version': 1, 'event_id': row['event_id'],
                  'terminal_etag': terminal_etag, 'bucket': bucket,
                  'source_key': key, 'new_report_id': receipt['reportId'],
                  'receipt_etag': receipt_etag,
                  'task_sha256': hashlib.sha256(task_text.encode()).hexdigest(),
                  'approval_reference': 'user-confirmed-two-report-recovery'}
    store.write_lock_object(ledger.resolution_root + '/' + row['event_id'] + '.json',
                            json.dumps(resolution), None)
    return row, receipt_path, status_path, resolution


def test_exact_reconciliation_allows_new_receipt_without_erasing_old_notice(tmp_path):
    store = Store(str(tmp_path / 'fake-s3.db'))
    ledger = S3TerminalLedger(store, ROOT)
    row, receipt_path, status_path, resolution = _approved_utilisation(store, ledger)
    event_id = row['event_id']
    assert ledger.permits_reconciled_utilisation(event_id)
    assert ledger.get(event_id)['terminal_state'] == 'RECONCILIATION_REQUIRED'
    assert ledger.get(event_id)['delivery_state'] == 'CONFIRMED'
    assert ledger.pending() == []  # Resolution objects stay outside the pending prefix.

    old_status, etag = store.read_lock_object(status_path)
    store.write_lock_object(status_path, json.dumps({**json.loads(old_status),
                                                     'reportStatus': 'FAILED'}), etag)
    assert not ledger.permits_reconciled_utilisation(event_id)
    store.write_lock_object(status_path, old_status,
                            store.read_lock_object(status_path)[1])
    old_receipt, etag = store.read_lock_object(receipt_path)
    store.write_lock_object(receipt_path, json.dumps({**json.loads(old_receipt),
                                                      'changed': True}), etag)
    assert not ledger.permits_reconciled_utilisation(event_id)  # ETag changed.
    assert resolution['receipt_etag'] != store.read_lock_object(receipt_path)[1]


def test_reconciliation_only_bypasses_matching_utilisation_terminal(tmp_path, settings,
                                                                    monkeypatch, router):
    from test_terminal_feedback import enable
    tasks, task, _ = router
    store = Store(str(tmp_path / 'fake-s3.db'))
    settings.update(ledger_backend='s3', ledger_path=ROOT)
    enable(tasks, settings, tmp_path)
    monkeypatch.setattr('xtrek.storage.get_storage', lambda *args: store)
    ledger = S3TerminalLedger(store, ROOT)
    row, _, _, _ = _approved_utilisation(store, ledger)
    business = MagicMock(return_value='started')
    monkeypatch.setattr(tasks, 'logic_utilisationReceipt', business)
    data = {'bucket': 'internal-bucket', 'key': row['source_key']}
    assert task.apply(args=[data]).successful()
    business.assert_called_once()
    assert isinstance(task.apply(kwargs={'data': data,
                                         'terminal_failure': 'utilisation_rejected'}).result,
                      feedback.TerminalRecorded)
    business.assert_called_once()


def test_corrupt_reconciliation_record_keeps_terminal_fence(tmp_path):
    store = Store(str(tmp_path / 'fake-s3.db'))
    ledger = S3TerminalLedger(store, ROOT)
    row, _, _, _ = _approved_utilisation(store, ledger)
    path = ledger.resolution_root + '/' + row['event_id'] + '.json'
    _, etag = store.read_lock_object(path)
    store.write_lock_object(path, '{broken', etag)
    assert not ledger.permits_reconciled_utilisation(row['event_id'])
    assert ledger.get(row['event_id'])['delivery_state'] == 'CONFIRMED'
