"""Cross-worker CAS and restart tests: synthetic storage and fake MAX only."""
import json
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
