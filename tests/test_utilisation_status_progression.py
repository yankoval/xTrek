"""Poll real workflow status transitions; no external sends or live services."""
import json
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
import requests

from xtrek import create_emission_task_sample as flow
from xtrek.operation_state import OperationBusy, OperationConflict, publish_utilisation_status
from xtrek.storage import LocalStorage, S3Storage
from test_tasks_unit_set_flow import import_tasks


def snapshot(status):
    return dict(omsId='OMS', reportId='REPORT', reportStatus=status,
                productionOrderId='T-JOB', errorReason=None)


@pytest.fixture
def polling(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    config = {'operation_state_path': str(tmp_path / 'state'),
              'utilisation_receipts': str(tmp_path / 'receipts'),
              'utilisation_reports': str(tmp_path / 'statuses')}
    store = LocalStorage()
    store.write_text(config['utilisation_receipts'] + '/T-JOB.json',
                     json.dumps(dict(reportId='REPORT', omsId='OMS', productionOrderId='T-JOB')))
    monkeypatch.setattr(flow, 'load_config', lambda *a: config)
    org = SimpleNamespace(inn='7701234567', oms_id='OMS', connection_id='CON')
    monkeypatch.setattr(flow, 'OrganizationManager', lambda *a: SimpleNamespace(list=lambda: [org]))
    monkeypatch.setattr(flow, 'TokenProcessor', lambda **kw: SimpleNamespace(
        get_token_value_for=lambda *a, **kw: 'TEST-TOKEN'))
    api = MagicMock()
    monkeypatch.setattr(flow, 'SUZ', lambda **kw: api)
    monkeypatch.setattr(requests.sessions.Session, 'request', MagicMock(side_effect=AssertionError('No live HTTP')))
    return config, store, api


def test_poll_ready_sent_success_then_stale_reply(polling):
    config, store, api = polling
    api.report_info.side_effect = [snapshot(s) for s in ('READY_TO_SEND', 'SENT', 'SUCCESS', 'SENT')]
    results = [flow.update_utilisation_report_status('T-JOB').reportStatus for _ in range(4)]
    assert results == ['READY_TO_SEND', 'SENT', 'SUCCESS', 'SUCCESS']
    path = config['utilisation_reports'] + '/T-JOB.json'
    assert json.loads(store.read_text(path))['reportStatus'] == 'SUCCESS'
    assert store.get_tags(path)['reportStatus'] == 'SUCCESS'
    api.utilisation_send.assert_not_called()


def test_route_retries_poll_then_introduces_once(polling, monkeypatch):
    config, store, api = polling
    tasks = import_tasks(monkeypatch)
    monkeypatch.setattr(tasks, 'config', config)
    monkeypatch.setattr('xtrek.config_loader.load_config', lambda *a: config)
    api.report_info.side_effect = [snapshot('SENT'), snapshot('SUCCESS')]
    monkeypatch.setattr(tasks, 'update_utilisation_report_status', flow.update_utilisation_report_status)
    monkeypatch.setattr(tasks, 'get_production_order_data', lambda *a: {'GtinType': 'UNIT'})
    create, send = MagicMock(return_value=True), MagicMock(return_value={'document_id': 'ONE'})
    monkeypatch.setattr(tasks, 'create_introduce_task_from_report', create)
    monkeypatch.setattr(tasks, 'sign_and_send_introduce', send)
    with pytest.raises(RuntimeError, match='SENT'):
        tasks.logic_utilisationReceipt('internal/utilisationReceipts/T-JOB.json')
    first = tasks.logic_utilisationReceipt('internal/utilisationReceipts/T-JOB.json')
    assert tasks.logic_utilisationReceipt('internal/utilisationReceipts/T-JOB.json') == first
    create.assert_called_once()
    send.assert_called_once()
    assert api.report_info.call_count == 2
    api.utilisation_send.assert_not_called()


class AtomicS3(S3Storage):
    """Conditional S3 body/tag writes, with deterministic race injection."""
    def __init__(self):
        self.value = self.etag = None
        self.tags = {}
        self.writes = 0

    def read_lock_object(self, path):
        return (self.value, self.etag) if self.value else None

    def get_tags(self, path):
        return self.tags.copy()

    def write_lock_object(self, path, text, etag, *, tags=None):
        if etag != self.etag:
            return None
        self.writes += 1
        self.value, self.etag, self.tags = text, str(self.writes), tags.copy()
        return self.etag


def test_competing_success_wins_over_late_sent(monkeypatch):
    store = AtomicS3()
    publish_utilisation_status(store, 'status', snapshot('READY_TO_SEND'))
    store.tags['audit'] = 'preserve'
    write = store.write_lock_object
    def competing(path, text, etag, *, tags):
        write(path, json.dumps(snapshot('SUCCESS')), etag,
              tags={'reportStatus': 'SUCCESS', 'audit': 'preserve'})
        return write(path, text, etag, tags=tags)
    monkeypatch.setattr(store, 'write_lock_object', competing)
    assert publish_utilisation_status(store, 'status', snapshot('SENT'))['reportStatus'] == 'SUCCESS'
    assert json.loads(store.value)['reportStatus'] == 'SUCCESS'
    assert store.tags == {'reportStatus': 'SUCCESS', 'audit': 'preserve'}


def test_unchanged_or_older_snapshot_does_not_write():
    store = AtomicS3()
    publish_utilisation_status(store, 'status', snapshot('SENT'))
    publish_utilisation_status(store, 'status', snapshot('SENT'))
    assert publish_utilisation_status(store, 'status', snapshot('READY_TO_SEND')) == snapshot('SENT')
    assert store.writes == 1


@pytest.mark.parametrize('field', ['omsId', 'reportId', 'productionOrderId'])
def test_status_cannot_replace_another_report(field):
    store = AtomicS3()
    publish_utilisation_status(store, 'status', snapshot('SENT'))
    with pytest.raises(OperationConflict, match='identity'):
        publish_utilisation_status(store, 'status', dict(snapshot('SUCCESS'), **{field: 'OTHER'}))
    assert store.writes == 1


def test_failed_cas_is_retryable(monkeypatch):
    store = AtomicS3()
    monkeypatch.setattr(store, 'write_lock_object', lambda *a, **kw: None)
    with pytest.raises(OperationBusy):
        publish_utilisation_status(store, 'status', snapshot('SUCCESS'))


def test_lost_success_reply_recovers_without_regression(monkeypatch):
    store = AtomicS3()
    write = store.write_lock_object
    def lost_reply(*a, **kw):
        write(*a, **kw)
        raise TimeoutError('accepted by S3')
    monkeypatch.setattr(store, 'write_lock_object', lost_reply)
    with pytest.raises(TimeoutError):
        publish_utilisation_status(store, 'status', snapshot('SUCCESS'))
    assert publish_utilisation_status(store, 'status', snapshot('SENT')) == snapshot('SUCCESS')
    assert store.tags['reportStatus'] == 'SUCCESS'


@pytest.mark.parametrize('coordinated', [False, True])
def test_failed_poll_saves_per_cis_errors_and_reuses_complete_details(polling, coordinated):
    config, store, api = polling
    if not coordinated:
        config.pop('operation_state_path')
    api.headers = {'clientToken': 'TEST-TOKEN'}
    api.report_info.return_value = snapshot('FAILED')
    api.receipt_info.return_value = {'results': [{
        'resultDocId': 'REPORT', 'operations': [
            {'operationType': 'RNMS_GIS_PROCESSED', 'docId': 'RESULT'}]}]}
    api.receipt_document.return_value = {'content': json.dumps({'cisList': [
        {'cis': 'SYNTHETIC-CIS', 'code': 12, 'description': 'Нет карточки НК'}]})}
    first = flow.update_utilisation_report_status('T-JOB')
    second = flow.update_utilisation_report_status('T-JOB')
    assert first.reportStatus == second.reportStatus == 'FAILED'
    assert first.failureDetails['state'] == 'complete'
    path = config['utilisation_reports'] + '/T-JOB.json'
    saved = json.loads(store.read_text(path))
    assert saved['failureDetails']['documents'][0]['cisList'][0]['code'] == 12
    assert store.get_tags(path)['reportStatus'] == 'FAILED'
    api.receipt_info.assert_called_once_with('REPORT')
    api.receipt_document.assert_called_once_with('REPORT', 'RESULT')
    api.utilisation_send.assert_not_called()


def test_failed_poll_keeps_status_when_receipt_is_not_available(polling):
    config, store, api = polling
    api.headers = {}
    api.report_info.return_value = snapshot('FAILED')
    api.receipt_info.side_effect = TimeoutError('unbounded-body-' * 1000)
    result = flow.update_utilisation_report_status('T-JOB')
    assert result.reportStatus == 'FAILED'
    assert result.failureDetails['state'] == 'unavailable'
    saved = json.loads(store.read_text(config['utilisation_reports'] + '/T-JOB.json'))
    assert saved['reportStatus'] == 'FAILED'
    assert 'unbounded-body' not in json.dumps(saved)
    api.utilisation_send.assert_not_called()


def test_non_failed_poll_never_fetches_receipts(polling):
    _, _, api = polling
    api.report_info.side_effect = [snapshot('SENT'), snapshot('SUCCESS')]
    assert flow.update_utilisation_report_status('T-JOB').reportStatus == 'SENT'
    assert flow.update_utilisation_report_status('T-JOB').reportStatus == 'SUCCESS'
    api.receipt_info.assert_not_called()
    api.receipt_document.assert_not_called()


def test_failed_publisher_keeps_details_from_competing_poll(monkeypatch):
    store = AtomicS3()
    details = {'state': 'complete', 'receipts': [], 'documents': [
        {'resultDocId': 'REPORT', 'docId': 'DOC', 'cisList': [
            {'cis': 'SYNTHETIC-CIS', 'code': 12}]}], 'issues': []}
    publish_utilisation_status(store, 'status', snapshot('SENT'))
    write = store.write_lock_object

    def competing(path, text, etag, *, tags):
        write(path, json.dumps(dict(snapshot('FAILED'), failureDetails=details)), etag,
              tags={'reportStatus': 'FAILED'})
        return write(path, text, etag, tags=tags)

    monkeypatch.setattr(store, 'write_lock_object', competing)
    unavailable = {'state': 'unavailable', 'receipts': [], 'documents': [], 'issues': []}
    effective = publish_utilisation_status(
        store, 'status', dict(snapshot('FAILED'), failureDetails=unavailable))
    assert effective['reportStatus'] == 'FAILED'
    assert effective['failureDetails'] == details
    assert json.loads(store.value)['failureDetails'] == details
