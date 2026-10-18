import hashlib
import json
from unittest.mock import MagicMock

import pytest
from celery.exceptions import Retry

from xtrek import equipment_progress as progress, operation_state, utils
from xtrek.terminal_feedback import WorkflowPending, TerminalBusinessError, TerminalRecorded
from test_equipment_report_prerequisites import router
from test_terminal_feedback import settings, ledger, enable
from token_lock_store import MemoryLockStore


@pytest.fixture
def checkpoint(monkeypatch):
    rid = 'T-4841-06Х6-С-03-test'
    config = {k: 's3://bucket/' + k for k in ('production_orders_path', 'equipment-reports',
              'utilisation_tasks_path', 'utilisation_receipts', 'utilisation_reports', 'operation_state_path')}
    code = '010461011763608321TEST'
    task = {'productionOrderId': rid, 'sntins': [code], 'attributes': {'expirationDate': '2029-06-01'}}
    values = {'production_orders_path': {'GtinType': 'SET'},
              'equipment-reports': {'readyBox': [{'productNumbersFull': [code + '\x1d93TEST']}]},
              'utilisation_tasks_path': task,
              'utilisation_receipts': {'productionOrderId': rid, 'orderId': rid, 'reportId': 'accepted-id'},
              'utilisation_reports': {'productionOrderId': rid, 'reportId': 'accepted-id', 'reportStatus': 'SUCCESS'}}
    store = MagicMock()
    def read(path):
        folder = path.rsplit('/', 2)[-2]
        return json.dumps(values[folder])
    store.read_text.side_effect = read
    store.exists.side_effect = lambda path: path.rsplit('/', 2)[-2] in values
    guard = {'phase': 'complete', 'operation': 'send-utilisation', 'business_key': rid,
             'input_hash': operation_state.fingerprint(json.dumps({
                 'source': operation_state.fingerprint(json.dumps(task)),
                 'parameters': {'oms_id': None, 'client_token': None}})),
             'result': {'model': None, 'value': 'accepted-id'}}
    store.read_lock_object.side_effect = lambda path: (json.dumps(guard), 'etag')
    monkeypatch.setattr(progress, 'get_storage', lambda *args: store)
    return rid, config, values, guard, store


def test_exact_success_checkpoint_is_read_only(checkpoint):
    rid, config, _, _, store = checkpoint
    assert progress.applied_checkpoint(rid, config) == 'SET'
    store.write_lock_object.assert_not_called()
    store.set_tags.assert_not_called()
    store.upload.assert_not_called()


@pytest.mark.parametrize('phase', ['uncertain', 'external', 'rejected'])
def test_applied_does_not_override_unknown_or_rejected_submission(checkpoint, phase):
    rid, config, _, guard, _ = checkpoint
    guard['phase'] = phase
    with pytest.raises(operation_state.ReconciliationRequired):
        progress.applied_checkpoint(rid, config)


@pytest.mark.parametrize('missing', ['utilisation_tasks_path', 'utilisation_receipts', 'utilisation_reports'])
def test_missing_eventual_checkpoint_waits(checkpoint, missing):
    rid, config, values, _, _ = checkpoint
    del values[missing]
    with pytest.raises(WorkflowPending):
        progress.applied_checkpoint(rid, config)


@pytest.mark.parametrize('change', ['codes', 'dates', 'receipt', 'status', 'guard_id'])
def test_conflicting_checkpoint_never_resumes(checkpoint, change):
    rid, config, values, guard, _ = checkpoint
    if change == 'codes': values['equipment-reports']['readyBox'][0]['productNumbersFull'] = ['different']
    if change == 'dates': values['utilisation_tasks_path']['attributes']['expirationDate'] = '2030-01-01'
    if change == 'receipt': values['utilisation_receipts']['reportId'] = 'different'
    if change == 'status': values['utilisation_reports']['productionOrderId'] = 'different'
    if change == 'guard_id': guard['business_key'] = 'different'
    with pytest.raises(operation_state.ReconciliationRequired):
        progress.applied_checkpoint(rid, config)


@pytest.mark.parametrize('status', ['SENT', 'IN_PROGRESS', None])
def test_accepted_in_progress_waits(checkpoint, status):
    rid, config, values, _, _ = checkpoint
    values['utilisation_reports']['reportStatus'] = status
    with pytest.raises(WorkflowPending):
        progress.applied_checkpoint(rid, config)


@pytest.mark.parametrize('status', ['APPLIED', 'IN_PROGRESS', None, 'NOT_FOUND', 'NEW_UNKNOWN_STATUS', 'WRITTEN_OFF'])
def test_precheck_distinguishes_waiting_from_invalid_status(tmp_path, status):
    code = '010461011763608321TEST'
    path = tmp_path / 'report.json'
    path.write_text(json.dumps({'readyBox': [{'productNumbersFull': [code]}]}))
    api, nk = MagicMock(), MagicMock()
    nk.product_info.return_value = {'isSet': True}
    api.get_list_cis_info.return_value = [{'cisInfo': {'cis': code, 'status': status}}]
    result = utils.AggregationAnalyzer(api, nk).check_report(str(path), strict_prerequisites=True)
    assert set(result) == ({'wrongsetstatus'} if status == 'WRITTEN_OFF' else {'waitingstatus'})


def test_waiting_route_retries_beyond_limit_then_resumes_without_utilisation_post(
        router, monkeypatch, settings, ledger, tmp_path):
    tasks, task, _ = router
    enable(tasks, settings, tmp_path)
    store = MemoryLockStore()
    tasks.config['operation_state_path'] = 's3://isolated/guards'
    monkeypatch.setattr(operation_state, 'get_storage', lambda *args: store)
    monkeypatch.setattr(tasks, 'check_aggregation_reports', MagicMock(return_value={'path': {'waitingstatus': ['APPLIED']}}))
    monkeypatch.setattr(tasks, 'applied_checkpoint', MagicMock(return_value='SET'))
    components = MagicMock(return_value=True)
    monkeypatch.setattr(tasks, 'create_virtual_tasks_from_equipment_report', components)
    monkeypatch.setattr(tasks, 'set_ready_check', MagicMock(side_effect=[None, 'setReady']))
    for name in ('create_equipment_set_report_from_report', 'create_aggregation_set_report', 'sign_and_send_aggregation_set'):
        monkeypatch.setattr(tasks, name, MagicMock(return_value=True))
    send = MagicMock(side_effect=AssertionError('No repeated utilisation POST'))
    monkeypatch.setattr(tasks, 'sign_and_send_utilisation', send)
    monkeypatch.setattr(tasks, 'create_utilisation_task_from_report', send)
    retry = MagicMock(side_effect=Retry())
    monkeypatch.setattr(task, 'retry', retry)
    data = {'bucket': 'internal-bucket', 'key': 'equipment-reports/report.json'}
    assert task.apply(args=[data], retries=250).state == 'RETRY'
    assert json.loads(store.lock_object[0])['phase'] == 'ready'
    assert not ledger.pending()
    assert task.apply(args=[data], retries=251).successful()
    assert json.loads(store.lock_object[0])['phase'] == 'complete'
    assert task.apply(args=[data], retries=252).successful()
    tasks.sign_and_send_aggregation_set.assert_called_once()
    send.assert_not_called()
    assert not ledger.pending()


def test_waiting_without_checkpoint_does_not_create_components_or_fence(
        router, monkeypatch, settings, ledger, tmp_path):
    tasks, task, _ = router
    enable(tasks, settings, tmp_path)
    monkeypatch.setattr(tasks, 'check_aggregation_reports', MagicMock(return_value={'path': {'waitingstatus': ['APPLIED']}}))
    monkeypatch.setattr(tasks, 'applied_checkpoint', MagicMock(side_effect=WorkflowPending()))
    create = MagicMock()
    monkeypatch.setattr(tasks, 'create_virtual_tasks_from_equipment_report', create)
    monkeypatch.setattr(task, 'retry', MagicMock(side_effect=Retry()))
    assert task.apply(args=[{'bucket': 'internal-bucket', 'key': 'equipment-reports/report.json'}], retries=201).state == 'RETRY'
    assert not ledger.pending()
    create.assert_not_called()


def test_invalid_set_status_still_records_terminal_event(router, monkeypatch, settings, ledger, tmp_path):
    tasks, task, _ = router
    enable(tasks, settings, tmp_path)
    monkeypatch.setattr(tasks, 'check_aggregation_reports', MagicMock(return_value={'path': {'wrongsetstatus': ['WRITTEN_OFF']}}))
    send = MagicMock()
    monkeypatch.setattr(tasks, 'sign_and_send_utilisation', send)
    assert isinstance(task.apply(args=[{'bucket': 'internal-bucket', 'key': 'equipment-reports/report.json'}]).result, TerminalRecorded)
    assert ledger.pending()[0]['category'] == 'report_status_rejected'
    send.assert_not_called()
