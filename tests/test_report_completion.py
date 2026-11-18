"""A completed scan stays completed when its goods subsequently leave circulation."""
import hashlib
import json
from unittest.mock import MagicMock

import pytest
from celery.exceptions import Retry

from xtrek import equipment_progress as progress, operation_state, utils
from xtrek import terminal_feedback as feedback, terminal_feedback_runtime as runtime
from test_equipment_report_prerequisites import router
from test_terminal_feedback import settings, enable
from test_terminal_feedback_s3 import Store


@pytest.fixture
def completed(monkeypatch):
    rid = 'T-completed-report'
    key = f'equipment-reports/{rid}.json'
    source_path = 's3://internal-bucket/' + key
    config = {name: 's3://internal-bucket/' + name
              for name in ('aggs', 'agg-receipts', 'operation_state_path')}
    codes = ['010461011763608321FIRST', '010461011763608321SECOND']
    source = {'readyBox': [{'boxNumber': '346070517902888592',
                           'productNumbersFull': codes}]}
    status = {'number': 'aggregation-document', 'type': 'AGGREGATION_DOCUMENT',
              'status': 'CHECKED_OK', 'productionOrderId': rid, 'senderInn': '7733154124'}
    receipt = {'document_id': status['number'], 'productionOrderId': rid}
    guard = {'operation': 'prepare-aggregation', 'business_key': rid,
             'phase': 'complete', 'input_hash': operation_state.fingerprint(json.dumps({
                 'source': operation_state.fingerprint(json.dumps(source)),
                 'parameters': {'inn_override': None}}))}
    paths = {'status': config['aggs'] + '/' + rid + '.json',
             'receipt': config['agg-receipts'] + '/' + rid + '.json',
             'guard': config['operation_state_path'] + '/prepare-aggregation/'
                      + hashlib.sha256(rid.encode()).hexdigest() + '.json'}
    objects = {paths['status']: status, paths['receipt']: receipt, paths['guard']: guard}
    store = MagicMock()
    store.read_lock_object.side_effect = lambda path: (
        (json.dumps(objects[path]), 'etag') if path in objects else None)
    store.read_text.side_effect = lambda path: json.dumps(source) if path == source_path else None
    monkeypatch.setattr(progress, 'get_storage', lambda *args: store)
    return rid, key, source_path, config, source, objects, paths, store


@pytest.mark.parametrize('as_list', [False, True])
def test_completion_matches_final_document_and_same_source(completed, as_list):
    rid, _, path, config, source, objects, paths, store = completed
    if as_list:
        objects[paths['status']] = [objects[paths['status']]]
    # Formatting/key order changes do not change the scanned contents.
    store.read_text.side_effect = lambda path: json.dumps(source, indent=4, sort_keys=True)
    assert progress.report_completed(rid, path, config)
    store.set_tags.assert_not_called()
    store.write_lock_object.assert_not_called()
    store.upload.assert_not_called()


@pytest.mark.parametrize('inn_override', [None, '7733154124', '500100732259'])
def test_completion_uses_the_real_preparation_guard_fingerprint(
        completed, tmp_path, monkeypatch, inn_override):
    from xtrek.storage import LocalStorage

    rid, _, path, config, source, objects, paths, store = completed
    if inn_override:
        objects[paths['status']]['senderInn'] = inn_override
    local_source = tmp_path / 'report.json'
    local_source.write_text(json.dumps(source))
    local = LocalStorage()
    config['operation_state_path'] = str(tmp_path / 'guards')
    monkeypatch.setattr(operation_state, 'get_storage', lambda *args: local)

    @operation_state.guarded('prepare-aggregation',
                            input_path=lambda cfg, key: str(local_source),
                            config_loader=lambda name: config)
    def prepare(task_uuid, inn_override=None):
        return task_uuid

    assert prepare(rid, inn_override=inn_override) == rid
    monkeypatch.setattr(progress, 'get_storage', lambda target, *args:
                        store if target.startswith('s3://') else local)
    assert progress.report_completed(rid, path, config)
    source['readyBox'][0]['productNumbersFull'].append('010461011763608321NEW')
    assert not progress.report_completed(rid, path, config)


@pytest.mark.parametrize('change', [
    'missing_status', 'in_progress', 'rejected', 'multiple_statuses', 'wrong_type',
    'wrong_status_order', 'missing_receipt', 'wrong_receipt_order', 'wrong_document',
    'missing_guard', 'unfinished_guard', 'wrong_operation', 'wrong_guard_order',
    'changed_codes', 'changed_box', 'changed_pallet', 'different_event_object',
])
def test_incomplete_or_unrelated_evidence_cannot_skip_validation(completed, change):
    rid, _, path, config, source, objects, paths, store = completed
    status, receipt, guard = [objects[paths[k]] for k in ('status', 'receipt', 'guard')]
    if change == 'missing_status': del objects[paths['status']]
    if change == 'in_progress': status['status'] = 'IN_PROGRESS'
    if change == 'rejected': status['status'] = 'CHECKED_NOT_OK'
    if change == 'multiple_statuses': objects[paths['status']] = [status, dict(status)]
    if change == 'wrong_type': status['type'] = 'LP_INTRODUCE_GOODS'
    if change == 'wrong_status_order': status['productionOrderId'] = 'another-report'
    if change == 'missing_receipt': del objects[paths['receipt']]
    if change == 'wrong_receipt_order': receipt['productionOrderId'] = 'another-report'
    if change == 'wrong_document': receipt['document_id'] = 'another-document'
    if change == 'missing_guard': del objects[paths['guard']]
    if change == 'unfinished_guard': guard['phase'] = 'ready'
    if change == 'wrong_operation': guard['operation'] = 'prepare-introduce'
    if change == 'wrong_guard_order': guard['business_key'] = 'another-report'
    if change == 'changed_codes': source['readyBox'][0]['productNumbersFull'][0] += 'NEW'
    if change == 'changed_box': source['readyBox'][0]['boxNumber'] = '346070517902888615'
    if change == 'changed_pallet':
        source.update(schemaVersion=2, readyPallet=[{'palletNumber': '346070517902888615',
                                                   'palletAggregate': True,
                                                   'readyBox': source.pop('readyBox')}])
    if change == 'different_event_object':
        path = 's3://internal-bucket/other/' + rid + '.json'
        store.read_text.side_effect = lambda actual: json.dumps({'readyBox': []})
    assert not progress.report_completed(rid, path, config)


@pytest.mark.parametrize('backend', ['sqlite', 's3'])
@pytest.mark.parametrize('inn_override', [None, '7733154124'])
def test_completed_then_retired_replays_never_block_or_notify(
        completed, router, settings, tmp_path, monkeypatch, backend, inn_override):
    rid, key, path, config, source, objects, paths, store = completed
    objects[paths['guard']]['input_hash'] = operation_state.fingerprint(json.dumps({
        'source': operation_state.fingerprint(json.dumps(source)),
        'parameters': {'inn_override': inn_override}}))
    tasks, task, _ = router
    if backend == 's3':
        ledger_store = Store(str(tmp_path / 'outbox.db'))
        monkeypatch.setattr('xtrek.storage.get_storage', lambda *args: ledger_store)
        settings.update(ledger_backend='s3', ledger_path='s3://shared/events')
    enable(tasks, settings, tmp_path)
    tasks.config.update(config)
    api, nk = MagicMock(), MagicMock()
    codes = source['readyBox'][0]['productNumbersFull']
    api.get_list_cis_info.return_value = [
        {'cisInfo': {'cis': c, 'status': 'INTRODUCED'}} for c in codes]
    nk.product_info.return_value = {'isSet': False}
    monkeypatch.setattr(utils, 'get_storage', lambda *args: store)
    checker = MagicMock(side_effect=lambda *args: {
        path: utils.AggregationAnalyzer(api, nk).check_report(path, strict_prerequisites=True)})
    monkeypatch.setattr(tasks, 'check_aggregation_reports', checker)
    business = MagicMock(side_effect=AssertionError('No repeated business operation'))
    for name in ('create_utilisation_task_from_report', 'sign_and_send_utilisation',
                 'create_introduce_task_from_report', 'sign_and_send_introduce',
                 'create_aggregation_report', 'sign_and_send_aggregation'):
        monkeypatch.setattr(tasks, name, business)
    # Also prove the success shortcut does not enter/cache a route guard.
    route_store = MagicMock(side_effect=AssertionError('No initial route guard'))
    monkeypatch.setattr(operation_state, 'get_storage', route_store)
    data = {'bucket': 'internal-bucket', 'key': key}
    assert task.apply(args=[data]).successful()
    api.get_list_cis_info.return_value[0]['cisInfo']['status'] = 'RETIRED'
    for retries in [1, 201, 1200]:
        result = task.apply(args=[data], retries=retries)
        assert result.successful()
        assert result.result is None
    checker.assert_not_called()
    api.get_list_cis_info.assert_not_called()
    business.assert_not_called()
    route_store.assert_not_called()
    assert store.read_text.call_count == 4  # Recheck source on every event.
    store.set_tags.assert_not_called()
    tasks.get_storage.return_value.set_tags.assert_not_called()
    assert runtime.open_ledger(settings, tasks.config).get(feedback.event_identity(data['bucket'], key)) is None
    transport = MagicMock()
    for _ in range(2):
        assert runtime.drain(settings, {}, read_json=MagicMock(), transport_factory=transport) == {}
    transport.assert_not_called()


@pytest.mark.parametrize('failure', ['s3', 'json'])
def test_unknown_completion_retries_without_notification(
        completed, router, settings, tmp_path, monkeypatch, failure):
    _, key, _, config, _, _, _, store = completed
    tasks, task, _ = router
    enable(tasks, settings, tmp_path)
    tasks.config.update(config)
    if failure == 's3': store.read_lock_object.side_effect = TimeoutError('unavailable')
    else: store.read_lock_object.side_effect = lambda path: ('not JSON', 'etag')
    checker = MagicMock(side_effect=AssertionError('No check of current CIS status'))
    monkeypatch.setattr(tasks, 'check_aggregation_reports', checker)
    monkeypatch.setattr(task, 'retry', MagicMock(side_effect=Retry()))
    assert task.apply(args=[{'bucket': 'internal-bucket', 'key': key}], retries=201).state == 'RETRY'
    checker.assert_not_called()
    assert not runtime.open_ledger(settings, tasks.config).pending()
    tasks.get_storage.return_value.set_tags.assert_not_called()


@pytest.mark.parametrize('change', ['no_final_document', 'in_progress', 'replaced_source'])
@pytest.mark.parametrize('cached_route', [False, True])
def test_unfinished_or_replaced_report_still_stops_once_on_rejected_code(
        completed, router, settings, tmp_path, monkeypatch, change, cached_route):
    from token_lock_store import MemoryLockStore

    _, key, path, config, source, objects, paths, store = completed
    tasks, task, _ = router
    enable(tasks, settings, tmp_path)
    tasks.config.update(config)
    if change == 'no_final_document': del objects[paths['status']]
    if change == 'in_progress': objects[paths['status']]['status'] = 'IN_PROGRESS'
    if change == 'replaced_source': source['readyBox'][0]['productNumbersFull'][0] += 'NEW'
    api, nk = MagicMock(), MagicMock()
    codes = source['readyBox'][0]['productNumbersFull']
    api.get_list_cis_info.return_value = [
        {'cisInfo': {'cis': '00' + source['readyBox'][0]['boxNumber'], 'status': 'NOT_FOUND'}},
        {'cisInfo': {'cis': codes[0], 'status': 'RETIRED'}},
        {'cisInfo': {'cis': codes[1], 'status': 'INTRODUCED'}}]
    nk.product_info.return_value = {'isSet': False}
    monkeypatch.setattr(utils, 'get_storage', lambda *args: store)
    checker = MagicMock(side_effect=lambda *args: {
        path: utils.AggregationAnalyzer(api, nk).check_report(path, strict_prerequisites=True)})
    monkeypatch.setattr(tasks, 'check_aggregation_reports', checker)
    route_store = MemoryLockStore()
    monkeypatch.setattr(operation_state, 'get_storage', lambda *args: route_store)
    if cached_route:
        old = operation_state.OperationState(route_store, config['operation_state_path'],
            'route-logic_start_equipment_reports', 'internal-bucket/' + key)
        old.acquire()
        old.finish('historical cached result')
    for retries in [0, 201, 1200]:
        result = task.apply(args=[{'bucket': 'internal-bucket', 'key': key}], retries=retries)
        assert isinstance(result.result, feedback.TerminalRecorded)
    checker.assert_called_once()
    ledger = runtime.open_ledger(settings, tasks.config)
    assert len(ledger.pending()) == 1
    assert ledger.pending()[0]['category'] == 'report_scan_conflict'
    transport = MagicMock()
    transport.return_value.find.side_effect = [None, 'message-once']
    assert runtime.drain(settings, {}, read_json=MagicMock(return_value={'operator': 'Alice'}),
                         transport_factory=transport) == {'CONFIRMED': 1}
    assert runtime.drain(settings, {}, read_json=MagicMock(), transport_factory=transport) == {}
    transport.return_value.post.assert_called_once()


@pytest.mark.parametrize('sender_inn', [None, '', 'other', '1234567890'])
def test_override_requires_matching_successful_document_sender(completed, sender_inn):
    rid, _, path, config, source, objects, paths, _ = completed
    objects[paths['guard']]['input_hash'] = operation_state.fingerprint(json.dumps({
        'source': operation_state.fingerprint(json.dumps(source)),
        'parameters': {'inn_override': '7733154124'}}))
    objects[paths['status']]['senderInn'] = sender_inn
    assert not progress.report_completed(rid, path, config)


def test_rechecking_complete_route_retains_exclusive_ownership():
    from token_lock_store import MemoryLockStore

    store = MemoryLockStore()
    args = (store, 's3://isolated/guards', 'route-logic_start_equipment_reports', 'report')
    original = operation_state.OperationState(*args)
    original.acquire()
    original.finish('old result')
    first = operation_state.OperationState(*args)
    first.acquire(recheck_complete=True)
    assert not first.cached
    assert first.value['phase'] == 'held'
    second = operation_state.OperationState(*args)
    with pytest.raises(operation_state.OperationBusy):
        second.acquire(recheck_complete=True)
    first.finish('new validated result')
    # Default behaviour remains cached for every other guarded operation.
    cached = operation_state.OperationState(*args)
    assert cached.acquire() == 'new validated result'
    assert cached.cached
