"""Regression cases from PR #148 review; external writes are mocked."""
import copy
import json
from unittest.mock import MagicMock

import pytest

from xtrek import terminal_feedback as feedback, terminal_feedback_runtime as runtime
from test_equipment_report_prerequisites import router
from test_terminal_feedback import settings
from test_terminal_feedback_s3 import Store
from test_pending_scan_stop import scenario, check, ready_responses, wire_review_router, KEY


def missing_code_response(code, error_code='404', requested_at='nested'):
    # Observed True API response: no status, item-level errorCode (HTTP 200).
    row = {'cisInfo': {'cis': code}, 'errorCode': error_code, 'errorMessage': 'КМ/КИ не найден'}
    (row if requested_at == 'top' else row['cisInfo'])['requestedCis'] = code
    return row


@pytest.mark.parametrize('error_code', ['404', 404])
@pytest.mark.parametrize('requested_at', ['nested', 'top'])
def test_explicit_not_found_sscc_allows_utilisation(
        scenario, router, settings, tmp_path, monkeypatch, error_code, requested_at):
    responses = ready_responses(scenario)
    for row in responses:
        code = row['cisInfo'].get('cis') or row.get('requestedCis')
        if code.startswith('00'):
            row.clear()
            row.update(missing_code_response(code, error_code, requested_at))
    original = copy.deepcopy(responses)
    tasks, task, _, blocked = wire_review_router(scenario, router, settings, tmp_path, monkeypatch)
    create, send = MagicMock(return_value=True), MagicMock(return_value=True)
    monkeypatch.setattr(tasks, 'create_utilisation_task_from_report', create)
    monkeypatch.setattr(tasks, 'sign_and_send_utilisation', send)
    monkeypatch.setattr(tasks, 'get_production_order_data', lambda *a: {'GtinType': 'UNIT'})
    assert task.apply(args=[{'bucket': 'internal-bucket', 'key': KEY}]).successful()
    create.assert_called_once()
    send.assert_called_once()
    blocked.assert_not_called()
    assert not feedback.TerminalLedger(settings['ledger_path']).pending()
    assert responses == original  # Normalization does not mutate API payloads.
    scenario[2].set_tags.assert_called_with('s3://internal-bucket/' + KEY, {'check': ''})


@pytest.mark.parametrize('defect', ['identity', 'nested_identity', 'status', 'no_requested',
                                  'other_error', 'duplicate_first', 'duplicate_last'])
def test_unproven_not_found_remains_pending(
        scenario, router, settings, tmp_path, monkeypatch, defect):
    responses = ready_responses(scenario)
    target = next(r for r in responses if r['cisInfo'].get('cis', '').startswith('00'))
    code = target['cisInfo']['cis']
    target.clear()
    target.update(missing_code_response(code))
    if defect == 'identity':
        target['cisInfo']['requestedCis'] = '00999999999999999999'
    elif defect == 'nested_identity':
        target['requestedCis'] = code
        target['cisInfo']['requestedCis'] = '00999999999999999999'
    elif defect == 'status':
        target['cisInfo']['status'] = 'INTRODUCED'
    elif defect == 'no_requested':
        del target['cisInfo']['requestedCis']
    elif defect == 'other_error':
        target['errorCode'] = '503'
    else:
        other = {'cisInfo': {'cis': code, 'status': 'INTRODUCED'}}
        responses.insert(0 if defect == 'duplicate_first' else len(responses), other)
    tasks, task, _, blocked = wire_review_router(scenario, router, settings, tmp_path, monkeypatch)
    assert task.apply(args=[{'bucket': 'internal-bucket', 'key': KEY}]).state == 'RETRY'
    assert not feedback.TerminalLedger(settings['ledger_path']).pending()
    blocked.assert_not_called()


def test_explicit_missing_sscc_proves_partial_repeat(scenario):
    for row in scenario[1].get_list_cis_info.return_value:
        if row.get('requestedCis'):
            code = row['requestedCis']
            row.clear()
            row.update(missing_code_response(code))
    assert check(scenario).feedback_facts['scan_case'] == 'partial_repeat'


def test_explicit_missing_product_stops_report(scenario, router, settings, tmp_path, monkeypatch):
    responses = ready_responses(scenario)
    child = next(r for r in responses if r['cisInfo'].get('cis', '').startswith('01'))
    code = child['cisInfo']['cis']
    child.clear()
    child.update(missing_code_response(code))
    tasks, task, _, blocked = wire_review_router(scenario, router, settings, tmp_path, monkeypatch)
    assert isinstance(task.apply(args=[{'bucket': 'internal-bucket', 'key': KEY}]).result, feedback.TerminalRecorded)
    assert len(feedback.TerminalLedger(settings['ledger_path']).pending()) == 1
    blocked.assert_not_called()


@pytest.mark.parametrize('backend', ['sqlite', 's3'])
@pytest.mark.parametrize('aggregate', ['box', 'pallet'])
@pytest.mark.parametrize('state', ['INTRODUCED', 'APPLIED'])
def test_used_sscc_with_ready_contents_stops_and_notifies_once_without_progress(
        scenario, router, settings, tmp_path, monkeypatch, backend, aggregate, state):
    responses = ready_responses(scenario)
    if aggregate == 'box':
        next(r['cisInfo'] for r in responses if r['cisInfo'].get('cis', '').startswith('00'))['status'] = state
    else:
        source = json.loads(scenario[2].read_text.return_value)
        scenario[2].read_text.return_value = json.dumps({'schemaVersion': 2, 'readyPallet': [{
            'palletNumber': '123456789012345678', 'palletAggregate': True, 'readyBox': source['readyBox']}]})
        responses.append({'cisInfo': {'cis': '00123456789012345678', 'status': state}})
    progress = MagicMock(side_effect=feedback.WorkflowPending('Progress source unavailable'))
    monkeypatch.setattr('xtrek.equipment_progress.has_report_progress', progress)
    if backend == 's3':
        store = Store(str(tmp_path / 's3.db'))
        monkeypatch.setattr('xtrek.storage.get_storage', lambda *a: store)
        settings.update(ledger_backend='s3', ledger_path='s3://shared/events')
    tasks, task, checker, blocked = wire_review_router(scenario, router, settings, tmp_path, monkeypatch)
    for retries in [0, 200, 1200]:
        assert isinstance(task.apply(args=[{'bucket': 'internal-bucket', 'key': KEY}], retries=retries).result,
                          feedback.TerminalRecorded)
    progress.assert_not_called()
    checker.assert_called_once()
    blocked.assert_not_called()
    tasks.get_storage.return_value.set_tags.assert_called_with(
        's3://internal-bucket/' + KEY, {'check': 'report_aggregate_conflict'})
    assert len(runtime.open_ledger(settings, tasks.config).pending()) == 1
    factory = MagicMock()
    factory.return_value.find.side_effect = [None, 'mid-used-sscc']
    reader = MagicMock(return_value={'operator': 'Alice'})
    assert runtime.drain(settings, {}, read_json=reader, transport_factory=factory) == {'CONFIRMED': 1}
    assert runtime.drain(settings, {}, read_json=reader, transport_factory=factory) == {}
    factory.return_value.post.assert_called_once()
