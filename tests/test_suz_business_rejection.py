import hashlib
import json
from unittest.mock import Mock, MagicMock

import pytest
import requests
from celery.exceptions import Retry

from xtrek.suz import SUZ
from xtrek.suz_rejection import SuzUtilisationRejected, classify_utilisation_rejection
from xtrek.operation_state import OperationState, ReconciliationRequired, OperationConflict, OperationBusy, guarded, before_external_request
from xtrek.storage import LocalStorage
from xtrek import terminal_feedback as feedback
from xtrek import terminal_feedback_runtime as runtime
from test_equipment_report_prerequisites import router
from test_terminal_feedback import settings, ledger, enable

BODY = b'{"attributes":{"expirationDate":"2020-09-01"},"sntins":["PRIVATE-CODE"]}'
REFUSAL = {'success': False, 'fieldErrors': [{'errorCode': 7745,
    'fieldName': 'attributes.expirationDate', 'fieldError': 'PRIVATE arbitrary text'}]}


def response(payload=REFUSAL, status=400):
    result = requests.Response()
    result.status_code = status
    result._content = json.dumps(payload).encode() if not isinstance(payload, bytes) else payload
    result.url = 'https://synthetic.invalid/utilisation'
    return result


@pytest.fixture(params=[7745, 7570, 7580])
def date_refusal(request):
    code = request.param
    field, category = {
        7745: ('attributes.expirationDate', 'suz_expiration_date_rejected'),
        7570: ('attributes.productionDate', 'suz_production_date_range_rejected'),
        7580: ('attributes.productionDate', 'suz_production_date_future_rejected'),
    }[code]
    value = {7745: '2020-09-01', 7570: '0001-09-07', 7580: '2027-09-01'}[code]
    body = json.dumps({'attributes': {field.split('.')[1]: value}, 'sntins': ['PRIVATE-CODE']}).encode()
    payload = {'success': False, 'fieldErrors': [{'errorCode': code, 'fieldName': field,
                                                'fieldError': 'PRIVATE arbitrary server text'}]}
    return code, field, category, value, body, payload


def api_send(tmp_path, monkeypatch, reply=None, body_bytes=BODY):
    body, signature = tmp_path / 'body.json', tmp_path / 'body.sig'
    body.write_bytes(body_bytes)
    signature.write_text('PRIVATE-SIGNATURE')
    api = SUZ(token='PRIVATE-TOKEN', omsId='synthetic-oms', clientToken='synthetic-client')
    post = Mock(return_value=response() if reply is None else reply)
    monkeypatch.setattr('xtrek.suz.requests.post', post)
    return lambda: api.utilisation_send(str(body), str(signature), max_retries=200), post


def test_confirmed_7745_uses_actual_submitted_bytes_and_redacts(tmp_path, monkeypatch, caplog):
    send, post = api_send(tmp_path, monkeypatch)
    with pytest.raises(SuzUtilisationRejected) as exc:
        send()
    post.assert_called_once()
    assert post.call_args.kwargs['allow_redirects'] is False
    assert exc.value.evidence == {'kind': 'suz-utilisation-rejected-v1', 'http_status': 400,
        'code': 7745, 'field': 'attributes.expirationDate', 'submitted_value': '2020-09-01',
        'request_sha256': hashlib.sha256(post.call_args.kwargs['data']).hexdigest()}
    assert '7745' in caplog.text
    assert 'PRIVATE' not in caplog.text + json.dumps(exc.value.evidence)
    assert feedback.classify(exc.value, retries=0, retry_limit=200) == 'suz_expiration_date_rejected'


@pytest.mark.parametrize('payload,status', [
    (REFUSAL, 401), (REFUSAL, 403), (REFUSAL, 429), (REFUSAL, 500), (REFUSAL, 503),
    ({'fieldErrors': REFUSAL['fieldErrors']}, 400),
    (dict(REFUSAL, success=0), 400), (dict(REFUSAL, success=True), 400),
    (dict(REFUSAL, reportId='already-accepted'), 400), (dict(REFUSAL, reportId=None), 400), (dict(REFUSAL, unknownEnvelope={}), 400),
    (dict(REFUSAL, fieldErrors=[]), 400), (dict(REFUSAL, fieldErrors=REFUSAL['fieldErrors'] * 2), 400),
    ({'success': False, 'fieldErrors': [{'errorCode': '7745', 'fieldName': 'attributes.expirationDate'}]}, 400),
    ({'success': False, 'fieldErrors': [{'errorCode': 7571, 'fieldName': 'attributes.productionDate'}]}, 400),
    ({'success': False, 'fieldErrors': [{'errorCode': 7745, 'fieldName': 'unexpected'}]}, 400),
    (b'<html>400 upstream</html>', 400), (b'{', 400), ([], 400), (None, 400),
    (json.dumps(REFUSAL), 400), (b'x' * 65537, 400),
])
def test_unproven_response_never_becomes_confirmed_rejection(payload, status):
    assert classify_utilisation_rejection(response(payload, status), BODY) is None


def guarded_send(tmp_path, send):
    root = str(tmp_path / 'guards')
    @guarded('send-utilisation', config_loader=lambda name: {'operation_state_path': root})
    def submit(identity):
        before_external_request()
        return send()
    return submit, root


def test_rejected_checkpoint_survives_restart_without_feedback_config(tmp_path, monkeypatch, date_refusal):
    code, field, category, value, body, payload = date_refusal
    send, post = api_send(tmp_path, monkeypatch, response(payload), body)
    submit, root = guarded_send(tmp_path, send)
    with pytest.raises(SuzUtilisationRejected) as first:
        submit('ORDER-1')
    guard = OperationState(LocalStorage(), root, 'send-utilisation', 'ORDER-1')
    saved = json.loads(guard.storage.read_lock_object(guard.path)[0])
    assert first.value.evidence['code'] == code
    assert first.value.evidence['field'] == field
    assert first.value.evidence['submitted_value'] == value
    assert first.value.evidence['request_sha256'] == hashlib.sha256(body).hexdigest()
    assert feedback.classify(first.value, retries=0, retry_limit=200) == category
    assert saved['phase'] == 'rejected' and saved['last_error'] == first.value.evidence
    assert 'PRIVATE' not in json.dumps(saved)
    # A fresh guard on another worker restores the same typed refusal before POST.
    with pytest.raises(SuzUtilisationRejected) as replay:
        guard.acquire()
    assert replay.value.evidence == first.value.evidence
    with pytest.raises(SuzUtilisationRejected):
        submit('ORDER-1')
    assert post.call_count == 1
    changed = OperationState(LocalStorage(), root, 'send-utilisation', 'ORDER-1', 'different-input')
    with pytest.raises(OperationConflict):
        changed.acquire()


@pytest.mark.parametrize('payload,status', [(b'<html>400</html>', 400), (REFUSAL, 503)])
def test_unproven_http_remains_uncertain_never_second_post(tmp_path, monkeypatch, payload, status):
    send, post = api_send(tmp_path, monkeypatch, response(payload, status))
    submit, root = guarded_send(tmp_path, send)
    with pytest.raises(requests.HTTPError):
        submit('ORDER-1')
    guard = OperationState(LocalStorage(), root, 'send-utilisation', 'ORDER-1')
    assert json.loads(guard.storage.read_lock_object(guard.path)[0])['phase'] == 'uncertain'
    with pytest.raises(ReconciliationRequired):
        submit('ORDER-1')
    assert post.call_count == 1


@pytest.mark.parametrize('lost_after_write', [False, True])
def test_rejection_checkpoint_failure_preserves_terminal_classification(tmp_path, monkeypatch, lost_after_write):
    send, post = api_send(tmp_path, monkeypatch)
    submit, root = guarded_send(tmp_path, send)
    original = OperationState.update
    def update(self, phase, result=None, **metadata):
        if phase == 'rejected':
            if lost_after_write:
                original(self, phase, result, **metadata)
            raise OSError('PRIVATE storage failure')
        return original(self, phase, result, **metadata)
    monkeypatch.setattr(OperationState, 'update', update)
    with pytest.raises(SuzUtilisationRejected) as exc:
        submit('ORDER-1')
    assert feedback.classify(exc.value, retries=0, retry_limit=200) == 'suz_expiration_date_rejected'
    guard = OperationState(LocalStorage(), root, 'send-utilisation', 'ORDER-1')
    assert json.loads(guard.storage.read_lock_object(guard.path)[0])['phase'] == ('rejected' if lost_after_write else 'external')
    with pytest.raises((ReconciliationRequired, OperationBusy)):
        guard.acquire()
    assert post.call_count == 1


def test_router_stops_first_attempt_and_delivers_default_without_business_replay(
        router, settings, ledger, tmp_path, monkeypatch, date_refusal):
    tasks, task, _ = router
    enable(tasks, settings, tmp_path)
    code, field, category, value, body, payload = date_refusal
    send, post = api_send(tmp_path, monkeypatch, response(payload), body)
    submit, root = guarded_send(tmp_path, send)
    tasks.config['operation_state_path'] = root
    business = Mock(side_effect=lambda key: submit('ORDER-1'))
    monkeypatch.setattr(tasks, 'logic_start_equipment_reports', business)
    retry = Mock(side_effect=Retry())
    monkeypatch.setattr(task, 'retry', retry)
    data = {'bucket': 'internal-bucket', 'key': 'equipment-reports/order.json'}
    assert isinstance(task.apply(args=[data]).result, feedback.TerminalRecorded)
    assert isinstance(task.apply(args=[data]).result, feedback.TerminalRecorded)
    post.assert_called_once(); business.assert_called_once(); retry.assert_not_called()
    assert ledger.pending()[0]['category'] == category
    factory = MagicMock(); factory.return_value.find.return_value = 'mid'
    assert runtime.drain(settings, {}, read_json=lambda *args: {}, transport_factory=factory) == {'CONFIRMED': 1}
    sent = factory.return_value.find.call_args.args[0]
    assert str(code) in sent['message_text'] and field in sent['message_text']
    assert 'Стадия: Отправка отчёта о нанесении' in sent['message_text']
    if code == 7570:
        assert '1970-01-01 — 2100-01-01' in sent['message_text']
    if code == 7580:
        assert 'позже текущей даты' in sent['message_text']
    if code in (7570, 7580):
        assert 'исправить' in sent['message_text'] and 'Повторную отправку согласовать отдельно' in sent['message_text']
    assert 'PRIVATE' not in sent['message_text']
    assert json.loads(sent['route_json'])['chat_id'] == 'chat-a'


def test_real_equipment_route_rejects_before_downstream_and_freezes_both_guards(
        router, settings, ledger, tmp_path, monkeypatch, date_refusal):
    tasks, task, _ = router
    enable(tasks, settings, tmp_path)
    code, field, category, value, body, payload = date_refusal
    send, post = api_send(tmp_path, monkeypatch, response(payload), body)
    submit, root = guarded_send(tmp_path, send)
    tasks.config['operation_state_path'] = root
    check = Mock(return_value={'report': None})
    prepare = Mock(return_value=True)
    downstream = Mock(side_effect=AssertionError('No downstream business after refusal'))
    monkeypatch.setattr(tasks, 'check_aggregation_reports', check)
    monkeypatch.setattr(tasks, 'create_utilisation_task_from_report', prepare)
    monkeypatch.setattr(tasks, 'sign_and_send_utilisation', lambda report_id, *args: submit(report_id))
    monkeypatch.setattr(tasks, 'get_production_order_data', downstream)
    retry = Mock(side_effect=Retry()); monkeypatch.setattr(task, 'retry', retry)
    data = {'bucket': 'internal-bucket', 'key': 'equipment-reports/order.json'}
    assert isinstance(task.apply(args=[data]).result, feedback.TerminalRecorded)
    rejected = []
    for operation, identity in [('send-utilisation', 'order'),
            ('route-logic_start_equipment_reports', 'internal-bucket/equipment-reports/order.json')]:
        g = OperationState(LocalStorage(), root, operation, identity)
        saved = json.loads(g.storage.read_lock_object(g.path)[0])
        assert saved['phase'] == 'rejected'
        rejected.append(saved['last_error'])
    assert rejected[0] == rejected[1]
    assert isinstance(task.apply(args=[data]).result, feedback.TerminalRecorded)
    check.assert_called_once(); prepare.assert_called_once(); post.assert_called_once()
    downstream.assert_not_called(); retry.assert_not_called()


def test_failed_notification_record_uses_persistence_only_retry(
        router, settings, ledger, tmp_path, monkeypatch, date_refusal):
    tasks, task, _ = router
    enable(tasks, settings, tmp_path)
    code, field, category, value, body, payload = date_refusal
    send, post = api_send(tmp_path, monkeypatch, response(payload), body)
    submit, root = guarded_send(tmp_path, send)
    monkeypatch.setattr(tasks, 'logic_start_equipment_reports', lambda key: submit('order'))
    original = feedback.TerminalLedger.record
    monkeypatch.setattr(feedback.TerminalLedger, 'record', Mock(side_effect=OSError()))
    retry = Mock(side_effect=Retry()); monkeypatch.setattr(task, 'retry', retry)
    data = {'bucket': 'internal-bucket', 'key': 'equipment-reports/order.json'}
    assert task.apply(args=[data]).state == 'RETRY'
    persisted = retry.call_args.kwargs
    assert persisted['kwargs'] == {'data': data, 'terminal_failure': category}
    assert persisted['max_retries'] is None
    monkeypatch.setattr(feedback.TerminalLedger, 'record', original)
    assert isinstance(task.apply(kwargs=persisted['kwargs']).result, feedback.TerminalRecorded)
    assert ledger.pending()[0]['category'] == category
    post.assert_called_once()


def test_success_still_caches_and_429_still_defers(tmp_path, monkeypatch):
    send, post = api_send(tmp_path, monkeypatch, response({'reportId': 'accepted-id'}, 200))
    submit, root = guarded_send(tmp_path, send)
    assert submit('accepted') == 'accepted-id'
    assert submit('accepted') == 'accepted-id'
    post.assert_called_once()
    post.return_value = response(REFUSAL, 429)
    with pytest.raises(requests.HTTPError):
        submit('rate-limited')
    g = OperationState(LocalStorage(), root, 'send-utilisation', 'rate-limited')
    assert json.loads(g.storage.read_lock_object(g.path)[0])['phase'] == 'ready'


def test_network_failure_remains_uncertain(tmp_path, monkeypatch):
    send, post = api_send(tmp_path, monkeypatch)
    post.side_effect = requests.Timeout('synthetic timeout')
    submit, root = guarded_send(tmp_path, send)
    with pytest.raises(requests.Timeout):
        submit('order')
    g = OperationState(LocalStorage(), root, 'send-utilisation', 'order')
    assert json.loads(g.storage.read_lock_object(g.path)[0])['phase'] == 'uncertain'
    with pytest.raises(ReconciliationRequired):
        submit('order')
    post.assert_called_once()


def _competing_rejection_worker(store_path):
    from token_lock_store import ProcessLockStore
    from xtrek.terminal_feedback_s3 import S3TerminalLedger
    store = ProcessLockStore(store_path)
    guard = OperationState(store, 's3://synthetic/guards', 'send-utilisation', 'order')
    ledger = S3TerminalLedger(store, 's3://synthetic/terminal')
    posted = 0
    try:
        guard.acquire()
        guard.external()
        posted = 1  # Synthetic business POST; no network transport exists here.
        refusal = classify_utilisation_rejection(response(), BODY)
        guard.failed(refusal)
        raise refusal
    except SuzUtilisationRejected as refusal:
        row = ledger.record('internal-bucket', 'equipment-reports/order.json',
                            'report-validation', refusal.category)
        ledger.bind_route(row['event_id'], 'order', None,
                          {'transport': 'max', 'chat_id': 'fake', 'bot_user_id': 'fake', 'token_env': 'FAKE'})
        return posted, ledger.claim_post(row['event_id'])
    except OperationBusy:
        return posted, False


def test_competing_processes_share_rejected_fence_and_one_notification_reservation(tmp_path):
    import multiprocessing
    from concurrent.futures import ProcessPoolExecutor
    from token_lock_store import ProcessLockStore
    path = str(tmp_path / 'shared-synthetic-cas.db')
    store = ProcessLockStore(path)
    with ProcessPoolExecutor(4, mp_context=multiprocessing.get_context('spawn')) as pool:
        results = list(pool.map(_competing_rejection_worker, [path] * 12))
    assert sum(row[0] for row in results) == 1
    assert sum(row[1] for row in results) == 1
    guard = OperationState(store, 's3://synthetic/guards', 'send-utilisation', 'order')
    assert json.loads(store.read_lock_object(guard.path)[0])['phase'] == 'rejected'


@pytest.mark.parametrize('code,field', [(7570, 'attributes.productionDate'), (7580, 'attributes.productionDate')])
@pytest.mark.parametrize('mutation', ['wrong-field', 'string-code', 'mixed-errors', 'accepted-id', 'success', 'server-error'])
def test_production_rejections_require_exact_confirmed_contract(code, field, mutation):
    payload = {'success': False, 'fieldErrors': [{'errorCode': code, 'fieldName': field}]}
    status = 400
    if mutation == 'wrong-field':
        payload['fieldErrors'][0]['fieldName'] = 'attributes.expirationDate'
    elif mutation == 'string-code':
        payload['fieldErrors'][0]['errorCode'] = str(code)
    elif mutation == 'mixed-errors':
        payload['fieldErrors'].append({'errorCode': 9999, 'fieldName': field})
    elif mutation == 'accepted-id':
        payload['reportId'] = 'accepted'
    elif mutation == 'success':
        payload['success'] = True
    elif mutation == 'server-error':
        status = 500
    assert classify_utilisation_rejection(response(payload, status), BODY) is None


@pytest.mark.parametrize('code', [7570, 7580])
def test_restored_production_evidence_rejects_mismatched_field(code):
    error = classify_utilisation_rejection(response({'success': False, 'fieldErrors': [
        {'errorCode': code, 'fieldName': 'attributes.productionDate'}]}), BODY)
    assert error.evidence['submitted_value'] is None
    with pytest.raises(ValueError):
        SuzUtilisationRejected(dict(error.evidence, field='attributes.expirationDate'))
