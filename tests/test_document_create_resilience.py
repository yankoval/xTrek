import json
import multiprocessing
import time
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
import requests

from xtrek.document_create_resilience import (
    DocumentCreateCircuit, DocumentCreateDeferred, DocumentCreateUncertain,
    bind_endpoint, send_document,
)
from xtrek.operation_state import (
    OperationState, ReconciliationRequired, before_external_request,
    guarded, current_operation, remember_external_result,
)
from xtrek.storage import LocalStorage

URL = 'https://markirovka.crpt.ru/api/v3/true-api/lk/documents/create'


def response(status=200, text='"DOC-1"', headers=None):
    r = requests.Response()
    r.status_code = status
    r._content = text.encode()
    r.headers.update(headers or {})
    r.url = URL
    return r


def circuit(tmp_path, clock):
    return DocumentCreateCircuit(LocalStorage(), str(tmp_path), URL, clock=clock)


def trip(c):
    for _ in range(3):
        c.finish(c.admit(), 503, failed=True)


def test_shared_threshold_and_bounded_probe_backoff(tmp_path):
    now = [100.0]
    c = circuit(tmp_path, lambda: now[0])
    trip(c)
    with pytest.raises(DocumentCreateDeferred) as e:
        c.admit()
    assert e.value.next_retry_at == 130
    assert e.value.retry_after == 30
    delays = []
    for _ in range(8):
        data, _ = c.read()
        now[0] = data['next_retry_at'] + 1
        permit = c.admit()
        assert permit['probe']
        c.finish(permit, 503, failed=True)
        data, _ = c.read()
        delays.append(data['next_retry_at'] - now[0])
    assert delays == [60, 120, 240, 300, 300, 300, 300, 300]


def test_success_only_closes_matching_half_open_probe(tmp_path):
    now = [100.0]
    c = circuit(tmp_path, lambda: now[0])
    old = c.admit()
    trip(c)
    c.finish(old, 200)
    assert c.read()[0]['next_retry_at'] == 130
    now[0] = 131
    permit = c.admit()
    with pytest.raises(DocumentCreateDeferred):
        c.admit()
    c.finish(permit, 200)
    assert c.read()[0]['next_retry_at'] == 0
    assert not c.admit()['probe']


def test_duplicate_callback_does_not_increment_failures(tmp_path):
    c = circuit(tmp_path, lambda: 100)
    permit = c.admit()
    c.finish(permit, 503, failed=True)
    c.finish(permit, 503, failed=True)
    assert len(c.read()[0]['failures']) == 1


def _probe_process(root, barrier, queue):
    c = DocumentCreateCircuit(LocalStorage(), root, URL, clock=lambda: 131)
    barrier.wait(timeout=15)
    try:
        queue.put(('admitted', c.admit()['probe']))
    except DocumentCreateDeferred:
        queue.put(('deferred', True))


def test_four_workers_concurrency_one_only_one_probe(tmp_path):
    trip(circuit(tmp_path, lambda: 100))
    ctx = multiprocessing.get_context('spawn')
    barrier, queue = ctx.Barrier(4), ctx.Queue()
    children = [ctx.Process(target=_probe_process, args=(str(tmp_path), barrier, queue)) for _ in range(4)]
    for p in children:
        p.start()
    results = [queue.get(timeout=20) for _ in children]
    for p in children:
        p.join(timeout=20)
        assert p.exitcode == 0
    assert results.count(('admitted', True)) == 1
    assert results.count(('deferred', True)) == 3


def test_closed_admits_four_distinct_workers(tmp_path):
    circuits = [circuit(tmp_path, lambda: 100) for _ in range(4)]
    assert len({c.admit()['id'] for c in circuits}) == 4


def test_missing_corrupt_or_contended_control_state_fail_closed(tmp_path):
    c = circuit(tmp_path, lambda: 100)
    c.storage = SimpleNamespace(read_lock_object=lambda path: ('{"version":9}', 'tag'))
    with pytest.raises(DocumentCreateDeferred):
        c.admit()
    calls = []
    c.storage = SimpleNamespace(read_lock_object=lambda path: None,
                                write_lock_object=lambda *args: calls.append(args) or None)
    with pytest.raises(DocumentCreateDeferred):
        c.admit()
    assert len(calls) == c.CAS_ATTEMPTS


def workflow(tmp_path, body='{}'):
    root = str(tmp_path / 'operations')

    @guarded('send-introduce', config_loader=lambda name: {'operation_state_path': root})
    def submit(task_id):
        bind_endpoint(URL)
        before_external_request()
        result = send_document(URL, {'Authorization': 'Bearer PRIVATE-TOKEN'}, body, 'chemistry')
        remember_external_result(result)
        return result

    return submit, root


@pytest.mark.parametrize('status', [500, 502, 503, 504])
def test_5xx_one_post_durable_uncertain_and_restart_never_replays(tmp_path, monkeypatch, caplog, status):
    post = Mock(return_value=response(status, 'no healthy upstream PRIVATE-TOKEN PRIVATE-SIGNATURE 010123CODE'))
    monkeypatch.setattr(requests, 'post', post)
    submit, root = workflow(tmp_path, '{"signature":"PRIVATE-SIGNATURE","code":"010123CODE"}')
    caplog.set_level('INFO')
    with pytest.raises(DocumentCreateUncertain):
        submit('TASK-1')
    state = OperationState(LocalStorage(), root, 'send-introduce', 'TASK-1')
    saved = json.loads(state.storage.read_lock_object(state.path)[0])
    assert saved['phase'] == 'uncertain'
    assert saved['document_attempt_count'] == 1
    assert saved['last_error']['http_status'] == status
    assert saved['http_attempt']['http_status'] == status
    assert saved['next_retry_at'] is None
    assert post.call_args.kwargs['allow_redirects'] is False
    with pytest.raises(ReconciliationRequired):
        submit('TASK-1')
    assert post.call_count == 1
    for secret in ['PRIVATE-TOKEN', 'PRIVATE-SIGNATURE', '010123CODE']:
        assert secret not in caplog.text
        assert secret not in json.dumps(saved)


def test_transport_exception_never_logs_secret_or_retries_post(tmp_path, monkeypatch, caplog):
    post = Mock(side_effect=requests.ReadTimeout('PRIVATE-TOKEN PRIVATE-SIGNATURE'))
    monkeypatch.setattr(requests, 'post', post)
    submit, _ = workflow(tmp_path)
    caplog.set_level('INFO')
    with pytest.raises(DocumentCreateUncertain):
        submit('TASK-1')
    with pytest.raises(ReconciliationRequired):
        submit('TASK-1')
    assert post.call_count == 1
    assert 'PRIVATE-TOKEN' not in caplog.text
    assert 'PRIVATE-SIGNATURE' not in caplog.text


def test_open_gate_durable_next_retry_before_post(tmp_path, monkeypatch):
    post = Mock(return_value=response())
    monkeypatch.setattr(requests, 'post', post)
    submit, root = workflow(tmp_path)
    c = DocumentCreateCircuit(LocalStorage(), root, URL)
    trip(c)
    with pytest.raises(DocumentCreateDeferred):
        submit('NEW-TASK')
    assert post.call_count == 0
    st = OperationState(LocalStorage(), root, 'send-introduce', 'NEW-TASK')
    saved = json.loads(st.storage.read_lock_object(st.path)[0])
    assert saved['phase'] == 'ready'
    assert saved['next_retry_at'] > time.time()
    assert saved['last_error']['business_post_started'] is False
    # A controller closing the gate does not erase this operation's durable ETA.
    data, tag = c.read()
    data.update(next_retry_at=0, probe=None, failures=[])
    assert c.write(data, tag) is not None
    with pytest.raises(DocumentCreateDeferred):
        submit('NEW-TASK')
    assert post.call_count == 0


def test_success_caches_result_and_no_replay_after_ack_loss(tmp_path, monkeypatch):
    post = Mock(return_value=response())
    monkeypatch.setattr(requests, 'post', post)
    submit, root = workflow(tmp_path)
    assert submit('TASK-1') == 'DOC-1'
    assert submit('TASK-1') == 'DOC-1'
    assert post.call_count == 1
    saved = json.loads(OperationState(LocalStorage(), root, 'send-introduce', 'TASK-1').storage.read_lock_object(
        OperationState(LocalStorage(), root, 'send-introduce', 'TASK-1').path)[0])
    assert saved['phase'] == 'complete'
    assert saved['http_attempt']['http_status'] == 200


def test_one_transport_use_per_guard_even_if_caller_catches_error(tmp_path, monkeypatch):
    post = Mock(return_value=response(503))
    monkeypatch.setattr(requests, 'post', post)

    @guarded('send-introduce', config_loader=lambda name: {'operation_state_path': str(tmp_path)})
    def submit(task_id):
        bind_endpoint(URL)
        before_external_request()
        with pytest.raises(DocumentCreateUncertain):
            send_document(URL, {}, '{}', 'chemistry')
        with pytest.raises(ReconciliationRequired):
            send_document(URL, {}, '{}', 'chemistry')
        with pytest.raises(ReconciliationRequired):
            before_external_request()
        raise ReconciliationRequired('Caller cannot replay')

    with pytest.raises(ReconciliationRequired):
        submit('TASK-1')
    assert post.call_count == 1


def test_response_correlation_cannot_echo_bearer_token(tmp_path, monkeypatch, caplog):
    secret = '9cf1d91a-606a-4f71-952c-3f4685c2a051'
    post = Mock(return_value=response(503, 'failure', {'X-Request-ID': secret}))
    monkeypatch.setattr(requests, 'post', post)
    caplog.set_level('INFO')
    with pytest.raises(DocumentCreateUncertain):
        send_document(URL, {'Authorization': 'Bearer ' + secret}, '{}', 'chemistry')
    assert secret not in caplog.text


def test_redirect_does_not_resubmit(tmp_path, monkeypatch):
    post = Mock(return_value=response(307, '', {'Location': 'https://other.test'}))
    monkeypatch.setattr(requests, 'post', post)
    submit, _ = workflow(tmp_path)
    with pytest.raises(DocumentCreateUncertain):
        submit('TASK-1')
    assert post.call_count == 1
    assert post.call_args.kwargs['allow_redirects'] is False


def test_non_document_guard_unchanged(tmp_path):
    state = OperationState(LocalStorage(), str(tmp_path), 'allocate-pallet-sscc', 'T-1')
    state.acquire()
    state.external()
    assert state.value['phase'] == 'external'
    assert 'document_attempt_count' not in state.value

def test_429_keeps_existing_explicit_rejection_retry_behavior(tmp_path, monkeypatch):
    post = Mock(side_effect=[response(429, 'rate limit'), response()])
    monkeypatch.setattr(requests, 'post', post)
    submit, root = workflow(tmp_path)
    with pytest.raises(requests.HTTPError):
        submit('RATE-TASK')
    state = OperationState(LocalStorage(), root, 'send-introduce', 'RATE-TASK')
    assert json.loads(state.storage.read_lock_object(state.path)[0])['phase'] == 'ready'
    assert submit('RATE-TASK') == 'DOC-1'
    saved = json.loads(state.storage.read_lock_object(state.path)[0])
    assert saved['document_attempt_count'] == 2
    assert post.call_count == 2


def test_control_finish_failure_cannot_discard_success(tmp_path, monkeypatch):
    monkeypatch.setattr(requests, 'post', Mock(return_value=response()))
    monkeypatch.setattr(DocumentCreateCircuit, 'finish', Mock(side_effect=OSError('private storage detail')))
    submit, _ = workflow(tmp_path)
    assert submit('TASK-1') == 'DOC-1'
    assert submit('TASK-1') == 'DOC-1'
    assert requests.post.call_count == 1


def test_attempt_checkpoint_failure_preserves_accepted_reply(tmp_path, monkeypatch):
    monkeypatch.setattr(requests, 'post', Mock(return_value=response()))
    original = OperationState.update

    def update(self, phase, result=None, **metadata):
        if metadata.get('http_attempt', {}).get('finished_at'):
            raise OSError('private storage detail')
        return original(self, phase, result, **metadata)

    monkeypatch.setattr(OperationState, 'update', update)
    submit, _ = workflow(tmp_path)
    assert submit('TASK-1') == 'DOC-1'
    assert submit('TASK-1') == 'DOC-1'
    assert requests.post.call_count == 1
