"""No production credentials/endpoints: real workflow + isolated CAS stores.

External call journals are asserted independently of final files, so overwriting
an output cannot conceal a second emission/document/SSCC allocation.
"""
import json
import multiprocessing
import os
from pathlib import Path
import time
from unittest.mock import MagicMock

import pytest
import requests

from xtrek.operation_state import (OperationState, OperationBusy, OperationConflict,
                                   ReconciliationRequired, fingerprint, publish_once)
from xtrek.storage import LocalStorage, S3Storage
from xtrek.suz import SUZ
from test_document_send_idempotency import signing_workflow, _DOCUMENT_CASES, _SIGN_INN, _SIGN_THUMB
from test_document_send_idempotency import FakeReceiptStorage
from token_lock_store import ProcessLockStore


def guard(store, key='JOB-1', digest='source-hash'):
    return OperationState(store, 's3://isolated/state', 'emission', key, digest)


def _compete(database, barrier, output, key):
    store = ProcessLockStore(database)
    operation = guard(store, key)
    barrier.wait(timeout=20)
    try:
        operation.acquire()
        if operation.cached:
            return
        # The journal models an externally accepted mutation, before output save.
        operation.external()
        with open(output, 'a') as journal:
            journal.write(str(os.getpid()) + '\n')
            journal.flush()
            os.fsync(journal.fileno())
        time.sleep(.05)
        operation.accepted({'orderId': 'ONE-ORDER'})
        operation.finish({'orderId': 'ONE-ORDER'})
    except (OperationBusy, ReconciliationRequired):
        return


def _crash(database, stage, output):
    operation = guard(ProcessLockStore(database))
    operation.acquire()
    if stage != 'before_external':
        operation.external()
        Path(output).write_text('accepted-by-api')
    if stage in {'accepted', 'complete'}:
        operation.accepted({'orderId': 'ONE-ORDER'})
    if stage == 'complete':
        operation.finish({'orderId': 'ONE-ORDER'})
    os._exit(23)


def test_30_duplicate_deliveries_three_independent_processes(tmp_path):
    context = multiprocessing.get_context('spawn')
    database, journal = str(tmp_path / 'state.db'), str(tmp_path / 'api-calls')
    store = ProcessLockStore(database)
    for _ in range(10):
        barrier = context.Barrier(3)
        children = [context.Process(target=_compete, args=(database, barrier, journal, 'SAME-JOB')) for _ in range(3)]
        for child in children:
            child.start()
        for child in children:
            child.join(25)
            assert child.exitcode == 0
    assert len(Path(journal).read_text().splitlines()) == 1
    assert guard(store, 'SAME-JOB').acquire() == {'orderId': 'ONE-ORDER'}


@pytest.mark.parametrize('stage', ['before_external', 'response_lost', 'accepted', 'complete'])
def test_hard_process_death_at_each_acceptance_boundary(tmp_path, stage):
    context = multiprocessing.get_context('spawn')
    database, output = str(tmp_path / 'state.db'), str(tmp_path / 'api-calls')
    store = ProcessLockStore(database)
    child = context.Process(target=_crash, args=(database, stage, output))
    child.start()
    child.join(25)
    assert child.exitcode == 23
    operation = guard(store)
    if stage == 'before_external':
        operation.acquire()  # PID death is proven locally, no external reservation.
        assert not Path(output).exists()
    elif stage == 'response_lost':
        with pytest.raises(ReconciliationRequired):
            operation.acquire()
        assert Path(output).read_text() == 'accepted-by-api'
    else:
        assert operation.acquire() == {'orderId': 'ONE-ORDER'}
        assert operation.cached


def test_foreign_owner_cannot_be_stolen_by_age(tmp_path):
    store = ProcessLockStore(str(tmp_path / 'state.db'))
    original = guard(store)
    original.acquire()
    state = dict(original.value, owner=dict(original.owner, host='unreachable-peer'), updated_at=0)
    store.write_lock_object(original.path, json.dumps(state), original.etag)
    with pytest.raises(OperationBusy):
        guard(store).acquire()


def test_stale_owner_cannot_release_or_finish_new_owner(tmp_path):
    store = ProcessLockStore(str(tmp_path / 'state.db'))
    original = guard(store)
    original.acquire()
    state = dict(original.value, owner_id='replacement-owner', phase='held')
    store.write_lock_object(original.path, json.dumps(state), original.etag)
    with pytest.raises(OperationBusy):
        original.release_safe()
    assert json.loads(store.read_lock_object(original.path)[0])['owner_id'] == 'replacement-owner'


def test_source_uuid_with_changed_business_payload_is_conflict(tmp_path):
    store = LocalStorage()
    first = OperationState(store, str(tmp_path), 'normalize', 'UUID', fingerprint('{"Quantity":1}'))
    first.acquire()
    first.finish('ORDER')
    duplicate = OperationState(store, str(tmp_path), 'normalize', 'UUID', fingerprint('{ "Quantity": 1 }'))
    assert duplicate.acquire() == 'ORDER'
    changed = OperationState(store, str(tmp_path), 'normalize', 'UUID', fingerprint('{"Quantity":2}'))
    with pytest.raises(OperationConflict):
        changed.acquire()


def test_lost_s3_connection_before_reservation_prevents_api_call(tmp_path):
    store = ProcessLockStore(str(tmp_path / 'state.db'))
    operation = guard(store)
    operation.acquire()
    store.write_lock_object = MagicMock(side_effect=OSError('S3 unavailable'))
    with pytest.raises(OSError):
        operation.external()
    assert operation.value['phase'] == 'held'


def test_successful_conditional_put_with_lost_reply_is_adopted_by_exact_readback(tmp_path):
    store = ProcessLockStore(str(tmp_path / 'state.db'))
    write = store.write_lock_object
    def lost_reply(*args):
        write(*args)
        raise OSError('response lost after successful CAS')
    store.write_lock_object = lost_reply
    operation = guard(store)
    operation.acquire()
    operation.external()
    operation.accepted({'orderId': 'ONE'})
    operation.finish({'orderId': 'ONE'})
    assert guard(store).acquire() == {'orderId': 'ONE'}


def test_immutable_output_is_never_overwritten(tmp_path):
    path = str(tmp_path / 'equipment.json')
    store = LocalStorage()
    publish_once(store, path, '{"palletNumbers":["ONE"]}')
    publish_once(store, path, '{ "palletNumbers": ["ONE"] }')
    with pytest.raises(OperationConflict):
        publish_once(store, path, '{"palletNumbers":["TWO"]}')
    assert json.loads(Path(path).read_text())['palletNumbers'] == ['ONE']


def _enable(ctx, tmp_path, monkeypatch):
    ctx['config']['operation_state_path'] = str(tmp_path / 'coordination')
    ctx['config']['signing'] = {'local_by_inn': {_SIGN_INN: {'thumbprint': _SIGN_THUMB}}}
    # Deny all real network requests even if an unexpected branch is reached.
    monkeypatch.setattr(requests.sessions.Session, 'request', MagicMock(side_effect=AssertionError('live API forbidden')))


def test_every_real_send_workflow_reuses_receipt_before_crypto_and_api(signing_workflow, tmp_path, monkeypatch):
    ctx = signing_workflow
    _enable(ctx, tmp_path, monkeypatch)
    first = ctx['execute']()
    for _ in range(29):
        assert ctx['execute']() == first
    ctx['local'].assert_called_once()
    if ctx['operation'] == 'emission':
        ctx['api'].order_create.assert_called_once()
    elif ctx['operation'] == 'utilisation':
        ctx['api'].utilisation_send.assert_called_once()
    else:
        ctx['api'].documents_create.assert_called_once()


def test_every_send_recovers_accepted_result_when_receipt_write_failed(signing_workflow, tmp_path, monkeypatch):
    ctx = signing_workflow
    _enable(ctx, tmp_path, monkeypatch)
    original = ctx['storage'].write_lock_object
    ctx['storage'].write_lock_object = MagicMock(side_effect=OSError('receipt storage offline'))
    with pytest.raises(OSError):
        ctx['execute']()
    assert not ctx['receipt'].exists()
    ctx['storage'].write_lock_object = original
    assert ctx['execute']()
    assert ctx['receipt'].exists()
    ctx['local'].assert_called_once()
    assert sum([ctx['api'].order_create.call_count, ctx['api'].utilisation_send.call_count,
                ctx['api'].documents_create.call_count]) == 1


def test_every_send_blocks_second_api_call_when_response_was_lost(signing_workflow, tmp_path, monkeypatch):
    ctx = signing_workflow
    _enable(ctx, tmp_path, monkeypatch)
    method = {'emission': 'order_create', 'utilisation': 'utilisation_send'}.get(ctx['operation'], 'documents_create')
    getattr(ctx['api'], method).side_effect = TimeoutError('accepted, then response lost')
    with pytest.raises(TimeoutError):
        ctx['execute']()
    with pytest.raises(ReconciliationRequired):
        ctx['execute']()
    getattr(ctx['api'], method).assert_called_once()
    assert not ctx['receipt'].exists()


def test_new_release_preserves_unresolved_legacy_document_lock(monkeypatch):
    from xtrek import create_emission_task_sample as flow
    lock = 's3://internal/documentSubmissionLocks/introduce/JOB.lock'
    storage = FakeReceiptStorage({lock: 'legacy unknown owner'})
    monkeypatch.setattr(flow, 'load_config', lambda _: {'operation_state_path': 's3://coord/state'})
    monkeypatch.setattr(flow, 'get_storage', lambda *a: storage)
    with pytest.raises(ReconciliationRequired):
        flow._claim_document_submission('s3://internal/introduceReceipts', {}, 'introduce', 'JOB')
    assert storage.acquired == storage.released == []


def test_failed_tag_read_cannot_overwrite_existing_downloaded_tag():
    storage = S3Storage.__new__(S3Storage)
    storage.s3 = MagicMock()
    storage.s3.get_object_tagging.side_effect = TimeoutError('tag read failed')
    with pytest.raises(TimeoutError):
        storage.set_tags('s3://test/file.csv', {'print-status': 'printed'})
    storage.s3.put_object_tagging.assert_not_called()


def test_failed_tag_write_cannot_report_success():
    storage = S3Storage.__new__(S3Storage)
    storage.s3 = MagicMock()
    storage.s3.get_object_tagging.return_value = {'TagSet': [{'Key': 'Downloaded', 'Value': 'true'}]}
    storage.s3.put_object_tagging.side_effect = TimeoutError('tag write failed')
    with pytest.raises(TimeoutError):
        storage.set_tags('s3://test/file.csv', {'print-status': 'printed'})


def test_initial_print_tags_are_part_of_the_same_atomic_s3_create():
    from urllib.parse import parse_qs
    storage = S3Storage.__new__(S3Storage)
    storage.s3 = MagicMock()
    storage.s3.put_object.return_value = {'ETag': 'first'}
    assert publish_once(storage, 's3://test/codes.json', '{"codes":["one"]}',
                        {'print-status': 'not-printed', 'productionOrderId': 'PROD'})
    request = storage.s3.put_object.call_args.kwargs
    assert request['IfNoneMatch'] == '*'
    assert parse_qs(request['Tagging']) == {'print-status': ['not-printed'], 'productionOrderId': ['PROD']}
    storage.s3.put_object_tagging.assert_not_called()


def test_explicit_rate_rejection_can_retry_without_ambiguous_send(signing_workflow, tmp_path, monkeypatch):
    ctx = signing_workflow
    _enable(ctx, tmp_path, monkeypatch)
    method = {'emission': 'order_create', 'utilisation': 'utilisation_send'}.get(ctx['operation'], 'documents_create')
    api_method = getattr(ctx['api'], method)
    successful = api_method.side_effect
    response = requests.Response()
    response.status_code = 429
    def reject_once(*args, **kwargs):
        if api_method.call_count == 1:
            raise requests.HTTPError('rejected by rate limit', response=response)
        return successful(*args, **kwargs)
    api_method.side_effect = reject_once
    with pytest.raises(requests.HTTPError):
        ctx['execute']()
    assert ctx['execute']()
    assert api_method.call_count == 2  # One rejected request, one accepted request.


@pytest.mark.parametrize('error', [requests.Timeout('lost'), requests.ConnectionError('broken')])
def test_suz_mutating_post_is_never_repeated_after_transport_error(tmp_path, monkeypatch, error):
    body, signature = tmp_path / 'body.json', tmp_path / 'signature.txt'
    body.write_text('{}')
    signature.write_text('c2ln')
    send = MagicMock(side_effect=error)
    monkeypatch.setattr(requests, 'post', send)
    api = SUZ(token='synthetic', omsId='synthetic', clientToken='synthetic')
    with pytest.raises(type(error)):
        api._send_signed_request('https://synthetic.invalid', str(body), str(signature), max_retries=200)
    send.assert_called_once()
