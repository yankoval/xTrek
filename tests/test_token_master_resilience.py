"""Fault injection for master lifetime and ownership, without live tokens/S3."""
import json
import os
from pathlib import Path
import selectors
import signal
import subprocess
import sys
import time
from unittest.mock import MagicMock

import pytest
from botocore.exceptions import ClientError
from token_lock_store import MemoryLockStore, ProcessLockStore
from xtrek import crpt_auth, storage, token_master_lock
from xtrek.token_master_lock import TokenMasterLock, owner_identity, owner_is_dead
from xtrek.token_registry import TokenValidationError
from xtrek.token_runtime import (DeadlineExpired, MasterStopped, current_runtime,
                                 master_runtime)

PATH = 's3://synthetic/tokens-v2.json.master.lock'


def lock(store, tmp_path):
    return TokenMasterLock(store, PATH, {'tokens_master_local_lock_dir': str(tmp_path)})


def test_exception_releases_and_second_owner_can_acquire(tmp_path):
    store = MemoryLockStore()
    with pytest.raises(RuntimeError):
        with master_runtime():
            with lock(store, tmp_path).hold():
                raise RuntimeError('failure')
    assert not store.locked
    with master_runtime():
        with lock(store, tmp_path).hold():
            assert store.locked


def test_live_owner_cannot_be_reclaimed_even_after_expiry(tmp_path):
    store = MemoryLockStore()
    first = lock(store, tmp_path)
    first.acquire()
    body = json.loads(store.lock_object[0])
    body['expires_at'] = 0
    store.lock_object = (json.dumps(body), store.lock_object[1])
    with pytest.raises(TokenValidationError, match='already locked'):
        lock(store, tmp_path).acquire()


def test_conditional_takeover_loser_cannot_enter(tmp_path):
    store = MemoryLockStore()
    store.write_lock_object = lambda *args: None
    with pytest.raises(TokenValidationError, match='already locked'):
        lock(store, tmp_path).acquire()


def test_old_release_does_not_remove_new_owners_lock(tmp_path):
    store = MemoryLockStore()
    first = lock(store, tmp_path)
    first.acquire()
    first.release()
    second = lock(store, tmp_path)
    second.acquire()
    first.release()
    second.assert_owned()


def test_release_is_conditional_even_if_owner_changes_after_read(tmp_path):
    store = MemoryLockStore()
    first = lock(store, tmp_path)
    first.acquire()
    original = store.write_lock_object

    def concurrent_change(path, body, etag):
        store.lock_object = ('new owner state', 'new-etag')
        return original(path, body, etag)

    store.write_lock_object = concurrent_change
    with pytest.raises(TokenValidationError, match='changed during release'):
        first.release()
    assert store.lock_object == ('new owner state', 'new-etag')


def test_accepted_put_with_lost_response_is_released_without_issuance(tmp_path):
    store = MemoryLockStore()
    original = store.write_lock_object
    calls = 0

    def lost_response(path, body, etag):
        nonlocal calls
        calls += 1
        result = original(path, body, etag)
        if calls == 1:
            raise OSError('lost response')
        return result

    store.write_lock_object = lost_response
    with pytest.raises(OSError):
        with master_runtime():
            with lock(store, tmp_path).hold():
                pytest.fail('Unconfirmed ownership must not issue')
    assert not store.locked


@pytest.mark.parametrize('body', ['old-random-uuid', '{}', '[]'])
def test_legacy_or_malformed_lock_is_not_deleted(tmp_path, body):
    store = MemoryLockStore()
    store.lock_object = (body, 'old-etag')
    with pytest.raises(TokenValidationError, match='migration'):
        with master_runtime():
            with lock(store, tmp_path).hold():
                pytest.fail('Legacy lock must block')
    assert store.lock_object == (body, 'old-etag')


def test_foreign_owner_and_pid_reuse(monkeypatch):
    local = dict(host='host', machine='machine', boot='boot', pid=100, process_start='50')
    owner = dict(local)
    monkeypatch.setattr(os, 'kill', lambda *a: None)
    monkeypatch.setattr(token_master_lock, '_process_start', lambda pid: '100')
    assert owner_is_dead(owner, local)  # PID now names a different process.
    assert not owner_is_dead(dict(owner, host='other'), local)
    assert not owner_is_dead(dict(owner, machine='other'), local)
    assert not owner_is_dead(dict(owner, process_start=None), local)
    assert not owner_is_dead(dict(owner, pid_namespace='another-container'), local)
    assert owner_is_dead(dict(owner, boot='old-boot'), local)


def test_owner_probe_permission_error_fails_closed(monkeypatch):
    local = owner_identity()
    monkeypatch.setattr(os, 'kill', MagicMock(side_effect=PermissionError()))
    assert not owner_is_dead(local, local)


def test_runtime_interrupts_blocked_io_and_restores_handlers():
    previous = {s: signal.getsignal(s) for s in (signal.SIGTERM, signal.SIGINT, signal.SIGALRM)}
    started = time.monotonic()
    with pytest.raises(DeadlineExpired):
        with master_runtime({'tokens_master_cycle_seconds': 0.08}):
            time.sleep(30)
    assert time.monotonic() - started < 2
    assert current_runtime() is None
    assert signal.getitimer(signal.ITIMER_REAL) == (0.0, 0.0)
    assert all(signal.getsignal(s) == h for s, h in previous.items())


def test_scope_deadline_does_not_consume_publication_reserve():
    with master_runtime() as runtime:
        with runtime.budget(1):
            with pytest.raises(DeadlineExpired):
                with runtime.budget(0.05):
                    time.sleep(30)
            assert runtime.remaining() > 0.5


def test_cleanup_is_bounded_and_does_not_disable_normal_deadline():
    with master_runtime() as runtime:
        with pytest.raises(DeadlineExpired):
            with runtime.cleanup(0.05):
                time.sleep(30)
        assert signal.getitimer(signal.ITIMER_REAL)[0] > 0


def test_stop_during_cleanup_is_not_swallowed():
    with pytest.raises(MasterStopped):
        with master_runtime() as runtime:
            with runtime.cleanup(1):
                os.kill(os.getpid(), signal.SIGTERM)
            runtime.check()


def test_stopping_never_retries_http_issuance(monkeypatch):
    session = MagicMock()
    session.request.side_effect = MasterStopped('stop')
    with pytest.raises(MasterStopped):
        crpt_auth._request(session, 'POST', 'https://example.invalid/auth', attempts=1)
    assert session.request.call_count == 1


def test_signing_cancellation_cleans_own_objects(monkeypatch):
    store = MagicMock()
    store.exists.return_value = False
    monkeypatch.setattr(crpt_auth, 'get_storage', lambda *args: store)
    with pytest.raises(DeadlineExpired):
        with master_runtime({'tokens_master_cycle_seconds': 0.05}):
            crpt_auth.sign_data('synthetic', '1234567890', {'sign': 's3://test/sign'})
    assert store.write_text.call_count == 1
    assert store.exists.call_count >= 3  # waiting + cleanup for both paths


@pytest.mark.parametrize('code,status', [('AccessDenied', 403), ('SlowDown', 503)])
def test_lock_backend_failure_is_not_treated_as_missing(code, status):
    s3 = storage.S3Storage.__new__(storage.S3Storage)
    s3.s3 = MagicMock()
    s3.s3.get_object.side_effect = ClientError(
        {'Error': {'Code': code}, 'ResponseMetadata': {'HTTPStatusCode': status}}, 'GetObject')
    with pytest.raises(ClientError):
        s3.read_lock_object(PATH)


def test_s3_lock_writes_use_server_side_conditions():
    s3 = storage.S3Storage.__new__(storage.S3Storage)
    s3.s3 = MagicMock()
    s3.s3.put_object.return_value = {'ETag': 'etag'}
    assert s3.write_lock_object(PATH, '{}', None) == 'etag'
    assert s3.s3.put_object.call_args.kwargs['IfNoneMatch'] == '*'
    s3.write_lock_object(PATH, '{}', 'old-etag')
    assert s3.s3.put_object.call_args.kwargs['IfMatch'] == 'old-etag'
    s3.s3.delete_object.assert_not_called()


def test_master_s3_transfer_is_synchronous_and_bounded(monkeypatch, tmp_path):
    client = MagicMock()
    client.get_object.return_value['Body'].read.return_value = b'{}'
    factory = MagicMock(return_value=client)
    monkeypatch.setattr(storage.boto3, 'client', factory)
    with master_runtime():
        s3 = storage.S3Storage({})
        s3.download('s3://test/file', tmp_path / 'file')
        s3.upload(tmp_path / 'file', 's3://test/file')
    config = factory.call_args.kwargs['config']
    assert config.connect_timeout == 5 and config.read_timeout == 10
    assert config.retries['total_max_attempts'] == 2
    client.download_file.assert_not_called()
    client.upload_file.assert_not_called()
    client.put_object.assert_called_once()


CHILD = '''
import sys, time
from token_lock_store import ProcessLockStore
from xtrek.token_master_lock import TokenMasterLock
from xtrek.token_runtime import master_runtime, MasterStopped
store = ProcessLockStore(sys.argv[1])
try:
    with master_runtime():
        with TokenMasterLock(store, sys.argv[3], {'tokens_master_local_lock_dir': sys.argv[2]}).hold():
            print('ACQUIRED', flush=True)
            time.sleep(30)
except MasterStopped:
    raise SystemExit(1)
'''


def start_child(tmp_path):
    env = dict(os.environ)
    root = Path(__file__).resolve().parents[1]
    env['PYTHONPATH'] = os.pathsep.join((str(root), str(root / 'tests')))
    proc = subprocess.Popen([sys.executable, '-c', CHILD, str(tmp_path / 'store.db'),
                             str(tmp_path), PATH], stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, text=True, env=env)
    with selectors.DefaultSelector() as selector:
        selector.register(proc.stdout, selectors.EVENT_READ)
        if not selector.select(5):
            proc.kill()
            proc.communicate(timeout=5)
            pytest.fail('Child did not acquire lock')
    assert proc.stdout.readline().strip() == 'ACQUIRED'
    return proc


@pytest.mark.parametrize('sig', [signal.SIGTERM, signal.SIGKILL])
def test_next_process_recovers_after_termination(tmp_path, sig):
    proc = start_child(tmp_path)
    try:
        store = ProcessLockStore(str(tmp_path / 'store.db'))
        with pytest.raises(TokenValidationError, match='already locked'):
            with master_runtime():
                with lock(store, tmp_path).hold():
                    pytest.fail('Concurrent master entered')
        proc.send_signal(sig)
        proc.communicate(timeout=5)
        before = json.loads(store.read_lock_object(PATH)[0])
        assert before['state'] == ('released' if sig == signal.SIGTERM else 'held')
        with master_runtime():
            with lock(store, tmp_path).hold():
                after = json.loads(store.read_lock_object(PATH)[0])
                assert after['owner_id'] != before['owner_id']
        assert json.loads(store.read_lock_object(PATH)[0])['state'] == 'released'
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.communicate(timeout=5)
