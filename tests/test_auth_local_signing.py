"""Auth routing, safe diagnostics and durable SUZ fencing; no real keys/network."""
from datetime import datetime, timedelta, timezone
import base64
import json
import os
from pathlib import Path
import subprocess
import sys
from unittest.mock import MagicMock

import pytest
import requests

from token_lock_store import ProcessLockStore
from xtrek import crpt_auth, sign
from xtrek.token_master_lock import TokenMasterLock
from xtrek.token_registry import TokenRecord
from xtrek.token_runtime import master_runtime

INN = '7733154124'
GLOBAL = '9718180660'
SMART = '9723161905'
THUMB = 'A1' * 20
CON = '11111111-1111-4111-8111-111111111111'
OMS = '22222222-2222-4222-8222-222222222222'
VALUE = '33333333-3333-4333-8333-333333333333'
CHALLENGE = '44444444-4444-4444-8444-444444444444'
LOCK = 's3://synthetic/tokens.json.master.lock'


def config():
    return {'sign': 's3://synthetic/sign', 'tokens_signing': {'by_inn': {
        INN: {'mode': 'local', 'thumbprint': THUMB, 'pin_file': '/private/secret-pin'},
        GLOBAL: {'mode': 'storage'}, SMART: {'mode': 'storage'},
    }}}


def response(data, status=200):
    result = MagicMock(status_code=status)
    result.json.return_value = data
    return result


@pytest.fixture
def session(monkeypatch):
    session = MagicMock()
    session.__enter__.return_value = session
    monkeypatch.setattr(crpt_auth.requests, 'Session', lambda: session)
    monkeypatch.setattr(crpt_auth.time, 'sleep', lambda _: None)
    return session


def record():
    return TokenRecord('suz', 'production', INN, 'UUID', VALUE,
                       (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat(),
                       'server', crpt_auth.AUTH_BASES['production'], connection_id=CON,
                       oms_id=OMS, auth_uuid=CHALLENGE)


def lock(store, tmp_path):
    return TokenMasterLock(store, LOCK, {'tokens_master_local_lock_dir': str(tmp_path)})


def test_local_auth_never_accesses_sign_storage_and_preserves_literal_data(monkeypatch):
    backend = MagicMock(return_value=' c2ln\nbmF0dXJl ')
    storage = MagicMock(side_effect=AssertionError('storage must not be opened'))
    monkeypatch.setattr(crpt_auth, 'sign_token_data', backend)
    monkeypatch.setattr(crpt_auth, 'get_storage', storage)
    settings = config()
    settings.pop('sign')
    assert crpt_auth.sign_data('YWJj==\r\nТест', INN, settings) == 'c2lnbmF0dXJl'
    backend.assert_called_once_with('YWJj==\r\nТест', detached=False, thumbprint=THUMB,
                                    store_location='current_user', pin_file='/private/secret-pin',
                                    expected_inn=INN)
    storage.assert_not_called()


@pytest.mark.parametrize('inn', [GLOBAL, SMART, '1234567890'])
def test_storage_routes_and_omitted_inns_keep_signjs_protocol(monkeypatch, inn):
    storage = MagicMock()
    storage.read_text.return_value = 'c2lnbmF0dXJl'
    monkeypatch.setattr(crpt_auth, 'get_storage', lambda *a: storage)
    backend = MagicMock()
    monkeypatch.setattr(crpt_auth, 'sign_token_data', backend)
    crpt_auth.sign_data('literal', inn, config())
    source, data = storage.write_text.call_args.args
    assert source.startswith('s3://synthetic/sign/' + inn + '_') and source.endswith('.txt')
    assert data == 'literal'
    backend.assert_not_called()


def test_document_local_config_does_not_enable_auth_local_mode(monkeypatch):
    storage = MagicMock()
    storage.read_text.return_value = 'c2lnbmF0dXJl'
    monkeypatch.setattr(crpt_auth, 'get_storage', lambda *a: storage)
    backend = MagicMock()
    monkeypatch.setattr(crpt_auth, 'sign_token_data', backend)
    crpt_auth.sign_data('x', SMART, {'sign': '/synthetic', 'signing': {'local_by_inn': {
        SMART: {'thumbprint': THUMB}}}})
    storage.write_text.assert_called_once()
    backend.assert_not_called()


@pytest.mark.parametrize('purpose', ['true_api', 'suz'])
def test_delegated_identity_binds_personal_signer_and_keeps_organization_scope(session, monkeypatch, tmp_path, purpose):
    backend = MagicMock(return_value='c2lnbmF0dXJl')
    monkeypatch.setattr(crpt_auth, 'sign_token_data', backend)
    settings = config()
    settings['tokens_signing']['by_inn'][GLOBAL] = {
        'mode': 'local', 'thumbprint': THUMB, 'identity': 'delegated',
        'signer_inn': '123456789012'}
    expiry = (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()
    session.request.side_effect = [response({'uuid': CHALLENGE, 'data': 'literal'}),
        response({'token': VALUE, 'uuidToken': VALUE, 'expireDate': expiry}),
        response({'omsId': OMS, 'apiVersion': '3', 'omsVersion': '5'})]
    with master_runtime():
        with lock(ProcessLockStore(tmp_path / 'locks.sqlite'), tmp_path).hold():
            issued = crpt_auth.issue_token(GLOBAL, purpose=purpose, config=settings,
                connection_id=CON if purpose == 'suz' else None, oms_id=OMS if purpose == 'suz' else None)
    assert backend.call_args_list[0].kwargs['expected_inn'] == '123456789012'
    assert issued.inn == GLOBAL
    assert session.request.call_args_list[1].kwargs['json']['inn'] == GLOBAL


@pytest.mark.parametrize('extra', [
    {'identity': 'delegated'}, {'identity': 'delegated', 'signer_inn': INN},
    {'signer_inn': '123456789012'}, {'identity': 'auto', 'signer_inn': '123456789012'},
    {'identity': 'delegated', 'signer_inn': 123456789012},
])
def test_delegated_identity_requires_explicit_complete_binding_before_network(session, extra):
    settings = config()
    settings['tokens_signing']['by_inn'][GLOBAL] = {'mode': 'local', 'thumbprint': THUMB, **extra}
    with pytest.raises(crpt_auth.TokenIssuanceError) as error:
        crpt_auth.issue_token(GLOBAL, purpose='true_api', config=settings)
    assert error.value.reason == 'invalid_configuration'
    session.request.assert_not_called()


@pytest.mark.parametrize('options', [
    {'mode': 'auto'}, {'mode': 'local'}, {'mode': 'local', 'thumbprint': 'bad'},
    {'mode': 'storage', 'thumbprint': THUMB},
    {'mode': 'local', 'thumbprint': THUMB, 'pin': 'SECRET'},
    {'mode': 'local', 'thumbprint': THUMB, 'store_location': 'auto'},
])
def test_invalid_route_fails_before_challenge_request(session, options):
    settings = config()
    settings['tokens_signing']['by_inn'][INN] = options
    with pytest.raises(crpt_auth.TokenIssuanceError) as error:
        crpt_auth.issue_token(INN, purpose='true_api', config=settings)
    assert error.value.diagnostic == 'stage=configuration reason=invalid_configuration'
    session.request.assert_not_called()


@pytest.mark.parametrize('reason', ['private_key_missing', 'certificate_inn_mismatch',
                                   'certificate_validation_failed', 'SECRET native details'])
def test_local_failure_has_safe_reason_no_storage_fallback_or_auth_post(session, monkeypatch, reason):
    backend = MagicMock(side_effect=sign.SigningError('SECRET challenge PIN', reason=reason))
    monkeypatch.setattr(crpt_auth, 'sign_token_data', backend)
    storage = MagicMock()
    monkeypatch.setattr(crpt_auth, 'get_storage', storage)
    session.request.return_value = response({'uuid': CHALLENGE, 'data': 'SECRET challenge'})
    with pytest.raises(crpt_auth.TokenIssuanceError) as error:
        crpt_auth.issue_token(INN, purpose='true_api', config=config())
    assert error.value.stage == 'signing_local'
    assert 'SECRET' not in str(error.value) + error.value.diagnostic
    assert session.request.call_count == 1
    storage.assert_not_called()


@pytest.mark.parametrize('purpose', ['true_api', 'suz'])
def test_local_auth_issues_distinct_uuid_scopes_and_verifies_before_return(session, monkeypatch, tmp_path, purpose):
    backend = MagicMock(return_value='c2lnbmF0dXJl')
    monkeypatch.setattr(crpt_auth, 'sign_token_data', backend)
    settings = config()
    settings.pop('sign')
    settings['suz_ping_sign'] = True
    expiry = (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()
    session.request.side_effect = [response({'uuid': CHALLENGE, 'data': 'literal YQ=='}),
        response({'token': VALUE, 'uuidToken': VALUE, 'expireDate': expiry}),
        response({'omsId': OMS, 'apiVersion': '3', 'omsVersion': '5'})]
    with master_runtime():
        with lock(ProcessLockStore(tmp_path / 'locks.sqlite'), tmp_path).hold():
            issued = crpt_auth.issue_token(INN, purpose=purpose, config=settings,
                connection_id=CON if purpose == 'suz' else None, oms_id=OMS if purpose == 'suz' else None)
    assert issued.purpose == purpose and issued.format == 'UUID'
    assert issued.connection_id == (CON if purpose == 'suz' else None)
    assert backend.call_args_list[0].args == ('literal YQ==',)
    assert backend.call_args_list[0].kwargs['detached'] is False
    assert session.request.call_count == 3
    payload = session.request.call_args_list[1].kwargs['json']
    assert ('unitedToken' in payload) == (purpose == 'true_api')
    if purpose == 'suz':
        assert backend.call_args_list[1].args == ('/api/v3/ping?omsId=' + OMS,)
        assert backend.call_args_list[1].kwargs['detached'] is True


@pytest.mark.parametrize('failure', [requests.Timeout('SECRET'), response({}, 503), response({})])
def test_ambiguous_suz_post_is_fenced_across_subsequent_master_cycles(session, monkeypatch, tmp_path, failure):
    monkeypatch.setattr(crpt_auth, 'sign_token_data', lambda *a, **kw: 'c2lnbmF0dXJl')
    session.request.side_effect = [response({'uuid': CHALLENGE, 'data': 'literal'}), failure]
    store = ProcessLockStore(tmp_path / 'locks.sqlite')
    with master_runtime():
        with lock(store, tmp_path).hold():
            with pytest.raises((crpt_auth.TokenIssuanceError, ValueError)):
                crpt_auth.issue_token(INN, purpose='suz', connection_id=CON, oms_id=OMS, config=config())
    with master_runtime():
        with lock(ProcessLockStore(tmp_path / 'locks.sqlite'), tmp_path).hold():
            with pytest.raises(crpt_auth.TokenIssuanceError, match='requires reconciliation'):
                crpt_auth.issue_token(INN, purpose='suz', connection_id=CON, oms_id=OMS, config=config())
    assert session.request.call_count == 2
    assert sum(c.args[0] == 'POST' for c in session.request.call_args_list) == 1


def test_suz_guard_requires_writer_lock_before_any_network(session):
    with pytest.raises(crpt_auth.TokenIssuanceError) as error:
        crpt_auth.issue_token(INN, purpose='suz', connection_id=CON, oms_id=OMS, config=config())
    assert error.value.reason == 'writer_lock_required'
    session.request.assert_not_called()


def test_local_jwt_keeps_expiry_and_united_token_policy(session, monkeypatch):
    expiry = int((datetime.now(timezone.utc) + timedelta(hours=1)).timestamp())
    payload = base64.urlsafe_b64encode(json.dumps({'inn': INN, 'exp': expiry}).encode()).decode().rstrip('=')
    token = 'eyJhbGciOiJIUzI1NiJ9.' + payload + '.signature'
    backend = MagicMock(return_value='c2lnbmF0dXJl')
    monkeypatch.setattr(crpt_auth, 'sign_token_data', backend)
    session.request.side_effect = [response({'uuid': CHALLENGE, 'data': 'literal'}),
                                    response({'token': token}), response([])]
    issued = crpt_auth.issue_token(INN, purpose='true_api', token_format='JWT', config=config())
    assert issued.format == 'JWT' and issued.expiry_source == 'jwt_exp'
    assert issued.expireDate == datetime.fromtimestamp(expiry, timezone.utc).isoformat()
    assert session.request.call_args_list[1].kwargs['json']['unitedToken'] is False
    assert backend.call_args.kwargs['detached'] is False


def test_post_retry_parameter_is_rejected_before_transport(session):
    with pytest.raises(crpt_auth.TokenIssuanceError, match='cannot be retried'):
        crpt_auth._request(session, 'POST', 'https://synthetic.invalid', attempts=3)
    session.request.assert_not_called()


def test_bad_challenge_uuid_has_safe_stage_and_never_signs(session, monkeypatch):
    backend = MagicMock()
    monkeypatch.setattr(crpt_auth, 'sign_token_data', backend)
    session.request.return_value = response({'uuid': 'SECRET', 'data': 'SECRET'})
    with pytest.raises(crpt_auth.TokenIssuanceError) as error:
        crpt_auth.issue_token(INN, purpose='true_api', config=config())
    assert error.value.diagnostic == 'stage=challenge reason=invalid_challenge'
    assert 'SECRET' not in str(error.value)
    backend.assert_not_called()


@pytest.mark.parametrize('value', ['', 'SECRET not base64', None])
@pytest.mark.parametrize('mode', ['local', 'storage'])
def test_bad_signature_result_is_redacted_and_stops_issuance(session, monkeypatch, value, mode):
    settings = config()
    if mode == 'local':
        monkeypatch.setattr(crpt_auth, 'sign_token_data', lambda *a, **kw: value)
    else:
        settings['tokens_signing']['by_inn'][INN] = {'mode': 'storage'}
        storage = MagicMock()
        storage.read_text.return_value = value
        monkeypatch.setattr(crpt_auth, 'get_storage', lambda *a: storage)
    session.request.return_value = response({'uuid': CHALLENGE, 'data': 'literal'})
    with pytest.raises(crpt_auth.TokenIssuanceError) as error:
        crpt_auth.issue_token(INN, purpose='true_api', config=settings)
    assert error.value.diagnostic == 'stage=signing_' + mode + ' reason=invalid_signature'
    assert session.request.call_count == 1
    assert 'SECRET' not in str(error.value)


def test_fence_contains_no_credentials_and_release_requires_matching_published_attempt(tmp_path):
    store = ProcessLockStore(tmp_path / 'locks.sqlite')
    with master_runtime():
        with lock(store, tmp_path).hold():
            guard = crpt_auth.current_runtime().guard
            guard.begin_suz('production', INN, CON, OMS, CHALLENGE)
            path, _, state, _ = guard._suz_state('production', INN, CON, OMS)
            body, _ = store.read_lock_object(path)
            assert VALUE not in body and 'secret' not in body.lower()
            assert state['state'] == 'attempted'
            from dataclasses import replace
            with pytest.raises(crpt_auth.TokenIssuanceError):
                guard.complete_suz(replace(record(), auth_uuid=VALUE))
            guard.complete_suz(record())
            guard.check_suz('production', INN, CON, OMS)
            # Other INNs/connections and the sandbox are independent scopes.
            guard.check_suz('sandbox', INN, CON, OMS)
            guard.begin_suz('production', INN, CON, OMS, VALUE)
            assert guard._suz_state('production', INN, CON, OMS)[2]['state'] == 'attempted'


def test_accepted_fence_write_with_lost_response_prevents_post_and_future_retry(session, monkeypatch, tmp_path):
    monkeypatch.setattr(crpt_auth, 'sign_token_data', lambda *a, **kw: 'c2lnbmF0dXJl')
    session.request.return_value = response({'uuid': CHALLENGE, 'data': 'literal'})
    store = ProcessLockStore(tmp_path / 'locks.sqlite')
    write = store.write_lock_object
    def lost(path, body, etag):
        result = write(path, body, etag)
        if '.suz-' in path:
            raise OSError('SECRET lost response')
        return result
    store.write_lock_object = lost
    with master_runtime():
        with lock(store, tmp_path).hold():
            with pytest.raises(crpt_auth.TokenIssuanceError):
                crpt_auth.issue_token(INN, purpose='suz', connection_id=CON, oms_id=OMS, config=config())
    with master_runtime():
        with lock(ProcessLockStore(tmp_path / 'locks.sqlite'), tmp_path).hold():
            with pytest.raises(crpt_auth.TokenIssuanceError, match='requires reconciliation'):
                crpt_auth.issue_token(INN, purpose='suz', connection_id=CON, oms_id=OMS, config=config())
    assert session.request.call_count == 1


def test_fence_survives_process_death_and_dead_writer_recovery(tmp_path):
    script = '''
import os, sys
from token_lock_store import ProcessLockStore
from xtrek.token_master_lock import TokenMasterLock
from xtrek.token_runtime import master_runtime
with master_runtime():
    guard = TokenMasterLock(ProcessLockStore(sys.argv[1]), sys.argv[2],
                            {'tokens_master_local_lock_dir': sys.argv[3]})
    with guard.hold():
        guard.begin_suz('production', '7733154124', sys.argv[4], sys.argv[5], sys.argv[6])
        os._exit(12)
'''
    root = Path(__file__).resolve().parents[1]
    env = dict(os.environ, PYTHONPATH=os.pathsep.join([str(root), str(root / 'tests')]),
               AWS_EC2_METADATA_DISABLED='true')
    run = subprocess.run([sys.executable, '-c', script, str(tmp_path / 'locks.sqlite'),
                           LOCK, str(tmp_path), CON, OMS, CHALLENGE], env=env, capture_output=True, timeout=10)
    assert run.returncode == 12
    with master_runtime():
        with lock(ProcessLockStore(tmp_path / 'locks.sqlite'), tmp_path).hold():
            with pytest.raises(crpt_auth.TokenIssuanceError, match='requires reconciliation'):
                crpt_auth.current_runtime().guard.check_suz('production', INN, CON, OMS)


def test_diagnostic_allowlists_are_checked_again_at_logging():
    error = crpt_auth.TokenIssuanceError('SECRET', stage='issuance', reason='transport_failed')
    error.stage = 'SECRET challenge'
    error.reason = 'SECRET PIN'
    assert crpt_auth.safe_diagnostic(error) == 'stage=unknown reason=operation_failed'
    assert 'SECRET' not in crpt_auth.safe_diagnostic(RuntimeError('SECRET token'))
    error.stage, error.reason = [], {}
    assert crpt_auth.safe_diagnostic(error) == 'stage=unknown reason=operation_failed'
