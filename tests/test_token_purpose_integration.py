import base64
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
import requests
from xtrek import crpt_auth, tokens
from xtrek.token_registry import TokenRecord, TokenRegistry, TokenValidationError

INN = '1234567890'
CON = '11111111-1111-4111-8111-111111111111'
OMS = '22222222-2222-4222-8222-222222222222'
VALUE = '33333333-3333-4333-8333-333333333333'
CHALLENGE = '44444444-4444-4444-8444-444444444444'
BASE = crpt_auth.AUTH_BASES['production']


def record(purpose='true_api', **changes):
    return replace(TokenRecord(purpose, 'production', INN, 'UUID', VALUE,
                               (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat(),
                               'server', BASE, connection_id=CON if purpose == 'suz' else None,
                               oms_id=OMS if purpose == 'suz' else None), **changes)


def jwt_record():
    exp = int((datetime.now(timezone.utc) + timedelta(hours=1)).timestamp())
    payload = base64.urlsafe_b64encode(json.dumps({'inn': INN, 'pid': 'test', 'exp': exp,
                                                'pad': 'x' * 100}).encode()).decode().rstrip('=')
    return record(format='JWT', token='eyJhbGciOiJIUzI1NiJ9.' + payload + '.sig',
                  expireDate=datetime.fromtimestamp(exp, timezone.utc).isoformat(), expiry_source='jwt_exp')


@pytest.fixture
def processor(monkeypatch, tmp_path):
    config = {'tokens_registry_path': 's3://test/tokens-v2.json', 'tokens_path': 's3://test/tokens.json',
              'true_api_token_format': 'UUID'}
    storage = MagicMock()
    data = TokenRegistry([record(), record('suz'), jwt_record()]).to_dict()
    storage.download.side_effect = lambda remote, local: Path(local).write_text(json.dumps(data))
    storage.upload.side_effect = lambda local, remote: data.update(json.loads(Path(local).read_text()))
    manager = MagicMock()
    manager.find.return_value = SimpleNamespace(inn=INN, connection_id=CON, oms_id=OMS)
    monkeypatch.setattr(tokens, 'load_config', lambda: config)
    monkeypatch.setattr(tokens, 'get_storage', lambda *args: storage)
    tokens.TokenProcessor.clear_command_snapshots()
    tp = tokens.TokenProcessor(str(tmp_path / 'tokens-v2.json'), org_manager=manager)
    return tp, storage, data


def test_same_uuid_has_separate_purposes_and_expiry(processor):
    tp, storage, _ = processor
    assert tp.get_token_value_for(INN, 'true_api') == VALUE
    assert tp.get_token_value_for(INN, 'suz', CON, OMS) == VALUE
    assert tp.remaining_for(INN, 'true_api') > 0
    assert storage.download.call_count == 1
    with pytest.raises(PermissionError):
        tp.save_record(record())
    storage.upload.assert_not_called()


def test_no_format_or_environment_fallback(processor):
    tp, _, _ = processor
    tp.registry = TokenRegistry([record('suz'), jwt_record()])
    assert tp.get_token_value_for(INN, 'true_api') is None
    with pytest.raises(TokenValidationError):
        tp.get_token_value_by_inn(INN, 'UUID')
    with pytest.raises(TokenValidationError):
        tp.get_token_value_for(INN, 'true_api', environment='sandbox')
    with pytest.raises(TokenValidationError):
        tp.get_token_value_for(INN, 'suz', CON, CHALLENGE)
    with pytest.raises(TokenValidationError):
        tp.get_token_value_for('0987654321', 'suz', CON, OMS)


def test_master_updates_one_identity_and_preserves_others(processor):
    tp, storage, data = processor
    tp.tokens_read_only = False
    new = record(token=CHALLENGE)
    tp.save_record(new)
    published = TokenRegistry.from_dict(data)
    assert len(published.records) == 3
    assert published.select(purpose='suz', environment='production', inn=INN,
                            connection_id=CON, oms_id=OMS).token == VALUE
    assert tp.get_token_value_for(INN, 'true_api') == CHALLENGE
    assert Path(tp.file_path).stat().st_mode & 0o777 == 0o600
    assert storage.upload.call_args.args[1] == 's3://test/tokens-v2.json'


def test_missing_source_prevents_publish(processor):
    tp, storage, _ = processor
    tp.tokens_read_only = False
    storage.download.side_effect = OSError('offline')
    with pytest.raises(RuntimeError):
        tp.save_record(record())
    storage.upload.assert_not_called()


def test_refresh_callback_rereads_scoped_token(processor):
    tp, storage, data = processor
    data.update(TokenRegistry([record(token=CHALLENGE), record('suz')]).to_dict())
    assert tp.get_token_value_for(INN, 'true_api') == VALUE
    assert tp.refresh_token_for(INN, 'true_api') == CHALLENGE
    assert tp.get_token_value_for(INN, 'suz', CON, OMS) == VALUE
    assert storage.download.call_count == 2
    storage.upload.assert_not_called()


def test_legacy_explicit_policy_and_exact_expiry(monkeypatch, tmp_path):
    monkeypatch.setattr(tokens, 'load_config', lambda: {})
    manager = MagicMock()
    manager.find.return_value = SimpleNamespace(inn=INN, oms_id=OMS)
    tp = tokens.TokenProcessor(str(tmp_path / 'tokens.json'), org_manager=manager, tokens_read_only=False)
    tp.save_record(jwt_record())
    suz = record('suz')
    tp.save_record(suz)
    assert tp.get_token_value_for(INN, 'true_api').startswith('eyJ')
    assert tp.get_token_value_for(INN, 'suz', CON, OMS) == VALUE
    assert tp.tokens[-1]['ДействуетДо'] == suz.expireDate
    with pytest.raises(TokenValidationError):
        tp.save_record(record())
    tp.config['true_api_token_format'] = 'UUID'
    with pytest.raises(TokenValidationError):
        tp.get_token_value_for(INN, 'true_api')


def response(data, status=200):
    result = MagicMock(status_code=status)
    result.json.return_value = data
    return result


@pytest.fixture
def issuer(monkeypatch):
    session = MagicMock()
    session.__enter__.return_value = session
    monkeypatch.setattr(crpt_auth.requests, 'Session', lambda: session)
    signing = MagicMock(return_value='c2lnbmF0dXJl')
    monkeypatch.setattr(crpt_auth, 'sign_data', signing)
    monkeypatch.setattr(crpt_auth.time, 'sleep', lambda _: None)
    config = {'sign': 's3://test/sign', 'suz_ping_sign': True}
    return session, signing, config


def test_true_api_uses_uuid_field_server_expiry_and_own_probe(issuer):
    session, signing, config = issuer
    expiry = record().expireDate
    session.request.side_effect = [response({'uuid': CHALLENGE, 'data': 'exact\nbytes'}),
                                    response({'token': 'wrong', 'uuidToken': VALUE, 'expireDate': expiry}),
                                    response([])]
    result = crpt_auth.issue_token(INN, purpose='true_api', config=config)
    assert result.token == VALUE and result.expireDate == expiry
    signing.assert_called_once_with('exact\nbytes', INN, config)
    calls = session.request.call_args_list
    assert calls[1].kwargs['json'] == {'uuid': CHALLENGE, 'data': 'c2lnbmF0dXJl', 'inn': INN, 'unitedToken': True}
    assert calls[2].args[1] == BASE + '/elk/product-groups/balance/all'
    assert calls[2].kwargs['headers']['Authorization'] == 'Bearer ' + VALUE
    assert all(call.kwargs['allow_redirects'] is False and call.kwargs['timeout'] == (10, 30) for call in calls)
    assert session.verify is True and session.trust_env is False


@pytest.mark.parametrize('result', [{'token': VALUE}, {'uuidToken': VALUE},
                                    {'uuidToken': VALUE, 'expireDate': '2026-01-01T00:00:00'}])
def test_uuid_requires_server_expiry_and_uuid_field(issuer, result):
    session, _, config = issuer
    session.request.side_effect = [response({'uuid': CHALLENGE, 'data': 'x'}), response(result)]
    with pytest.raises(TokenValidationError):
        crpt_auth.issue_token(INN, purpose='true_api', config=config)
    assert session.request.call_count == 2


def test_suz_signed_ping_exact_path_and_ttl(issuer):
    session, signing, config = issuer
    session.request.side_effect = [response({'uuid': CHALLENGE, 'data': 'x'}), response({'token': VALUE}),
                                    response({'omsId': OMS, 'apiVersion': '3', 'omsVersion': '5'})]
    start = datetime.now(timezone.utc)
    result = crpt_auth.issue_token(INN, purpose='suz', connection_id=CON, oms_id=OMS, config=config)
    assert result.expiry_source == 'suz_ttl'
    assert 35990 < result.remaining_seconds(start) <= 36010
    path = '/api/v3/ping?omsId=' + OMS
    signing.assert_any_call(path, INN, config, detached=True)
    calls = session.request.call_args_list
    assert calls[1].args[1].endswith('/auth/simpleSignIn/' + CON)
    assert 'unitedToken' not in calls[1].kwargs['json']
    assert calls[2].args[1] == crpt_auth.SUZ_BASES['production'] + path
    assert calls[2].kwargs['headers'] == {'Accept': 'application/json', 'clientToken': VALUE,
                                         'X-Signature': 'c2lnbmF0dXJl'}


@pytest.mark.parametrize('probe', [response({}, 401), response({'omsId': CHALLENGE, 'apiVersion': '3', 'omsVersion': '5'})])
def test_suz_probe_failure_explains_revocation_without_reissue(issuer, probe):
    session, _, config = issuer
    session.request.side_effect = [response({'uuid': CHALLENGE, 'data': 'x'}), response({'token': VALUE}), probe]
    with pytest.raises(crpt_auth.TokenIssuanceError, match='previous connection token may be invalid'):
        crpt_auth.issue_token(INN, purpose='suz', connection_id=CON, oms_id=OMS, config=config)
    assert sum(c.args[0] == 'POST' for c in session.request.call_args_list) == 1


def test_post_transport_failure_is_not_retried_and_does_not_leak(issuer):
    session, _, config = issuer
    session.request.side_effect = [response({'uuid': CHALLENGE, 'data': 'x'}), requests.Timeout('secret credential')]
    with pytest.raises(crpt_auth.TokenIssuanceError) as error:
        crpt_auth.issue_token(INN, purpose='true_api', config=config)
    assert 'secret' not in str(error.value)
    assert session.request.call_count == 2


def test_only_health_probe_retries_transient_error(issuer):
    session, _, config = issuer
    session.request.side_effect = [response({'uuid': CHALLENGE, 'data': 'x'}),
                                    response({'uuidToken': VALUE, 'expireDate': record().expireDate}),
                                    response({}, 503), response({}, 429), response([])]
    assert crpt_auth.issue_token(INN, purpose='true_api', config=config).token == VALUE
    assert session.request.call_count == 5


def test_signjs_filename_payload_and_cleanup(monkeypatch):
    storage = MagicMock()
    storage.read_text.return_value = 'c2ln\n bmF0dXJl '
    monkeypatch.setattr(crpt_auth, 'get_storage', lambda *args: storage)
    assert crpt_auth.sign_data('raw?x=1', INN, {'sign': '/test'}, detached=True) == 'c2lnbmF0dXJl'
    source, data = storage.write_text.call_args.args
    assert source.startswith('/test/' + INN + '_') and source.endswith('.json')
    assert data == 'raw?x=1'
    assert {c.args[0] for c in storage.delete.call_args_list} == {source, source + '.sig'}


def test_signing_timeout_cleans_up_without_auth_post(monkeypatch):
    storage = MagicMock()
    storage.exists.side_effect = [False, False, True]
    monkeypatch.setattr(crpt_auth, 'get_storage', lambda *args: storage)
    monkeypatch.setattr(crpt_auth.time, 'monotonic', MagicMock(side_effect=[0, 61]))
    with pytest.raises(crpt_auth.TokenIssuanceError, match='timed out'):
        crpt_auth.sign_data('x', INN, {'sign': '/test'})
    storage.delete.assert_called_once()


def test_failed_publish_does_not_change_active_snapshot(processor):
    tp, storage, _ = processor
    tp.tokens_read_only = False
    storage.upload.side_effect = OSError('upload failed')
    with pytest.raises(RuntimeError):
        tp.save_record(record(token=CHALLENGE))
    assert tp.get_token_value_for(INN, 'true_api') == VALUE
    assert tp.get_token_value_for(INN, 'suz', CON, OMS) == VALUE


def test_lock_contention_and_release_on_failure(processor):
    tp, storage, _ = processor
    tp.tokens_read_only = False
    storage.acquire_lock.return_value = False
    with pytest.raises(TokenValidationError, match='already locked'):
        with tp.writer_lock():
            pytest.fail('Busy master must not run')
    storage.release_lock.assert_not_called()
    storage.acquire_lock.return_value = True
    with pytest.raises(RuntimeError):
        with tp.writer_lock():
            raise RuntimeError('cycle failed')
    storage.release_lock.assert_called_once_with('s3://test/tokens.json.master.lock')


def test_wrong_true_api_host_rejected_before_use(processor):
    tp, _, _ = processor
    tp.config['true_api_host'] = 'https://markirovka.sandbox.crptech.ru'
    with pytest.raises(TokenValidationError, match='host differs'):
        tp.get_token_value_for(INN, 'true_api')


@pytest.mark.parametrize('module_name', ['trueapi', 'nk'])
def test_cli_missing_selected_token_does_not_fall_back_to_env(monkeypatch, module_name):
    import importlib
    module = importlib.import_module('xtrek.' + module_name)
    processor = MagicMock()
    processor.get_token_for.return_value = None
    monkeypatch.setattr(module, 'TokenProcessor', MagicMock(return_value=processor))
    monkeypatch.setattr(module, 'setup_logging', MagicMock())
    api = MagicMock()
    monkeypatch.setattr(module, 'HonestSignAPI' if module_name == 'trueapi' else 'NK', api)
    monkeypatch.setenv('FIND_TOKEN_BY_INN', INN)
    monkeypatch.setenv('HONEST_SIGN_TOKEN', 'unrelated')
    monkeypatch.setenv('TRUE_API_TOKEN', 'unrelated')
    monkeypatch.setattr('sys.argv', [module_name])
    try:
        module.main()
    except SystemExit as exc:
        assert exc.code == 1
    api.assert_not_called()
    processor.get_token_for.assert_called_once()


def test_product_owner_lookup_ignores_ambiguous_client_token(monkeypatch):
    from xtrek import create_emission_task_sample as workflow
    monkeypatch.setattr(workflow, 'load_config', lambda *args: {'client_token': VALUE})
    manager = MagicMock()
    manager.list.return_value = [SimpleNamespace(inn=INN)]
    processor = MagicMock()
    processor.get_token_value_for.return_value = 'scoped-true-api'
    monkeypatch.setattr(workflow, 'OrganizationManager', MagicMock(return_value=manager))
    monkeypatch.setattr(workflow, 'TokenProcessor', MagicMock(return_value=processor))
    assert workflow._get_participant_token() == 'scoped-true-api'
    processor.get_token_value_for.assert_called_once_with(INN, purpose='true_api')


def test_registry_mode_ignores_stale_environment_token(monkeypatch):
    from xtrek import utils
    monkeypatch.setenv('TRUE_API_TOKEN', 'stale-jwt')
    monkeypatch.setattr(utils, 'load_config', lambda: {'tokens_registry_path': 's3://test/v2.json'})
    assert utils._environment_true_api_token() is None
    monkeypatch.setattr(utils, 'load_config', lambda: {})
    assert utils._environment_true_api_token() == 'stale-jwt'


def test_jwt_issuance_preserves_exp_and_checks_true_api(issuer):
    session, _, config = issuer
    jwt = jwt_record()
    session.request.side_effect = [response({'uuid': CHALLENGE, 'data': 'x'}),
                                    response({'token': jwt.token}), response([])]
    issued = crpt_auth.issue_token(INN, purpose='true_api', token_format='JWT', config=config)
    assert issued.token == jwt.token and issued.expireDate == jwt.expireDate
    assert session.request.call_args_list[1].kwargs['json']['unitedToken'] is False


def test_generator_routes_sandbox_without_using_production(issuer):
    session, _, config = issuer
    config['tokens_environment'] = 'sandbox'
    session.request.side_effect = [response({'uuid': CHALLENGE, 'data': 'x'}),
                                    response({'uuidToken': VALUE, 'expireDate': record().expireDate}), response([])]
    issued = crpt_auth.issue_token(INN, purpose='true_api', config=config)
    assert issued.environment == 'sandbox'
    assert all(call.args[1].startswith(crpt_auth.AUTH_BASES['sandbox']) for call in session.request.call_args_list)
