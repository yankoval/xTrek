"""Cross-component regressions with a frozen clock and an in-memory S3 boundary."""
import base64
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import importlib.util
from itertools import permutations
import json
import os
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from xtrek import tokens, token_worker, token_registry, token_access
from xtrek.token_registry import TokenRecord, TokenRegistry, TokenValidationError
from token_lock_store import MemoryLockStore

INN = '1234567890'
OTHER = '0987654321'
CON = 'aaaaaaaa-1111-4111-8111-111111111111'
OMS = 'bbbbbbbb-2222-4222-8222-222222222222'
OLD = 'cccccccc-3333-4333-8333-333333333333'
NEW = 'dddddddd-4444-4444-8444-444444444444'
BASE = 'https://markirovka.crpt.ru/api/v3/true-api'


class Clock(datetime):
    instant = datetime(2030, 1, 1, tzinfo=timezone.utc)

    @classmethod
    def now(cls, tz=None):
        return cls.instant.astimezone(tz) if tz else cls.instant.replace(tzinfo=None)


def make_record(purpose, seconds=3600, **overrides):
    result = TokenRecord(purpose, 'production', INN, 'UUID', OLD,
                         (Clock.instant + timedelta(seconds=seconds)).isoformat(), 'server', BASE,
                         connection_id=CON if purpose == 'suz' else None,
                         oms_id=OMS if purpose == 'suz' else None)
    return replace(result, **overrides)


def legacy_jwt():
    exp = int((Clock.instant + timedelta(hours=2)).timestamp())
    payload = base64.urlsafe_b64encode(json.dumps({'inn': INN, 'pid': 'test', 'exp': exp,
                                                 'padding': 'x' * 100}).encode()).decode().rstrip('=')
    return make_record('true_api', format='JWT', token='eyJhbGciOiJIUzI1NiJ9.' + payload + '.sig',
                       expireDate=datetime.fromtimestamp(exp, timezone.utc).isoformat(), expiry_source='jwt_exp')


class MemoryS3(MemoryLockStore):
    def __init__(self):
        super().__init__()
        self.data = TokenRegistry().to_dict()
        self.uploads = []
        self.download_hook = None
        self.fail_upload = False

    def download(self, remote, local):
        if self.download_hook:
            self.download_hook()
        Path(local).write_text(json.dumps(self.data))

    def upload(self, local, remote):
        if self.fail_upload:
            raise OSError('simulated upload failure')
        self.data = json.loads(Path(local).read_text())
        self.uploads.append(remote)

@pytest.fixture
def lab(monkeypatch, tmp_path):
    Clock.instant = datetime(2030, 1, 1, tzinfo=timezone.utc)
    for module in (tokens, token_registry, token_access):
        monkeypatch.setattr(module, 'datetime', Clock)
    config = {'tokens_path': 's3://test/tokens.json', 'tokens_registry_path': 's3://test/tokens-v2.json',
              'tokens_master_local_lock_dir': str(tmp_path),
              'tokens_allowed_inns': [INN], 'true_api_token_format': 'UUID'}
    storage = MemoryS3()
    organization = SimpleNamespace(inn=INN, connection_id=CON, oms_id=OMS, name='Synthetic')
    manager = MagicMock()
    manager.list.return_value = [organization]
    manager.find.side_effect = lambda **kw: next((o for o in manager.list() if all(
        getattr(o, k, None) == v for k, v in kw.items())), None)
    for module in (tokens, token_worker):
        monkeypatch.setattr(module, 'load_config', lambda: config)
        monkeypatch.setattr(module, 'OrganizationManager', lambda *args: manager)
    monkeypatch.setattr(tokens, 'get_storage', lambda *args: storage)
    monkeypatch.setattr(tokens, 'home_dir', tmp_path)
    monkeypatch.delenv('TRUE_API_HOST', raising=False)
    tokens.TokenProcessor.clear_command_snapshots()
    issue = MagicMock(side_effect=lambda inn, **kw: make_record(kw['purpose'], token=NEW))
    monkeypatch.setattr(token_worker, 'issue_token', issue)
    yield SimpleNamespace(config=config, storage=storage, manager=manager, issue=issue)
    tokens.TokenProcessor.clear_command_snapshots()


@pytest.mark.parametrize('order', list(permutations(range(3))))
@pytest.mark.parametrize('purpose', ['true_api', 'suz'])
def test_master_refreshes_only_due_identity_for_every_record_order(lab, order, purpose):
    source = [make_record('true_api', 60 if purpose == 'true_api' else 3600),
              make_record('suz', 60 if purpose == 'suz' else 3600), legacy_jwt()]
    lab.storage.data = TokenRegistry(source).to_dict()
    lab.storage.data['tokens'] = [lab.storage.data['tokens'][i] for i in order]
    worker = token_worker.TokenRefreshWorker()
    assert worker.check_and_refresh()
    assert lab.issue.call_count == 1
    assert lab.issue.call_args.kwargs['purpose'] == purpose
    expected = TokenRegistry(source)
    expected.upsert(make_record(purpose, token=NEW))
    assert lab.storage.data == expected.to_dict()
    assert lab.storage.uploads == ['s3://test/tokens-v2.json']
    assert not lab.storage.locked
    # A second scheduled cycle must not issue or publish again.
    assert worker.check_and_refresh()
    assert lab.issue.call_count == 1
    assert len(lab.storage.uploads) == 1


@pytest.mark.parametrize('seconds,issued', [(1801, False), (1800, True), (1, True), (0, True), (-1, True)])
def test_exact_refresh_threshold_and_expiry(lab, seconds, issued):
    lab.storage.data = TokenRegistry([make_record('true_api', seconds), make_record('suz')]).to_dict()
    assert token_worker.TokenRefreshWorker().check_and_refresh()
    assert bool(lab.issue.call_count) is issued


def test_other_inn_and_sandbox_records_survive_publication(lab):
    untouched = [make_record('true_api', 10, inn=OTHER),
                 make_record('true_api', 10, environment='sandbox',
                             issuer_base_url='https://markirovka.sandbox.crptech.ru/api/v3/true-api')]
    lab.manager.list.return_value.append(SimpleNamespace(inn=OTHER, connection_id=None, name='Other'))
    lab.storage.data = TokenRegistry([make_record('true_api', 10), make_record('suz')] + untouched).to_dict()
    assert token_worker.TokenRefreshWorker().check_and_refresh()
    assert lab.issue.call_count == 1
    actual = TokenRegistry.from_dict(lab.storage.data)
    assert all(item in actual.records for item in untouched)


def test_expiry_during_source_reload_prevents_publication(lab):
    lab.storage.data = TokenRegistry([make_record('true_api'), make_record('suz')]).to_dict()
    processor = tokens.TokenProcessor(org_manager=lab.manager, tokens_read_only=False)
    candidate = make_record('true_api', 1, token=NEW)
    original = json.dumps(lab.storage.data, sort_keys=True)
    lab.storage.download_hook = lambda: setattr(Clock, 'instant', Clock.instant + timedelta(seconds=2))
    with pytest.raises(TokenValidationError, match='expired'):
        processor.save_record(candidate)
    assert not lab.storage.uploads
    assert json.dumps(lab.storage.data, sort_keys=True) == original


@pytest.mark.parametrize('purpose', ['true_api', 'suz'])
def test_failed_publish_recovers_from_authoritative_source(lab, purpose):
    source = [make_record('true_api', 30 if purpose == 'true_api' else 3600),
              make_record('suz', 30 if purpose == 'suz' else 3600)]
    lab.storage.data = TokenRegistry(source).to_dict()
    original = json.dumps(lab.storage.data, sort_keys=True)
    lab.storage.fail_upload = True
    assert not token_worker.TokenRefreshWorker().check_and_refresh()
    assert json.dumps(lab.storage.data, sort_keys=True) == original
    assert not lab.storage.locked
    # The failed candidate exists locally, but a new client must use S3.
    client = tokens.TokenProcessor(org_manager=lab.manager)
    scope = {'conid': CON, 'oms_id': OMS} if purpose == 'suz' else {}
    assert client.get_token_value_for(INN, purpose, **scope) == OLD
    lab.storage.fail_upload = False
    assert token_worker.TokenRefreshWorker().check_and_refresh()
    assert client.refresh_token_for(INN, purpose, **scope) == NEW
    assert len(lab.storage.uploads) == 1


def test_corrupt_refresh_cannot_poison_next_command(lab):
    good = TokenRegistry([make_record('true_api'), make_record('suz')]).to_dict()
    lab.storage.data = good
    client = tokens.TokenProcessor(org_manager=lab.manager)
    lab.storage.data = {'schema_version': 2, 'tokens': [{'token': 'not-a-record'}]}
    with pytest.raises(RuntimeError):
        client.refresh_token_for(INN, 'true_api')
    tokens.TokenProcessor.clear_command_snapshots()
    with pytest.raises(RuntimeError):
        tokens.TokenProcessor(org_manager=lab.manager)
    lab.storage.data = TokenRegistry([make_record('true_api', token=NEW), make_record('suz')]).to_dict()
    recovered = tokens.TokenProcessor(org_manager=lab.manager)
    assert recovered.get_token_value_for(INN, 'true_api') == NEW
    assert not lab.storage.uploads


@pytest.mark.parametrize('first_task_fails', [False, True])
def test_long_lived_celery_sees_v2_refresh_after_task_boundary(lab, monkeypatch, first_task_fails):
    from celery import Celery
    from celery.signals import task_postrun, task_prerun
    monkeypatch.setenv('YMQ_ACCESS_KEY', 'synthetic')
    monkeypatch.setenv('YMQ_SECRET_KEY', 'synthetic')
    monkeypatch.setenv('YMQ_QUEUE_URL', 'https://example.test/queue')
    monkeypatch.setattr('xtrek.config_loader.load_config', lambda *args: {})
    spec = importlib.util.spec_from_file_location('xtrek._v2_regression_tasks', Path(tokens.__file__).with_name('tasks.py'))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    app = Celery('v2-local-regression', broker='memory://')
    app.conf.update(task_always_eager=True, task_eager_propagates=True)
    seen = []

    @app.task
    def read_tokens(fail=False):
        from xtrek.trueapi import HonestSignAPI
        from xtrek.suz import SUZ
        client = tokens.TokenProcessor(org_manager=lab.manager)
        true_api = HonestSignAPI(token=client.get_token_value_for(INN, 'true_api'))
        suz = SUZ(token=client.get_token_value_for(INN, 'suz', CON, OMS), omsId=OMS, clientToken=CON)
        seen.append((os.getpid(), true_api.headers['Authorization'], suz.headers['clientToken']))
        if fail:
            raise RuntimeError('synthetic task failure')

    try:
        lab.storage.data = TokenRegistry([make_record('true_api', 60), make_record('suz'), legacy_jwt()]).to_dict()
        if first_task_fails:
            with pytest.raises(RuntimeError, match='synthetic task failure'):
                read_tokens.delay(True)
        else:
            read_tokens.delay().get()
        assert token_worker.TokenRefreshWorker().check_and_refresh()
        lab.storage.data['tokens'].reverse()
        read_tokens.delay().get()
        assert seen == [(os.getpid(), 'Bearer ' + OLD, OLD), (os.getpid(), 'Bearer ' + NEW, OLD)]
    finally:
        task_prerun.disconnect(module._start_token_snapshot)
        task_postrun.disconnect(module._finish_token_snapshot)
        app.close()
        module.app.close()


def test_expiry_during_local_write_prevents_upload_and_restores_snapshot(lab, monkeypatch):
    lab.storage.data = TokenRegistry([make_record('true_api'), make_record('suz')]).to_dict()
    processor = tokens.TokenProcessor(org_manager=lab.manager, tokens_read_only=False)
    candidate = make_record('true_api', 1, token=NEW)
    write = processor._write_tokens_file_atomic

    def delayed_write():
        write()
        Clock.instant += timedelta(seconds=2)

    monkeypatch.setattr(processor, '_write_tokens_file_atomic', delayed_write)
    with pytest.raises(TokenValidationError, match='expired'):
        processor.save_record(candidate)
    assert not lab.storage.uploads
    assert processor.get_token_value_for(INN, 'true_api') == OLD


@pytest.mark.parametrize('probe_status', [200, 401])
def test_master_generator_probe_publish_chain(lab, monkeypatch, probe_status):
    from xtrek import crpt_auth
    lab.config['sign'] = 's3://test/sign'
    lab.storage.data = TokenRegistry([make_record('true_api', 10), make_record('suz')]).to_dict()
    original = json.dumps(lab.storage.data, sort_keys=True)
    session = MagicMock()
    session.__enter__.return_value = session

    def reply(body, status=200):
        response = MagicMock(status_code=status)
        response.json.return_value = body
        return response

    session.request.side_effect = [reply({'uuid': CON, 'data': 'synthetic challenge'}),
                                   reply({'uuidToken': NEW, 'expireDate': make_record('true_api').expireDate}),
                                   reply([], probe_status)]
    monkeypatch.setattr(crpt_auth.requests, 'Session', lambda: session)
    sign = MagicMock(return_value='c2ln')
    monkeypatch.setattr(crpt_auth, 'sign_data', sign)
    monkeypatch.setattr(crpt_auth, 'datetime', Clock)
    monkeypatch.setattr(token_worker, 'issue_token', crpt_auth.issue_token)
    assert token_worker.TokenRefreshWorker().check_and_refresh() is (probe_status == 200)
    assert session.request.call_count == 3
    sign.assert_called_once_with('synthetic challenge', INN, lab.config)
    assert session.request.call_args_list[1].kwargs['json']['unitedToken'] is True
    assert session.request.call_args_list[2].kwargs['headers']['Authorization'] == 'Bearer ' + NEW
    if probe_status == 200:
        client = tokens.TokenProcessor(org_manager=lab.manager)
        assert client.get_token_value_for(INN, 'true_api') == NEW
        assert client.get_token_value_for(INN, 'suz', CON, OMS) == OLD
    else:
        assert not lab.storage.uploads
        assert json.dumps(lab.storage.data, sort_keys=True) == original
    assert not lab.storage.locked


def test_true_api_only_pilot_never_reissues_suz_even_with_large_threshold(lab):
    lab.config.update(tokens_purposes=['true_api'], tokens_refresh_before_expiry_seconds=40000)
    original_suz = make_record('suz', 1)
    lab.storage.data = TokenRegistry([make_record('true_api'), original_suz]).to_dict()
    assert token_worker.TokenRefreshWorker().check_and_refresh()
    assert lab.issue.call_count == 1
    assert lab.issue.call_args.kwargs['purpose'] == 'true_api'
    assert original_suz in TokenRegistry.from_dict(lab.storage.data).records


@pytest.mark.parametrize('purposes', [[], ['unknown'], 'true_api'])
def test_invalid_pilot_purposes_prevent_issuance(lab, purposes):
    lab.config['tokens_purposes'] = purposes
    with pytest.raises(ValueError, match='tokens_purposes'):
        token_worker.TokenRefreshWorker().check_and_refresh()
    lab.issue.assert_not_called()
