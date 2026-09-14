from types import SimpleNamespace
from unittest.mock import MagicMock
import pytest
from xtrek import token_worker


@pytest.fixture
def worker(monkeypatch):
    manager = MagicMock()
    manager.list.return_value = [SimpleNamespace(inn='123', connection_id='connection', name='Company')]
    processor = MagicMock()
    processor.get_token_value_by_inn.return_value = 'current'
    processor.get_token_remaining_seconds.return_value = 3600
    monkeypatch.setattr(token_worker, 'load_config', lambda: {})
    monkeypatch.setattr(token_worker, 'OrganizationManager', lambda path: manager)
    factory = MagicMock(return_value=processor)
    monkeypatch.setattr(token_worker, 'TokenProcessor', factory)
    instance = token_worker.TokenRefreshWorker()
    factory.assert_called_once_with(org_manager=manager, tokens_read_only=False)
    return instance


def test_fresh_tokens_are_not_reissued_or_written(worker, monkeypatch):
    refresh = MagicMock()
    monkeypatch.setattr(token_worker, 'refresh_token', refresh)
    assert worker.check_and_refresh()
    worker.tp._sync_from_s3.assert_called_once_with(required=True)
    refresh.assert_not_called()
    worker.tp.save_token.assert_not_called()


@pytest.mark.parametrize('remaining', [1800, 0, None])
def test_only_due_token_is_refreshed(worker, monkeypatch, remaining):
    worker.tp.get_token_remaining_seconds.side_effect = [remaining, 3600]
    worker.tp.get_token_value_by_inn.side_effect = ['old', 'new', 'current-auth']
    refresh = MagicMock(return_value='new')
    monkeypatch.setattr(token_worker, 'refresh_token', refresh)
    assert worker.check_and_refresh()
    refresh.assert_called_once_with('123', conid=None, mode='jwt')
    worker.tp.save_token.assert_called_once_with('new', conid=None)


def test_missing_auth_is_created_with_connection(worker, monkeypatch):
    worker.tp.get_token_remaining_seconds.side_effect = [3600, None]
    worker.tp.get_token_value_by_inn.side_effect = ['current-jwt', None, 'new-auth']
    refresh = MagicMock(return_value='new-auth')
    monkeypatch.setattr(token_worker, 'refresh_token', refresh)
    assert worker.check_and_refresh()
    refresh.assert_called_once_with('123', conid='connection', mode='auth')
    worker.tp.save_token.assert_called_once_with('new-auth', conid='connection')


def test_refresh_failure_preserves_tokens_and_returns_failure(worker, monkeypatch):
    worker.tp.get_token_remaining_seconds.return_value = 0
    monkeypatch.setattr(token_worker, 'refresh_token', lambda *a, **kw: None)
    assert not worker.check_and_refresh()
    worker.tp.save_token.assert_not_called()


def test_s3_unavailable_prevents_refresh(worker, monkeypatch):
    worker.tp._sync_from_s3.side_effect = RuntimeError('S3 unavailable')
    refresh = MagicMock()
    monkeypatch.setattr(token_worker, 'refresh_token', refresh)
    monkeypatch.setattr(token_worker, 'TokenRefreshWorker', lambda: worker)
    assert token_worker.main(['--once']) == 1
    refresh.assert_not_called()
    worker.tp.save_token.assert_not_called()


@pytest.mark.parametrize('argv', [[], ['--once']])
@pytest.mark.parametrize('success', [True, False])
def test_cli_runs_exactly_once_and_returns_status(monkeypatch, argv, success):
    worker = MagicMock()
    worker.check_and_refresh.return_value = success
    monkeypatch.setattr(token_worker, 'TokenRefreshWorker', lambda: worker)
    assert token_worker.main(argv) == (0 if success else 1)
    worker.check_and_refresh.assert_called_once_with()


def test_publish_failure_marks_cycle_failed(worker, monkeypatch):
    worker.tp.get_token_remaining_seconds.side_effect = [0, 3600]
    worker.tp.save_token.side_effect = RuntimeError('S3 upload failed')
    monkeypatch.setattr(token_worker, 'refresh_token', lambda *a, **kw: 'new')
    assert not worker.check_and_refresh()


def test_allowed_inns_exclude_test_organization(worker, monkeypatch):
    worker.config['tokens_allowed_inns'] = ['123']
    worker.org_manager.list.return_value.append(
        SimpleNamespace(inn='1234567890', connection_id='test', name='Test Org')
    )
    refresh = MagicMock()
    monkeypatch.setattr(token_worker, 'refresh_token', refresh)
    assert worker.check_and_refresh()
    assert worker.tp.get_token_remaining_seconds.call_count == 2
    refresh.assert_not_called()


def test_missing_allowed_organization_fails_without_issuing(worker, monkeypatch):
    worker.config['tokens_allowed_inns'] = ['missing']
    refresh = MagicMock()
    monkeypatch.setattr(token_worker, 'refresh_token', refresh)
    assert not worker.check_and_refresh()
    refresh.assert_not_called()
