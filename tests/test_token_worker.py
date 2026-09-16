from types import SimpleNamespace
from unittest.mock import MagicMock
import pytest
from xtrek import token_worker


@pytest.fixture
def worker(monkeypatch):
    manager = MagicMock()
    manager.list.return_value = [SimpleNamespace(inn='1234567890', connection_id='connection', oms_id='oms', name='Company')]
    processor = MagicMock()
    processor.remaining_for.return_value = 3600
    processor.true_api_format.return_value = 'UUID'
    processor.get_token_value_for.return_value = 'new'
    monkeypatch.setattr(token_worker, 'load_config', lambda: {})
    monkeypatch.setattr(token_worker, 'OrganizationManager', lambda path: manager)
    factory = MagicMock(return_value=processor)
    monkeypatch.setattr(token_worker, 'TokenProcessor', factory)
    instance = token_worker.TokenRefreshWorker()
    factory.assert_called_once_with(org_manager=manager, tokens_read_only=False)
    return instance


def test_fresh_tokens_are_not_reissued_or_written(worker, monkeypatch):
    issue = MagicMock()
    monkeypatch.setattr(token_worker, 'issue_token', issue)
    assert worker.check_and_refresh()
    worker.tp._sync_from_s3.assert_called_once_with(required=True)
    issue.assert_not_called()
    worker.tp.save_record.assert_not_called()


@pytest.mark.parametrize('remaining', [1800, 0, None])
def test_only_due_true_api_token_is_refreshed(worker, monkeypatch, remaining):
    worker.tp.remaining_for.side_effect = [remaining, 3600]
    record = SimpleNamespace(token='new')
    issue = MagicMock(return_value=record)
    monkeypatch.setattr(token_worker, 'issue_token', issue)
    assert worker.check_and_refresh()
    issue.assert_called_once_with('1234567890', purpose='true_api', token_format='UUID',
                                  connection_id=None, oms_id=None, config=worker.config)
    worker.tp.save_record.assert_called_once_with(record)


def test_missing_suz_is_created_with_connection_and_oms(worker, monkeypatch):
    worker.tp.remaining_for.side_effect = [3600, None]
    record = SimpleNamespace(token='new')
    issue = MagicMock(return_value=record)
    monkeypatch.setattr(token_worker, 'issue_token', issue)
    assert worker.check_and_refresh()
    issue.assert_called_once_with('1234567890', purpose='suz', token_format='UUID',
                                  connection_id='connection', oms_id='oms', config=worker.config)
    worker.tp.save_record.assert_called_once_with(record)


def test_probe_failure_prevents_publish_and_logs_no_secret(worker, monkeypatch, caplog):
    worker.tp.remaining_for.return_value = 0
    monkeypatch.setattr(token_worker, 'issue_token', MagicMock(side_effect=RuntimeError('secret credential')))
    assert not worker.check_and_refresh()
    worker.tp.save_record.assert_not_called()
    assert 'secret credential' not in caplog.text


def test_s3_unavailable_prevents_refresh(worker, monkeypatch):
    worker.tp._sync_from_s3.side_effect = RuntimeError('S3 unavailable')
    issue = MagicMock()
    monkeypatch.setattr(token_worker, 'issue_token', issue)
    monkeypatch.setattr(token_worker, 'TokenRefreshWorker', lambda: worker)
    assert token_worker.main(['--once']) == 1
    issue.assert_not_called()


@pytest.mark.parametrize('argv', [[], ['--once']])
@pytest.mark.parametrize('success', [True, False])
def test_cli_runs_exactly_once_and_returns_status(monkeypatch, argv, success):
    worker = MagicMock()
    worker.check_and_refresh.return_value = success
    monkeypatch.setattr(token_worker, 'TokenRefreshWorker', lambda: worker)
    assert token_worker.main(argv) == (0 if success else 1)
    worker.check_and_refresh.assert_called_once_with()


def test_publish_failure_marks_cycle_failed(worker, monkeypatch):
    worker.tp.remaining_for.side_effect = [0, 3600]
    worker.tp.save_record.side_effect = RuntimeError('S3 upload failed')
    monkeypatch.setattr(token_worker, 'issue_token', MagicMock(return_value=SimpleNamespace(token='new')))
    assert not worker.check_and_refresh()


def test_allowed_inns_exclude_other_organizations(worker, monkeypatch):
    worker.config['tokens_allowed_inns'] = ['1234567890']
    worker.org_manager.list.return_value.append(SimpleNamespace(inn='0987654321', connection_id='other', name='Other'))
    issue = MagicMock()
    monkeypatch.setattr(token_worker, 'issue_token', issue)
    assert worker.check_and_refresh()
    assert worker.tp.remaining_for.call_count == 2
    issue.assert_not_called()


def test_missing_allowed_organization_fails_without_issuing(worker, monkeypatch):
    worker.config['tokens_allowed_inns'] = ['missing']
    issue = MagicMock()
    monkeypatch.setattr(token_worker, 'issue_token', issue)
    assert not worker.check_and_refresh()
    issue.assert_not_called()
