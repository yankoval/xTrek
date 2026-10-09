import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from xtrek import org_manager
from xtrek.storage import S3Storage


def make_manager(tmp_path, monkeypatch, *, read_only=True, download=None, files=None):
    source = tmp_path / 'release' / 'my_orgs'
    source.mkdir(parents=True)
    (source / 'example.json').write_text('{}')
    cache = tmp_path / 'cache'
    cache.mkdir(exist_ok=True)
    config = {'orgs_cache_dir': str(cache), 'orgs_read_only': read_only,
              'orgs_path': 's3://orgs/reference', 's3_config': {}}
    monkeypatch.setattr(org_manager, 'load_config', lambda: config)
    storage = MagicMock()
    storage.list_files.return_value = files if files is not None else ['s3://orgs/reference/live.json']
    card = {'org_id': 'live', 'inn': '123', 'name': 'Live', 'phone': '', 'person': ''}
    storage.download.side_effect = download or (lambda remote, local: Path(local).write_text(json.dumps(card)))
    monkeypatch.setattr(org_manager, 'get_storage', lambda *args: storage)
    return source, cache, config, storage


def test_external_cache_leaves_release_unchanged_and_never_publishes(tmp_path, monkeypatch):
    source, cache, _, storage = make_manager(tmp_path, monkeypatch)
    (cache / 'stale.json').write_text(json.dumps(
        {'org_id': 'stale', 'inn': '456', 'name': 'Stale', 'phone': '', 'person': ''}))
    manager = org_manager.OrganizationManager(str(source))
    assert [org.inn for org in manager.list()] == ['123']
    assert manager.storage_dir == str(cache)
    assert list(source.iterdir()) == [source / 'example.json']
    assert (source / 'example.json').read_text() == '{}'
    assert not list(cache.glob('*.tmp'))
    storage.list_files.assert_called_once_with('s3://orgs/reference', '*.json', include_processed=True, required=True)
    storage.upload.assert_not_called()


@pytest.mark.parametrize('action', ['save', 'upload_one', 'upload_all'])
def test_read_only_denies_every_write_before_changing_memory_or_disk(tmp_path, monkeypatch, action):
    source, cache, _, storage = make_manager(tmp_path, monkeypatch)
    manager = org_manager.OrganizationManager(str(source))
    old_files = {p.name: p.read_bytes() for p in cache.iterdir()}
    with pytest.raises(PermissionError):
        if action == 'save':
            manager.save_local(org_manager.Organization('New', '', '', org_id='new'))
        elif action == 'upload_one':
            manager._sync_to_s3(str(cache / 'live.json'))
        else:
            manager.sync_to_s3('other', 'all.json')
    assert [org.org_id for org in manager.list()] == ['live']
    assert old_files == {p.name: p.read_bytes() for p in cache.iterdir()}
    storage.upload.assert_not_called()


@pytest.mark.parametrize('failure', ['listing', 'download', 'invalid_json', 'invalid_card'])
def test_sync_failure_cannot_use_stale_card(tmp_path, monkeypatch, failure):
    source, cache, _, storage = make_manager(tmp_path, monkeypatch)
    (cache / 'live.json').write_text('{"stale": true}')
    if failure == 'listing':
        storage.list_files.side_effect = OSError('storage unavailable')
    elif failure == 'download':
        def incomplete(remote, local):
            Path(local).write_text('{')
            raise OSError('connection lost')
        storage.download.side_effect = incomplete
    elif failure == 'invalid_json':
        storage.download.side_effect = lambda remote, local: Path(local).write_text('{')
    else:
        storage.download.side_effect = lambda remote, local: Path(local).write_text('{"org_id": "bad"}')
    with pytest.raises((OSError, ValueError, TypeError)):
        org_manager.OrganizationManager(str(source))
    assert not list(cache.glob('*.tmp'))
    if failure != 'invalid_card':
        assert (cache / 'live.json').read_text() == '{"stale": true}'
    storage.upload.assert_not_called()


def test_removed_remote_card_is_not_reloaded_from_cache(tmp_path, monkeypatch):
    source, _, _, storage = make_manager(tmp_path, monkeypatch)
    manager = org_manager.OrganizationManager(str(source))
    storage.list_files.return_value = []
    manager._sync_from_s3()
    assert manager.list() == []
    storage.upload.assert_not_called()


def test_legacy_local_mode_keeps_original_directory_and_writes(tmp_path, monkeypatch):
    monkeypatch.setattr(org_manager, 'load_config', lambda: {})
    manager = org_manager.OrganizationManager(str(tmp_path))
    manager.save_local(org_manager.Organization('Local', '', '', org_id='local'))
    assert (tmp_path / 'local.json').exists()


def test_strict_s3_listing_propagates_outage_and_ignores_task_tags():
    storage = S3Storage.__new__(S3Storage)
    storage.s3 = MagicMock()
    paginator = storage.s3.get_paginator.return_value
    paginator.paginate.side_effect = OSError('listing unavailable')
    with pytest.raises(OSError):
        storage.list_files('s3://orgs/reference', '*.json', include_processed=True, required=True)
    assert storage.list_files('s3://orgs/reference', '*.json') == []
    paginator.paginate.side_effect = None
    paginator.paginate.return_value = [{'Contents': [{'Key': 'reference/live.json'}]}]
    assert storage.list_files('s3://orgs/reference', '*.json', include_processed=True, required=True) == ['s3://orgs/reference/live.json']
    storage.s3.get_object_tagging.assert_not_called()
