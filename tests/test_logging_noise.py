import json
import logging
from unittest.mock import MagicMock, patch

from xtrek.config_loader import load_config
from xtrek.org_manager import OrganizationManager


def test_successful_config_sources_are_quiet_at_info(tmp_path, monkeypatch, caplog):
    config_path = tmp_path / "tokens_config.json"
    config_path.write_text(json.dumps({"tokens_read_only": True}), encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("suz_worker_config", raising=False)
    monkeypatch.setenv("TEST_TOKENS_CONFIG", str(config_path))
    caplog.set_level(logging.INFO, logger="ConfigLoader")

    assert load_config("TEST_TOKENS_CONFIG")["tokens_read_only"] is True
    assert not [record for record in caplog.records if record.levelno == logging.INFO]


def _manager_for_sync(tmp_path, storage):
    manager = OrganizationManager.__new__(OrganizationManager)
    manager.storage_dir = str(tmp_path)
    manager.storage = storage
    manager.orgs_path = "s3://bucket/firm/"
    return manager


def test_read_only_organization_sync_is_quiet_at_info(tmp_path, caplog):
    storage = MagicMock()
    storage.list_files.return_value = []
    manager = _manager_for_sync(tmp_path, storage)
    caplog.set_level(logging.INFO, logger="OrganizationManager")

    manager._sync_on_init()

    storage.upload.assert_not_called()
    assert not [record for record in caplog.records if record.levelno == logging.INFO]


def test_organization_upload_remains_visible_at_info(tmp_path, caplog):
    local_file = tmp_path / "organization.json"
    local_file.write_text("{}", encoding="utf-8")
    storage = MagicMock()
    storage.list_files.return_value = []
    manager = _manager_for_sync(tmp_path, storage)
    caplog.set_level(logging.INFO, logger="OrganizationManager")

    manager._sync_on_init()

    storage.upload.assert_called_once()
    assert "Выгружено: 1" in caplog.text
