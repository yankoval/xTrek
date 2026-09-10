import base64
import importlib
import json
import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch
from zoneinfo import ZoneInfo

import pytest

from xtrek.tokens import TokenProcessor
from xtrek.nkapi import NK
from xtrek.suz import SUZ
from xtrek import utils


def _jwt(inn, exp):
    payload = base64.urlsafe_b64encode(json.dumps({
        "inn": inn,
        "pid": "pid",
        "exp": int(exp),
        "pad": "x" * 80,
    }).encode()).decode().rstrip("=")
    return f"eyJhbGciOiJIUzI1NiJ9.{payload}.signature"


@pytest.fixture(autouse=True)
def clear_command_snapshots():
    TokenProcessor._command_snapshots.clear()
    yield
    TokenProcessor._command_snapshots.clear()


@pytest.fixture
def orgs_dir(tmp_path):
    path = tmp_path / "orgs"
    path.mkdir()
    return path


def _storage_with(data):
    storage = MagicMock()
    storage.download.side_effect = lambda remote, local: Path(local).write_text(
        json.dumps(data), encoding="utf-8"
    )
    return storage


def test_client_downloads_once_and_never_reads_stale_local_file(tmp_path, orgs_dir):
    stale_file = tmp_path / "tokens.json"
    stale_file.write_text("not valid json", encoding="utf-8")
    current = _jwt("123", datetime.now(timezone.utc).timestamp() + 3600)
    storage = _storage_with([{"Идентификатор": "pid", "Токен": current}])

    with patch("xtrek.tokens.load_config", return_value={"tokens_path": "s3://bucket/tokens.json"}), \
         patch("xtrek.tokens.get_storage", return_value=storage):
        first = TokenProcessor(str(stale_file), str(orgs_dir))
        second = TokenProcessor(str(stale_file), str(orgs_dir))

    assert first.get_jwt_token_value_by_inn("123") == current
    assert second.get_jwt_token_value_by_inn("123") == current
    assert storage.download.call_count == 1
    assert stale_file.read_text(encoding="utf-8") == "not valid json"


def test_clearing_command_snapshot_downloads_fresh_tokens_for_next_command(tmp_path, orgs_dir):
    first_token = _jwt("123", datetime.now(timezone.utc).timestamp() + 3600)
    second_token = _jwt("456", datetime.now(timezone.utc).timestamp() + 3600)
    storage = _storage_with([{"Идентификатор": "pid-1", "Токен": first_token}])

    with patch("xtrek.tokens.load_config", return_value={"tokens_path": "s3://bucket/tokens.json"}), \
         patch("xtrek.tokens.get_storage", return_value=storage):
        first = TokenProcessor(str(tmp_path / "tokens.json"), str(orgs_dir))
        storage.download.side_effect = lambda remote, local: Path(local).write_text(
            json.dumps([{"Идентификатор": "pid-2", "Токен": second_token}]),
            encoding="utf-8",
        )
        TokenProcessor.clear_command_snapshots()
        second = TokenProcessor(str(tmp_path / "tokens.json"), str(orgs_dir))

    assert first.get_jwt_token_value_by_inn("123") == first_token
    assert second.get_jwt_token_value_by_inn("456") == second_token
    assert storage.download.call_count == 2


@pytest.fixture
def report_resources(monkeypatch):
    monkeypatch.delenv("TRUE_API_TOKEN", raising=False)
    config = {"tokens_path": "s3://bucket/tokens.json"}
    monkeypatch.setattr("xtrek.tokens.load_config", lambda: config)
    # Avoid organization file synchronization; these JWTs contain the INN.
    monkeypatch.setattr("xtrek.tokens.OrganizationManager", MagicMock())
    reports = MagicMock()
    reports.read_text.side_effect = lambda path: json.dumps({
        "readyBox": [{"productNumbersFull": [
            "01" + {"first.json": "04610117654308", "second.json": "04670404506352"}[path] + "21serial"
        ]}]
    })
    monkeypatch.setattr(utils, "get_storage", lambda *args: reports)
    monkeypatch.setattr(utils, "get_inn_by_gtin", lambda gtin: {
        "04610117654308": "123", "04670404506352": "456"
    }[gtin])
    return config


def test_celery_picks_up_updated_token_without_process_restart(report_resources, monkeypatch):
    from celery import Celery

    # Load the production signal handlers with harmless local configuration.
    monkeypatch.setenv("YMQ_ACCESS_KEY", "test-access")
    monkeypatch.setenv("YMQ_SECRET_KEY", "test-secret")
    monkeypatch.setenv("YMQ_QUEUE_URL", "https://example.test/queue")
    # Use a separate module namespace so legacy-entrypoint tests stay isolated.
    spec = importlib.util.spec_from_file_location(
        "xtrek._token_refresh_test_tasks", Path(utils.__file__).with_name("tasks.py")
    )
    tasks = importlib.util.module_from_spec(spec)
    with patch("xtrek.config_loader.load_config", return_value={
        "input_bucket": "input-bucket", "internal_bucket": "internal-bucket",
        "product_group": "chemistry", "contact_person": "scan", "sign": "/tmp/sign",
    }):
        spec.loader.exec_module(tasks)
    app = Celery("token-refresh-regression", broker="memory://")
    app.conf.update(task_always_eager=True, task_eager_propagates=True)

    @app.task
    def check_report():
        _, api, nk, _ = utils._ensure_resources("first.json", config=report_resources)
        api.get_list_cis_info(["fake-cis"])
        return os.getpid(), api, nk

    expiry = datetime.now(timezone.utc).timestamp() + 3600
    old_token = _jwt("123", expiry)
    new_token = _jwt("123", expiry + 60)
    source = [{"Идентификатор": "pid", "Токен": old_token}]
    storage = _storage_with(source)

    response = MagicMock(status_code=200)
    response.json.return_value = []
    try:
        with patch("xtrek.tokens.get_storage", return_value=storage), \
             patch("xtrek.trueapi.requests.post", return_value=response) as request:
            first_pid, old_api, old_nk = check_report.delay().get()
            source[0]["Токен"] = new_token
            second_pid, new_api, new_nk = check_report.delay().get()
        assert [call.kwargs["headers"]["Authorization"] for call in request.call_args_list] == [
            f"Bearer {old_token}", f"Bearer {new_token}",
        ]
    finally:
        from celery.signals import task_postrun, task_prerun
        task_prerun.disconnect(tasks._start_token_snapshot)
        task_postrun.disconnect(tasks._finish_token_snapshot)
        app.close()
        tasks.app.close()

    assert first_pid == second_pid == os.getpid()
    assert old_api.headers["Authorization"] == f"Bearer {old_token}"
    assert new_api.headers["Authorization"] == f"Bearer {new_token}"
    assert new_nk.token == new_token
    assert new_api is not old_api
    assert new_nk is not old_nk
    assert storage.download.call_count == 2


def test_reports_select_their_own_inn_with_one_token_download(report_resources):
    expiry = datetime.now(timezone.utc).timestamp() + 3600
    first_token, second_token = _jwt("123", expiry), _jwt("456", expiry)
    storage = _storage_with([
        {"Идентификатор": "pid-1", "Токен": first_token},
        {"Идентификатор": "pid-2", "Токен": second_token},
    ])

    with patch("xtrek.tokens.get_storage", return_value=storage):
        _, first_api, first_nk, _ = utils._ensure_resources("first.json", config=report_resources)
        _, second_api, second_nk, _ = utils._ensure_resources("second.json", config=report_resources)

    assert first_api.token == first_nk.token == first_token
    assert second_api.token == second_nk.token == second_token
    assert storage.download.call_count == 1


def test_report_does_not_reuse_previous_token_when_its_inn_is_unknown(report_resources, monkeypatch):
    token = _jwt("123", datetime.now(timezone.utc).timestamp() + 3600)
    storage = _storage_with([{"Идентификатор": "pid", "Токен": token}])
    with patch("xtrek.tokens.get_storage", return_value=storage):
        utils._ensure_resources("first.json", config=report_resources)
        monkeypatch.setattr(utils, "get_inn_by_gtin", lambda gtin: None)
        with pytest.raises(ValueError, match="Не удалось определить токен"):
            utils._ensure_resources("second.json", config=report_resources)


def test_client_fails_closed_when_s3_is_unavailable(tmp_path, orgs_dir):
    stale_file = tmp_path / "tokens.json"
    stale_file.write_text("[]", encoding="utf-8")
    storage = MagicMock()
    storage.download.side_effect = OSError("network unavailable")

    with patch("xtrek.tokens.load_config", return_value={"tokens_path": "s3://bucket/tokens.json"}), \
         patch("xtrek.tokens.get_storage", return_value=storage), \
         pytest.raises(RuntimeError, match="выполнение xTrek запрещено"):
        TokenProcessor(str(stale_file), str(orgs_dir))


def test_client_rejects_invalid_s3_json(orgs_dir):
    storage = _storage_with({"Токен": "secret"})
    with patch("xtrek.tokens.load_config", return_value={"tokens_path": "s3://bucket/tokens.json"}), \
         patch("xtrek.tokens.get_storage", return_value=storage), \
         pytest.raises(RuntimeError, match="выполнение xTrek запрещено"):
        TokenProcessor(orgs_dir=str(orgs_dir))


def test_client_forbids_save_and_upload(orgs_dir):
    storage = _storage_with([])
    with patch("xtrek.tokens.load_config", return_value={"tokens_path": "s3://bucket/tokens.json"}), \
         patch("xtrek.tokens.get_storage", return_value=storage):
        processor = TokenProcessor(orgs_dir=str(orgs_dir))

    with pytest.raises(PermissionError, match="save_token"):
        processor.save_token("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", conid="connection")
    storage.upload.assert_not_called()


def test_expired_jwt_is_not_extended_by_metadata(tmp_path, orgs_dir):
    expired = _jwt("123", datetime.now(timezone.utc).timestamp() - 60)
    tokens_file = tmp_path / "tokens.json"
    tokens_file.write_text(json.dumps([{
        "Идентификатор": "pid",
        "Токен": expired,
        "ДействуетДо": "2099-01-01T00:00:00",
    }]), encoding="utf-8")

    processor = TokenProcessor(str(tokens_file), str(orgs_dir), tokens_read_only=False)
    assert processor.get_jwt_token_value_by_inn("123") is None


def test_naive_uuid_expiry_is_moscow_time(tmp_path, orgs_dir):
    processor = TokenProcessor(str(tmp_path / "missing.json"), str(orgs_dir), tokens_read_only=False)
    value = (datetime.now(ZoneInfo("Europe/Moscow")).replace(tzinfo=None) + timedelta(hours=1)).isoformat()
    parsed = processor._parse_expiry(value)
    expected = datetime.fromisoformat(value).replace(tzinfo=ZoneInfo("Europe/Moscow")).astimezone(timezone.utc)
    assert parsed == expected


def test_normal_client_sync_is_quiet_at_info(orgs_dir, caplog):
    storage = _storage_with([])
    caplog.set_level(logging.INFO, logger="TokenProcessor")
    with patch("xtrek.tokens.load_config", return_value={"tokens_path": "s3://bucket/tokens.json"}), \
         patch("xtrek.tokens.get_storage", return_value=storage):
        TokenProcessor(orgs_dir=str(orgs_dir))
    assert not [record for record in caplog.records if record.levelno == logging.INFO]


def test_detailed_logging_never_contains_token_value(tmp_path, orgs_dir, caplog):
    secret = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
    tokens_file = tmp_path / "tokens.json"
    tokens_file.write_text(json.dumps([{
        "Идентификатор": "connection",
        "Токен": secret,
        "ДействуетДо": "2099-01-01T00:00:00",
    }]), encoding="utf-8")
    processor = TokenProcessor(str(tokens_file), str(orgs_dir), tokens_read_only=False)
    caplog.set_level(logging.DEBUG, logger="TokenProcessor")

    processor.print_detailed_info()

    assert secret not in caplog.text


def test_nk_reloads_token_and_retries_safe_request_once():
    unauthorized = MagicMock(status_code=401)
    success = MagicMock(status_code=200)
    refresher = MagicMock(return_value="new-jwt")
    api = NK(token="old-jwt", token_refresher=refresher)

    with patch("xtrek.nkapi.requests.get", side_effect=[unauthorized, success]) as request:
        result = api._request("GET", "https://example.test", headers=api._true_api_headers())

    assert result is success
    assert request.call_count == 2
    assert request.call_args.kwargs["headers"]["Authorization"] == "Bearer new-jwt"
    refresher.assert_called_once_with()


def test_suz_reloads_token_and_retries_get_once():
    unauthorized = MagicMock(status_code=403, text="forbidden")
    success = MagicMock(status_code=200)
    success.json.return_value = {"ok": True}
    refresher = MagicMock(return_value="new-client-token")
    api = SUZ(token="old-client-token", omsId="oms", clientToken="connection", token_refresher=refresher)

    with patch("xtrek.suz.requests.get", side_effect=[unauthorized, success]) as request:
        result = api._get("https://example.test")

    assert result == {"ok": True}
    assert request.call_count == 2
    assert request.call_args.kwargs["headers"]["clientToken"] == "new-client-token"
    refresher.assert_called_once_with()
