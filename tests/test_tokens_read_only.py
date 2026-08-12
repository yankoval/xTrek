import base64
import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch
from zoneinfo import ZoneInfo

import pytest

from xtrek.tokens import TokenProcessor
from xtrek.nkapi import NK
from xtrek.suz import SUZ


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
