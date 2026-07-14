from unittest.mock import MagicMock, patch

from xtrek import vbg


@patch("xtrek.storage.get_storage")
def test_read_token_from_config_reads_s3_path(mock_get_storage):
    storage = MagicMock()
    storage.read_text.return_value = " token-from-s3 \n"
    mock_get_storage.return_value = storage

    token = vbg.read_token_from_config({
        "vbg_api_key_path": "s3://bucket/secrets/vbg.key",
        "s3_config": {"endpoint_url": "https://storage.yandexcloud.net"},
    })

    assert token == "token-from-s3"
    mock_get_storage.assert_called_once_with(
        "s3://bucket/secrets/vbg.key",
        {"endpoint_url": "https://storage.yandexcloud.net"},
    )
    storage.read_text.assert_called_once_with("s3://bucket/secrets/vbg.key")


@patch("xtrek.storage.get_storage")
def test_upload_token_to_s3_uses_storage_upload(mock_get_storage, tmp_path):
    local_token = tmp_path / "vbg.key"
    local_token.write_text("token", encoding="utf-8")
    storage = MagicMock()
    mock_get_storage.return_value = storage

    result = vbg.upload_token_to_s3(
        "s3://bucket/secrets/vbg.key",
        local_path=str(local_token),
        config={"s3_config": {"region_name": "ru-central1"}},
    )

    assert result == "s3://bucket/secrets/vbg.key"
    mock_get_storage.assert_called_once_with(
        "s3://bucket/secrets/vbg.key",
        {"region_name": "ru-central1"},
    )
    storage.upload.assert_called_once_with(str(local_token), "s3://bucket/secrets/vbg.key")
