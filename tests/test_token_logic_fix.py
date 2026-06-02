import json
import os
import pytest
from pathlib import Path
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch
from xtrek.tokens import TokenProcessor

@pytest.fixture
def temp_tokens_file(tmp_path):
    f = tmp_path / "tokens.json"
    f.write_text("[]")
    return f

@pytest.fixture
def temp_orgs_dir(tmp_path):
    d = tmp_path / "my_orgs"
    d.mkdir()
    return d

def test_date_based_sync(temp_tokens_file, temp_orgs_dir):
    with patch('xtrek.tokens.get_storage') as mock_get_storage, \
         patch('xtrek.tokens.load_config', return_value={'tokens_path': 's3://bucket/tokens.json'}):

        mock_s3_storage = MagicMock()
        mock_local_storage = MagicMock()

        def get_storage_side_effect(path, config=None):
            if str(path).startswith('s3://'):
                return mock_s3_storage
            return mock_local_storage

        mock_get_storage.side_effect = get_storage_side_effect

        # Initial setup: S3 is newer
        s3_time = datetime.now(timezone.utc)
        local_time = s3_time - timedelta(minutes=10)

        mock_s3_storage.get_info.return_value = {'LastModified': s3_time, 'Size': 100}
        mock_local_storage.get_info.return_value = {'LastModified': local_time, 'Size': 50}

        tp = TokenProcessor(str(temp_tokens_file), str(temp_orgs_dir))

        # Initial sync happened in __init__ -> _sync_on_init
        assert mock_s3_storage.download.called

        # Reset mock to test get_token_value_by_inn
        mock_s3_storage.download.reset_mock()

        # Now make S3 even newer
        new_s3_time = s3_time + timedelta(minutes=5)
        mock_s3_storage.get_info.return_value = {'LastModified': new_s3_time, 'Size': 100}
        # local_info stays at s3_time (simulated)
        mock_local_storage.get_info.return_value = {'LastModified': s3_time, 'Size': 100}

        tp.get_token_value_by_inn("12345")
        # Should sync because S3 is newer
        assert mock_s3_storage.download.called

def test_save_token_removes_duplicates_jwt(temp_tokens_file, temp_orgs_dir):
    # Setup: one existing JWT token for INN 12345
    # eyJ... contains inn: 12345, pid: pid1
    old_jwt = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpbm4iOiIxMjM0NSIsInBpZCI6InBpZDEiLCJleHAiOjE5MDAwMDAwMDB9.sig"

    with open(temp_tokens_file, 'w') as f:
        json.dump([{
            "Идентификатор": "pid1",
            "Токен": old_jwt,
            "ДействуетДо": "2030-01-01T00:00:00"
        }], f)

    tp = TokenProcessor(str(temp_tokens_file), str(temp_orgs_dir))
    assert len(tp.tokens) == 1

    # New JWT for same INN but different PID (identifier)
    new_jwt = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpbm4iOiIxMjM0NSIsInBpZCI6InBpZDIiLCJleHAiOjE5MDAwMDAwMDB9.sig"
    tp.save_token(new_jwt)

    # Should only have one token now (the new one)
    assert len(tp.tokens) == 1
    assert tp.tokens[0]["Идентификатор"] == "pid2"
    assert tp.tokens[0]["Токен"] == new_jwt

def test_save_token_removes_duplicates_uuid(temp_tokens_file, temp_orgs_dir):
    # Setup: one existing UUID token for INN 12345, Conid con1
    old_uuid = "11111111-2222-3333-4444-555555555555"

    with open(temp_tokens_file, 'w') as f:
        json.dump([{
            "Идентификатор": "con1",
            "Токен": old_uuid,
            "ДействуетДо": "2030-01-01T00:00:00"
        }], f)

    # Mock org manager to return INN 12345 for con1 and con2
    mock_org = MagicMock()
    mock_org.inn = "12345"

    tp = TokenProcessor(str(temp_tokens_file), str(temp_orgs_dir))
    tp.org_manager.find = MagicMock(return_value=mock_org)

    assert len(tp.tokens) == 1

    # New UUID for SAME conid
    new_uuid = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
    tp.save_token(new_uuid, conid="con1")

    assert len(tp.tokens) == 1
    assert tp.tokens[0]["Идентификатор"] == "con1"
    assert tp.tokens[0]["Токен"] == new_uuid

    # New UUID for DIFFERENT conid but same INN (should NOT remove unless conid matches for UUID)
    # Re-reading user requirement: "для одного и того же ИНН (и того же типа/conid) появляется новый токен, старый должен удаляться немедленно"
    # Wait, for UUID we have conid. If conid is different, it might be a different connection for the same organization.
    # User said: (и того же типа/conid)

    third_uuid = "99999999-8888-7777-6666-555555555555"
    tp.save_token(third_uuid, conid="con2")

    # Now we should have 2 tokens: con1 and con2
    assert len(tp.tokens) == 2
    ids = [t["Идентификатор"] for t in tp.tokens]
    assert "con1" in ids
    assert "con2" in ids
