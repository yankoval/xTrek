import pytest
import json
import os
import shutil
import base64
from pathlib import Path
from tokens import TokenProcessor
from datetime import datetime, timedelta

@pytest.fixture
def test_env(tmp_path):
    # Setup tokens.json
    tokens_file = tmp_path / "tokens.json"
    orgs_dir = tmp_path / "my_orgs"
    orgs_dir.mkdir()

    # Create dummy organization
    org_data = {
        "org_id": "org123",
        "name": "Test Org",
        "phone": "123",
        "person": "Test Person",
        "inn": "1234567890",
        "connection_id": "conn_abc"
    }
    with open(orgs_dir / "org123.json", "w") as f:
        json.dump(org_data, f)

    # Create dummy tokens
    # 1. Old JWT (must be > 100 chars for _is_jwt_token)
    payload_old = base64.urlsafe_b64encode(json.dumps({"inn": "1234567890", "exp": 2000000000, "pad": "x"*50}).encode()).decode().rstrip('=')
    old_jwt = f"eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.{payload_old}.sig"
    # 2. New JWT
    payload_new = base64.urlsafe_b64encode(json.dumps({"inn": "1234567890", "exp": 2100000000, "pad": "x"*50}).encode()).decode().rstrip('=')
    new_jwt = f"eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.{payload_new}.sig"

    # 3. UUID token (active)
    uuid_token = "12345678-1234-1234-1234-1234567890ab"

    now = datetime.now()
    tokens = [
        {
            "Идентификатор": "pid1",
            "Токен": old_jwt,
            "ДействуетДо": (now + timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%S")
        },
        {
            "Идентификатор": "pid2",
            "Токен": new_jwt,
            "ДействуетДо": (now + timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%S")
        },
        {
            "Идентификатор": "conn_abc",
            "Токен": uuid_token,
            "ДействуетДо": (now + timedelta(hours=5)).strftime("%Y-%m-%dT%H:%M:%S")
        }
    ]

    with open(tokens_file, "w") as f:
        json.dump(tokens, f)

    return tokens_file, orgs_dir

def test_get_token_value_by_inn_default_jwt(test_env):
    tokens_file, orgs_dir = test_env
    tp = TokenProcessor(str(tokens_file), str(orgs_dir))

    # Should return the newest JWT
    token = tp.get_token_value_by_inn("1234567890")
    assert token is not None
    assert token.startswith("eyJ")

    # Verify it is indeed the newest one by decoding and checking exp
    payload = tp._decode_jwt_payload(token)
    assert payload['exp'] == 2100000000

def test_get_jwt_token_wrapper(test_env):
    tokens_file, orgs_dir = test_env
    tp = TokenProcessor(str(tokens_file), str(orgs_dir))

    token = tp.get_jwt_token_value_by_inn("1234567890")
    assert token is not None
    assert token.startswith("eyJ")

    payload = tp._decode_jwt_payload(token)
    assert payload['exp'] == 2100000000

def test_get_uuid_token_wrapper(test_env):
    tokens_file, orgs_dir = test_env
    tp = TokenProcessor(str(tokens_file), str(orgs_dir))

    token = tp.get_uuid_token_value_by_inn("1234567890")
    assert token == "12345678-1234-1234-1234-1234567890ab"

def test_uuid_synonyms(test_env):
    tokens_file, orgs_dir = test_env
    tp = TokenProcessor(str(tokens_file), str(orgs_dir))

    token_auth = tp.get_token_value_by_inn("1234567890", token_type='auth')
    token_uuid = tp.get_token_value_by_inn("1234567890", token_type='uuid')

    assert token_auth == "12345678-1234-1234-1234-1234567890ab"
    assert token_uuid == "12345678-1234-1234-1234-1234567890ab"

def test_token_not_found(test_env):
    tokens_file, orgs_dir = test_env
    tp = TokenProcessor(str(tokens_file), str(orgs_dir))

    assert tp.get_token_value_by_inn("nonexistent") is None
    assert tp.get_token_value_by_inn("1234567890", token_type='UNKNOWN') is None
