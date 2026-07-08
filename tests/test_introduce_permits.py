import json
import pytest
from unittest.mock import MagicMock, patch
from xtrek.suz_api_models import GtinDocument

@patch("xtrek.create_emission_task_sample.load_config")
@patch("xtrek.create_emission_task_sample.get_storage")
@patch("xtrek.create_emission_task_sample.get_inn_by_gtin")
@patch("xtrek.create_emission_task_sample.NK")
@patch("xtrek.create_emission_task_sample.OrganizationManager")
@patch("xtrek.create_emission_task_sample.TokenProcessor")
def test_create_introduce_task_with_new_permits_logic(mock_token_proc, mock_org_man, mock_nk, mock_get_inn, mock_get_storage, mock_load_config):
    from xtrek.create_emission_task_sample import create_introduce_task

    mock_load_config.return_value = {
        "kodes": "s3://bucket/kodes",
        "introduce-tasks": "s3://bucket/intro",
        "emission_receipts": "s3://bucket/receipts",
        "production_orders_path": "s3://bucket/prod",
        "emission_orders_path": "s3://bucket/em_orders"
    }

    mock_storage_kodes = MagicMock()
    mock_storage_intro = MagicMock()

    def get_storage_side_effect(path, config):
        if "kodes" in path: return mock_storage_kodes
        if "intro" in path: return mock_storage_intro
        return MagicMock()

    mock_get_storage.side_effect = get_storage_side_effect

    mock_storage_kodes.exists.return_value = True
    mock_storage_kodes.read_text.return_value = json.dumps({"codes": ["0104610117624776215\u001d!3krb"]})

    mock_get_inn.return_value = "7733154124"

    mock_nk_inst = mock_nk.return_value

    # Mock feedProduct response
    mock_nk_inst.feedProduct.return_value = {
        "result": [{
            "good_attrs": [
                {
                    "attr_group_id": 22,
                    "attr_id": 13933,
                    "attr_name": "Код ТНВЭД",
                    "attr_value": "3307300000"
                }
            ]
        }]
    }

    mock_nk_inst.get_active_permit_documents_by_gtin.return_value = [
        {
            "number": "ЕАЭС N RU Д-RU.РА09.В.37749/24",
            "date": "2024-10-11",
            "type": "CONFORMITY_DECLARATION"
        }
    ]

    mock_token_proc.return_value.get_token_value_by_inn.return_value = "fake_token"

    res = create_introduce_task("uuid", production_date="2026-04-01")

    assert res == "uuid"

@patch("xtrek.create_emission_task_sample.load_config")
@patch("xtrek.create_emission_task_sample.get_storage")
@patch("xtrek.create_emission_task_sample.get_inn_by_gtin")
@patch("xtrek.create_emission_task_sample.NK")
@patch("xtrek.create_emission_task_sample.OrganizationManager")
@patch("xtrek.create_emission_task_sample.TokenProcessor")
def test_create_introduce_task_with_new_permits_logic_verified(mock_token_proc, mock_org_man, mock_nk, mock_get_inn, mock_get_storage, mock_load_config):
    from xtrek.create_emission_task_sample import create_introduce_task

    mock_load_config.return_value = {
        "kodes": "s3://bucket/kodes",
        "introduce-tasks": "s3://bucket/intro",
        "emission_receipts": "s3://bucket/receipts",
        "production_orders_path": "s3://bucket/prod",
        "emission_orders_path": "s3://bucket/em_orders"
    }

    mock_storage_kodes = MagicMock()
    mock_storage_intro = MagicMock()

    # Capture content during upload
    captured_data = {}
    def upload_side_effect(local_path, remote_path):
        with open(local_path, 'r', encoding='utf-8') as f:
            captured_data['json'] = json.load(f)

    mock_storage_intro.upload.side_effect = upload_side_effect

    def get_storage_side_effect(path, config):
        if "kodes" in path: return mock_storage_kodes
        if "intro" in path: return mock_storage_intro
        return MagicMock()

    mock_get_storage.side_effect = get_storage_side_effect

    mock_storage_kodes.exists.return_value = True
    mock_storage_kodes.read_text.return_value = json.dumps({"codes": ["0104610117624776215\u001d!3krb"]})

    mock_get_inn.return_value = "7733154124"

    mock_nk_inst = mock_nk.return_value

    # Mock feedProduct response
    mock_nk_inst.feedProduct.return_value = {
        "result": [{
            "good_attrs": [
                {
                    "attr_group_id": 22,
                    "attr_id": 13933,
                    "attr_name": "Код ТНВЭД",
                    "attr_value": "3307300000"
                }
            ]
        }]
    }

    mock_nk_inst.get_active_permit_documents_by_gtin.return_value = [
        {
            "number": "ЕАЭС N RU Д-RU.РА09.В.37749/24",
            "date": "2024-10-11",
            "type": "CONFORMITY_DECLARATION"
        }
    ]

    mock_token_proc.return_value.get_token_value_by_inn.return_value = "fake_token"

    res = create_introduce_task("uuid", production_date="2026-04-01")

    assert res == "uuid"

    uploaded_data = captured_data['json']
    product = uploaded_data['products'][0]
    assert product['tnved_code'] == "3307300000"
    assert len(product['certificate_document_data']) == 1
    cert = product['certificate_document_data'][0]
    assert cert['certificate_number'] == "ЕАЭС N RU Д-RU.РА09.В.37749/24"
    assert cert['certificate_date'] == "2024-10-11"
    assert cert['certificate_type'] == "CONFORMITY_DECLARATION"

@patch("xtrek.create_emission_task_sample.load_config")
@patch("xtrek.create_emission_task_sample.get_storage")
@patch("xtrek.create_emission_task_sample.get_inn_by_gtin")
@patch("xtrek.create_emission_task_sample.NK")
@patch("xtrek.create_emission_task_sample.OrganizationManager")
@patch("xtrek.create_emission_task_sample.TokenProcessor")
def test_create_introduce_task_error_no_permits(mock_token_proc, mock_org_man, mock_nk, mock_get_inn, mock_get_storage, mock_load_config):
    from xtrek.create_emission_task_sample import create_introduce_task

    mock_load_config.return_value = {
        "kodes": "s3://bucket/kodes",
        "introduce-tasks": "s3://bucket/intro"
    }

    mock_storage_kodes = MagicMock()
    mock_get_storage.return_value = mock_storage_kodes
    mock_storage_kodes.exists.return_value = True
    mock_storage_kodes.read_text.return_value = json.dumps({"codes": ["0104610117624776215\u001d!3krb"]})

    mock_get_inn.return_value = "7733154124"
    mock_nk_inst = mock_nk.return_value

    mock_nk_inst.feedProduct.return_value = {
        "result": [{
            "tnved_code": "3307300000",
            "good_attrs": []
        }]
    }

    mock_nk_inst.get_active_permit_documents_by_gtin.return_value = []

    mock_token_proc.return_value.get_token_value_by_inn.return_value = "fake_token"

    res = create_introduce_task("uuid", production_date="2026-04-01")

    assert res is None
    mock_storage_kodes.set_tags.assert_called_with("s3://bucket/kodes/uuid.json", {"статусСообщенияВводаВОборот": "Error"})
