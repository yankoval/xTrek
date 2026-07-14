import json
import pytest
from unittest.mock import MagicMock, patch
from xtrek.nkapi import NK
from xtrek.suz_api_models import GtinDocument
from datetime import date

def test_get_active_permit_documents_filtering():
    # Mock NK with a token
    nk = NK(token="fake_token")

    # 1. Mock response from /product/info
    mock_info_response = MagicMock()
    mock_info_response.status_code = 200
    mock_info_response.json.return_value = {
        "results": [
            {
                "gtin": "04630446581021",
                "inn": "7730057676",
                "certDocList": [
                    {
                        "type": "CONFORMITY_DECLARATION",
                        "number": "ACTIVE_DOC",
                        "date": "2025-03-25",
                        "dateTo": "2030-03-05"
                    },
                    {
                        "type": "STATE_REGISTRATION_CERTIFICATE",
                        "number": "CARD_TRUSTED_SGR",
                        "active": False
                    }
                ]
            }
        ]
    }

    # 2. Mock response from /rd/list
    mock_rd_response = MagicMock()
    mock_rd_response.status_code = 200
    mock_rd_response.json.return_value = {
        "result": {
            "documents": [
                {
                    "type": "CONFORMITY_DECLARATION",
                    "number": "ACTIVE_DOC",
                    "dateFrom": "2025-03-25",
                    "status": "Действует",
                    "indx": 123
                }
            ]
        }
    }

    def side_effect(url, **kwargs):
        if "product/info" in url:
            return mock_info_response
        if "rd/list" in url:
            return mock_rd_response
        return MagicMock(status_code=404)

    with patch("requests.post", side_effect=side_effect):
        docs = nk.get_active_permit_documents_by_gtin("04630446581021")

        assert len(docs) == 2

        docs_by_number = {doc["number"]: doc for doc in docs}
        assert docs_by_number["ACTIVE_DOC"]["type"] == "CONFORMITY_DECLARATION"
        assert docs_by_number["ACTIVE_DOC"]["registryStatus"] == "Действует"

        sgr_doc = docs_by_number["CARD_TRUSTED_SGR"]
        assert sgr_doc["type"] == "STATE_REGISTRATION_CERTIFICATE"
        assert sgr_doc["active"] is False
        assert sgr_doc["registryStatus"] is None

def test_state_registration_certificate_is_included_from_card_when_inactive():
    nk = NK(token="fake_token")

    mock_info_response = MagicMock()
    mock_info_response.status_code = 200
    mock_info_response.json.return_value = {
        "results": [
            {
                "gtin": "04660205470129",
                "inn": "9718180660",
                "name": "Test SGR Product",
                "certDocList": [
                    {
                        "type": "STATE_REGISTRATION_CERTIFICATE",
                        "number": "KG.11.01.09.001.R.000700.02.22",
                        "active": False
                    }
                ]
            }
        ]
    }

    with patch("requests.post", return_value=mock_info_response) as mock_post:
        docs = nk.get_active_permit_documents_by_gtin("04660205470129")

    assert len(docs) == 1
    assert docs[0]["number"] == "KG.11.01.09.001.R.000700.02.22"
    assert docs[0]["type"] == "STATE_REGISTRATION_CERTIFICATE"
    assert docs[0]["date"] == "2022-02-01"
    assert docs[0]["active"] is False
    assert docs[0]["registryStatus"] is None
    assert mock_post.call_count == 1
    assert "product/info" in mock_post.call_args.args[0]

@patch("xtrek.create_emission_task_sample.load_config")
@patch("xtrek.create_emission_task_sample.get_storage")
@patch("xtrek.create_emission_task_sample.get_inn_by_gtin")
@patch("xtrek.create_emission_task_sample.NK")
@patch("xtrek.create_emission_task_sample.OrganizationManager")
@patch("xtrek.create_emission_task_sample.TokenProcessor")
def test_create_introduce_task_uses_new_permit_method(mock_token_proc, mock_org_man, mock_nk, mock_get_inn, mock_get_storage, mock_load_config):
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
    captured_data = {}
    def upload_side_effect(local_path, remote_path):
        with open(local_path, 'r', encoding='utf-8') as f:
            captured_data['json'] = json.load(f)
    mock_storage_intro.upload.side_effect = upload_side_effect

    mock_get_storage.side_effect = lambda path, config: mock_storage_kodes if "kodes" in path else mock_storage_intro
    mock_storage_kodes.exists.return_value = True
    mock_storage_kodes.read_text.return_value = json.dumps({"codes": ["0104610117624776215\u001d!3krb"]})
    mock_get_inn.return_value = "7733154124"

    mock_nk_inst = mock_nk.return_value
    mock_nk_inst.feedProduct.return_value = {
        "result": [{"tnved_code": "3305900009"}]
    }

    # Mock the new method
    mock_nk_inst.get_active_permit_documents_by_gtin.return_value = [
        {
            "number": "ACTIVE_DOC",
            "date": "2026-07-01",
            "type": "CONFORMITY_DECLARATION"
        }
    ]

    mock_token_proc.return_value.get_token_value_by_inn.return_value = "fake_token"

    res = create_introduce_task("uuid", production_date="2026-04-01")

    assert res == "uuid"
    assert mock_nk_inst.get_active_permit_documents_by_gtin.called

    uploaded_data = captured_data['json']
    certs = uploaded_data['products'][0]['certificate_document_data']
    assert len(certs) == 1
    assert certs[0]['certificate_number'] == "ACTIVE_DOC"
