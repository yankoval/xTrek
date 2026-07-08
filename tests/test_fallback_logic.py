import json
import pytest
from unittest.mock import MagicMock, patch
from xtrek.nkapi import NK
from xtrek.suz_api_models import GtinDocument

@patch("xtrek.create_emission_task_sample.load_config")
@patch("xtrek.create_emission_task_sample.get_storage")
@patch("xtrek.create_emission_task_sample.get_inn_by_gtin")
@patch("xtrek.create_emission_task_sample.NK")
@patch("xtrek.create_emission_task_sample.OrganizationManager")
@patch("xtrek.create_emission_task_sample.TokenProcessor")
def test_process_incoming_task_fallback_to_product_info(mock_token_proc, mock_org_man, mock_nk, mock_get_inn, mock_get_storage, mock_load_config):
    from xtrek.create_emission_task_sample import process_incoming_task

    mock_load_config.return_value = {
        "production_orders_path": "s3://bucket/prod",
        "allowed_gtins": ["04680328040887"]
    }

    mock_storage = MagicMock()
    mock_get_storage.return_value = mock_storage
    mock_storage.get_tags.return_value = {}
    mock_storage.read_text.return_value = json.dumps({
        "Gtin": "04680328040887",
        "Quantity": "10",
        "PasportData": {"Batch_number": "BATCH1"},
        "Article": "ART1"
    })

    mock_get_inn.return_value = "7733154124"
    mock_nk_inst = mock_nk.return_value

    # 1. feedProduct returns None (e.g. on 403)
    mock_nk_inst.feedProduct.return_value = None

    # 2. product_info returns valid data
    mock_nk_inst.product_info.return_value = {
        "gtin": "04680328040887",
        "name": "Fallback Product",
        "inn": "7733154124",
        "isSet": False,
        "tnVedCode10": "3305900009"
    }

    mock_token_proc.return_value.get_token_value_by_inn.return_value = "fake_token"

    result = process_incoming_task("s3://bucket/tasks/task1.json")

    assert result is not None
    assert mock_nk_inst.product_info.called

    # Verify that the normalized task was uploaded
    mock_storage.upload.assert_called()

@patch("xtrek.create_emission_task_sample.load_config")
@patch("xtrek.create_emission_task_sample.get_storage")
@patch("xtrek.create_emission_task_sample.get_inn_by_gtin")
@patch("xtrek.create_emission_task_sample.NK")
@patch("xtrek.create_emission_task_sample.OrganizationManager")
@patch("xtrek.create_emission_task_sample.TokenProcessor")
def test_create_introduce_task_with_fallback(mock_token_proc, mock_org_man, mock_nk, mock_get_inn, mock_get_storage, mock_load_config):
    from xtrek.create_emission_task_sample import create_introduce_task

    mock_load_config.return_value = {
        "kodes": "s3://bucket/kodes",
        "introduce-tasks": "s3://bucket/intro",
        "emission_receipts": "s3://bucket/receipts",
        "production_orders_path": "s3://bucket/prod",
        "emission_orders_path": "s3://bucket/em_orders"
    }

    mock_storage = MagicMock()
    mock_get_storage.return_value = mock_storage
    mock_storage.exists.return_value = True
    mock_storage.read_text.return_value = json.dumps({"codes": ["01046803280408871726070810BATCH1\u001d"]})

    mock_get_inn.return_value = "7733154124"
    mock_nk_inst = mock_nk.return_value

    # Fallback logic
    mock_nk_inst.feedProduct.return_value = None
    mock_nk_inst.product_info.return_value = {
        "gtin": "04680328040887",
        "name": "Fallback Product",
        "inn": "7733154124",
        "isSet": False,
        "tnVedCode10": "3305900009"
    }

    # get_active_permit_documents_by_gtin should use the passed product info
    mock_nk_inst.get_active_permit_documents_by_gtin.return_value = [
        {"number": "CERT123", "date": "2024-09-27", "type": "CONFORMITY_DECLARATION"}
    ]

    mock_token_proc.return_value.get_token_value_by_inn.return_value = "fake_token"

    res = create_introduce_task("uuid", production_date="2026-04-01")

    assert res == "uuid"
    assert mock_nk_inst.product_info.called
    # Check that product info was passed to avoid redundant call
    args, kwargs = mock_nk_inst.get_active_permit_documents_by_gtin.call_args
    assert kwargs.get('product') is not None
    assert kwargs['product']['name'] == "Fallback Product"
