import json
import pytest
from unittest.mock import MagicMock, patch
from suz_api_models import IntroduceMessage, IntroduceProduct, GtinDocument

def test_introduce_message_serialization():
    doc = GtinDocument(
        certificate_date="2024-09-27",
        certificate_number="CERT123",
        certificate_type="CONFORMITY_DECLARATION"
    )
    product = IntroduceProduct(
        uit_code="0104610117624776215!3krb",
        tnved_code="3305900009",
        certificate_document_data=[doc]
    )
    msg = IntroduceMessage(
        production_date="2026-04-01",
        owner_inn="7733154124",
        producer_inn="7733154124",
        participant_inn="7733154124",
        products=[product]
    )

    json_str = msg.to_json()
    data = json.loads(json_str)

    assert data["production_type"] == "OWN_PRODUCTION"
    assert data["owner_inn"] == "7733154124"
    assert len(data["products"]) == 1
    assert data["products"][0]["tnved_code"] == "3305900009"
    assert data["products"][0]["certificate_document_data"][0]["certificate_number"] == "CERT123"

@patch("create_emission_task_sample.load_config")
@patch("create_emission_task_sample.get_storage")
@patch("create_emission_task_sample.get_inn_by_gtin")
@patch("create_emission_task_sample.NK")
@patch("create_emission_task_sample.OrganizationManager")
@patch("create_emission_task_sample.TokenProcessor")
def test_create_introduce_task_basic(mock_token_proc, mock_org_man, mock_nk, mock_get_inn, mock_get_storage, mock_load_config):
    from create_emission_task_sample import create_introduce_task

    mock_load_config.return_value = {
        "kodes": "s3://bucket/kodes",
        "introduce-tasks": "s3://bucket/intro",
        "emission_receipts": "s3://bucket/receipts",
        "production_orders_path": "s3://bucket/prod"
    }

    mock_storage = MagicMock()
    mock_get_storage.return_value = mock_storage
    mock_storage.exists.return_value = True
    mock_storage.read_text.return_value = json.dumps({"codes": ["0104610117624776215!3krb"]})

    mock_get_inn.return_value = "7733154124"

    mock_nk_inst = mock_nk.return_value
    # Test with result as a list
    mock_nk_inst.get_set_by_gtin.return_value = {"result": [{"tnved_code": "3305900009"}]}
    mock_nk_inst.get_permit_document_by_gtin.return_value = [
        GtinDocument("2024-09-27", "CERT123", "CONFORMITY_DECLARATION")
    ]

    mock_token_proc.return_value.get_token_value_by_inn.return_value = "fake_token"

    # Mock for emission receipts search
    mock_storage.list_objects_v2.return_value = {"Contents": []}

    with patch("create_emission_task_sample.Path") as mock_path:
        mock_path.return_value.stem = "receipt_uuid"

        # We need to mock more for the loop over files if it's local, but let's assume S3 for simplicity or mock the loop
        # Actually create_introduce_task tries to find production_order_id.

        res = create_introduce_task("uuid", production_date="2026-04-01")

    assert res == "uuid"
    mock_storage.upload.assert_called()
