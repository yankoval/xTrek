import json
import os
from unittest.mock import MagicMock, patch
import pytest
from xtrek.create_emission_task_sample import create_virtual_production_tasks

@patch('xtrek.create_emission_task_sample.load_config')
@patch('xtrek.create_emission_task_sample.get_storage')
@patch('xtrek.create_emission_task_sample.get_inn_by_gtin')
@patch('xtrek.create_emission_task_sample.OrganizationManager')
@patch('xtrek.create_emission_task_sample.TokenProcessor')
@patch('xtrek.create_emission_task_sample.NK')
def test_create_virtual_production_tasks(mock_nk_class, mock_token_processor_class, mock_org_manager_class, mock_get_inn, mock_get_storage, mock_load_config):
    # Setup mocks
    mock_load_config.return_value = {
        'production_orders_path': 's3://bucket/prod_orders/',
        's3_config': {}
    }

    mock_storage = MagicMock()
    mock_get_storage.return_value = mock_storage
    mock_storage.exists.return_value = True

    source_order = {
        "Gtin": "12345678901234",
        "Quantity": "10",
        "Article": "SET_ART",
        "PasportData": {
            "Batch_number": "B1",
            "Batch_date_production": "2023-01-01"
        }
    }
    mock_storage.read_text.return_value = json.dumps(source_order)

    mock_get_inn.return_value = "1234567890"

    mock_token_processor = mock_token_processor_class.return_value
    mock_token_processor.get_token_value_by_inn.return_value = "mock_jwt_token"

    mock_nk = mock_nk_class.return_value

    # NK response for the SET
    mock_nk.feedProduct.side_effect = [
        {"result": [{"is_set": True}]}, # for source GTIN
        {"result": [{"is_set": False, "good_name": "Comp 1", "article": "ART1"}]}, # for component 1
        {"result": [{"is_set": False, "good_name": "Comp 2", "article": "ART2"}]}  # for component 2
    ]

    mock_nk.get_set_by_gtin.return_value = {
        "result": [{
            "is_set": True,
            "set_gtins": [
                {"gtin": "111", "quantity": 2},
                {"gtin": "222", "quantity": 3}
            ]
        }]
    }

    uploaded_contents = {}

    def mock_upload(local_path, remote_path):
        with open(local_path, 'r', encoding='utf-8') as f:
            uploaded_contents[remote_path] = json.load(f)

    mock_storage.upload.side_effect = mock_upload

    # Run
    create_virtual_production_tasks("source_order.json")

    # Verify
    assert len(uploaded_contents) == 2

    target_path_1 = "s3://bucket/prod_orders/V-source_order-111.json"
    assert target_path_1 in uploaded_contents
    data1 = uploaded_contents[target_path_1]
    assert data1['virtual'] is True
    assert data1['Gtin'] == "111"
    assert data1['Quantity'] == "20" # 10 * 2
    assert data1['PasportData']['Product_gtin'] == "111"
    assert data1['PasportData']['Product_name_part1'] == "Comp 1"
    assert data1['PasportData']['Batch_number'] == "B1"

    target_path_2 = "s3://bucket/prod_orders/V-source_order-222.json"
    assert target_path_2 in uploaded_contents
    data2 = uploaded_contents[target_path_2]
    assert data2['virtual'] is True
    assert data2['Gtin'] == "222"
    assert data2['Quantity'] == "30" # 10 * 3
