import json
import os
from unittest.mock import MagicMock, patch
import pytest
from xtrek.create_emission_task_sample import create_virtual_production_tasks, create_virtual_tasks_from_equipment_report

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

    mock_nk.feedProduct.side_effect = [
        {"result": [{"is_set": True}]},
        {"result": [{"is_set": False, "good_name": "Comp 1", "article": "ART1"}]}
    ]

    mock_nk.get_set_by_gtin.return_value = {
        "result": [{
            "is_set": True,
            "set_gtins": [
                {"gtin": "111", "quantity": 2}
            ]
        }]
    }

    uploaded_contents = {}
    def mock_upload(local_path, remote_path):
        with open(local_path, 'r', encoding='utf-8') as f:
            uploaded_contents[remote_path] = json.load(f)
    mock_storage.upload.side_effect = mock_upload

    create_virtual_production_tasks("source_order")

    assert "s3://bucket/prod_orders/V-source_order-00000000000111.json" in uploaded_contents

@patch('xtrek.create_emission_task_sample.load_config')
@patch('xtrek.create_emission_task_sample.get_storage')
@patch('xtrek.create_emission_task_sample.create_virtual_production_tasks')
def test_create_virtual_tasks_from_equipment_report_two_step(mock_create_virtual, mock_get_storage, mock_load_config):
    # Setup mocks
    mock_load_config.return_value = {
        'equipment-tasks': 's3://bucket/tasks/',
        'equipment-reports': 's3://bucket/reports/',
        's3_config': {}
    }

    mock_storage_tasks = MagicMock()
    mock_storage_reports = MagicMock()

    # Side effect for get_storage based on the path
    def get_storage_side_effect(path, config):
        if 'tasks' in path:
            return mock_storage_tasks
        if 'reports' in path:
            return mock_storage_reports
        return MagicMock()

    mock_get_storage.side_effect = get_storage_side_effect

    mock_storage_tasks.exists.return_value = True
    task_data = {
        "task-export-signed-link": "https://s3.amazonaws.com/bucket/reports/report123.json?signature=xyz"
    }
    mock_storage_tasks.read_text.return_value = json.dumps(task_data)

    mock_storage_reports.exists.return_value = True
    report_data = {
        "readyBox": [
            {"productNumbersFull": ["code1", "code2", "code3"]}
        ]
    }
    mock_storage_reports.read_text.return_value = json.dumps(report_data)

    # Run
    create_virtual_tasks_from_equipment_report("production_order_123")

    # Verify
    mock_storage_tasks.exists.assert_called_with("s3://bucket/tasks/production_order_123.json")
    mock_storage_reports.exists.assert_called_with("s3://bucket/reports/report123.json")
    mock_create_virtual.assert_called_with("production_order_123", qty=3)
