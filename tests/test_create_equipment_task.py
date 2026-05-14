import json
import pytest
from unittest.mock import MagicMock, patch
from xtrek.create_emission_task_sample import create_equipment_aggregation_task
from xtrek.storage import S3Storage

@pytest.fixture
def mock_suz_config():
    return {
        'production_orders_path': 'prod_orders',
        'equipment-tasks': 'eq_tasks',
        'equipment-reports': 'eq_reports',
        's3_config': {'bucket': 'test-bucket'}
    }

@patch('xtrek.create_emission_task_sample.load_config')
@patch('xtrek.create_emission_task_sample.get_storage')
@patch('xtrek.create_emission_task_sample.Path.unlink')
def test_create_equipment_aggregation_task_success(mock_unlink, mock_get_storage, mock_load_config, mock_suz_config):
    mock_load_config.return_value = mock_suz_config

    mock_storage_prod = MagicMock()
    mock_storage_tasks = MagicMock()

    # Create a mock that has the 's3' attribute
    mock_storage_reports = MagicMock(spec=S3Storage)
    mock_storage_reports.s3 = MagicMock()

    def get_storage_side_effect(path, config):
        if path == 'prod_orders': return mock_storage_prod
        if path == 'eq_tasks': return mock_storage_tasks
        if path == 'eq_reports': return mock_storage_reports
        return MagicMock()

    mock_get_storage.side_effect = get_storage_side_effect

    # Setup production order data
    production_order_id = "PROD123"
    prod_data = {
        "Gtin": "01234567890123",
        "PasportData": {
            "Batch_date_expired": "31.12.2023",
            "Batch_number": "LOT001",
            "Product_PackQty": "10",
            "Product_name_part1": "Product",
            "Product_name_part2": "Name"
        }
    }
    mock_storage_prod.exists.return_value = True
    mock_storage_prod.read_text.return_value = json.dumps(prod_data)

    # Mock S3 presigned URL generation
    mock_storage_reports._parse_s3_url.return_value = ("bucket", "reports")
    mock_storage_reports.s3.generate_presigned_url.return_value = "http://presigned.url/PROD123.json"

    # Mock task existence check
    mock_storage_tasks.exists.return_value = False

    # We need to capture the data passed to storage_tasks.upload
    uploaded_data = {}
    def upload_side_effect(local_path, target_path):
        with open(local_path, 'r', encoding='utf-8') as f:
            uploaded_data['content'] = json.load(f)
        uploaded_data['target_path'] = target_path

    mock_storage_tasks.upload.side_effect = upload_side_effect

    # Call the function
    result = create_equipment_aggregation_task(production_order_id)

    assert result == production_order_id

    assert uploaded_data['target_path'] == "eq_tasks/PROD123.json"
    task_data = uploaded_data['content']

    assert task_data["id"] == production_order_id
    assert "PROD123.json" in task_data["task-export-signed-link"]

@patch('xtrek.create_emission_task_sample.load_config')
@patch('xtrek.create_emission_task_sample.get_storage')
def test_create_equipment_aggregation_task_duplicate(mock_get_storage, mock_load_config, mock_suz_config):
    mock_load_config.return_value = mock_suz_config

    mock_storage_prod = MagicMock()
    mock_storage_tasks = MagicMock()

    def get_storage_side_effect(path, config):
        if path == 'prod_orders': return mock_storage_prod
        if path == 'eq_tasks': return mock_storage_tasks
        return MagicMock()

    mock_get_storage.side_effect = get_storage_side_effect

    production_order_id = "PROD123"
    mock_storage_prod.exists.return_value = True
    mock_storage_prod.read_text.return_value = json.dumps({
        "Gtin": "123",
        "PasportData": {}
    })

    # Mock task existence check - IT EXISTS
    mock_storage_tasks.exists.return_value = True

    # Call the function - it should log error and return None because it catches all exceptions
    result = create_equipment_aggregation_task(production_order_id)
    assert result is None

    # Verify upload was NOT called
    assert not mock_storage_tasks.upload.called
