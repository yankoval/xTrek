import json
import pytest
from unittest.mock import MagicMock, patch
from xtrek.create_emission_task_sample import (
    _create_pallet_assignment,
    create_equipment_aggregation_task,
)
from xtrek.storage import S3Storage

@pytest.fixture
def mock_suz_config():
    return {
        'production_orders_path': 'prod_orders',
        'equipment-tasks': 'eq_tasks',
        'equipment-reports': 'eq_reports',
        's3_config': {'bucket': 'test-bucket'},
        'sscc_service_url': 'https://sscc.example.test',
        'sscc_prefix': '460705179',
        'sscc_extension': '0',
        'pallet_sscc_reserve': 1,
    }

@patch('xtrek.create_emission_task_sample.load_config')
@patch('xtrek.create_emission_task_sample.get_storage')
@patch('xtrek.create_emission_task_sample.get_sscc_from_service')
def test_create_equipment_aggregation_task_success(
    mock_get_sscc,
    mock_get_storage,
    mock_load_config,
    mock_suz_config,
):
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
        "Quantity": "17",
        "numBoxesInPallet": 8,
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
    mock_get_sscc.return_value = [
        "046070517921585754",
        "046070517921585761",
        "046070517921585778",
        "046070517921585785",
    ]

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
    assert task_data["reportSchemaVersion"] == 2
    assert task_data["numBoxesInPallet"] == 8
    assert task_data["plannedPalletCount"] == 3
    assert task_data["palletSsccReserve"] == 1
    assert task_data["palletNumbers"] == mock_get_sscc.return_value
    mock_get_sscc.assert_called_once_with(
        'https://sscc.example.test',
        '460705179',
        4,
        '0',
    )


@patch('xtrek.create_emission_task_sample.load_config')
@patch('xtrek.create_emission_task_sample.get_storage')
@patch('xtrek.create_emission_task_sample.get_sscc_from_service')
def test_create_equipment_aggregation_task_without_norm_uses_one_v1_pallet(
    mock_get_sscc,
    mock_get_storage,
    mock_load_config,
    mock_suz_config,
):
    mock_load_config.return_value = mock_suz_config

    mock_storage_prod = MagicMock()
    mock_storage_tasks = MagicMock()
    mock_storage_reports = MagicMock(spec=S3Storage)
    mock_storage_reports.s3 = MagicMock()

    def get_storage_side_effect(path, config):
        if path == 'prod_orders':
            return mock_storage_prod
        if path == 'eq_tasks':
            return mock_storage_tasks
        if path == 'eq_reports':
            return mock_storage_reports
        return MagicMock()

    mock_get_storage.side_effect = get_storage_side_effect
    mock_storage_tasks.exists.return_value = False
    mock_storage_prod.exists.return_value = True
    mock_storage_prod.read_text.return_value = json.dumps({
        "Gtin": "01234567890123",
        "Quantity": "17",
        "PasportData": {},
    })
    mock_storage_reports._parse_s3_url.return_value = ("bucket", "reports")
    mock_storage_reports.s3.generate_presigned_url.return_value = (
        "http://presigned.url/PROD123.json"
    )
    mock_get_sscc.return_value = ["046070517921585754"]

    uploaded_data = {}

    def upload_side_effect(local_path, target_path):
        with open(local_path, 'r', encoding='utf-8') as file:
            uploaded_data['content'] = json.load(file)

    mock_storage_tasks.upload.side_effect = upload_side_effect

    assert create_equipment_aggregation_task("PROD123") == "PROD123"

    task_data = uploaded_data['content']
    assert task_data["reportSchemaVersion"] == 1
    assert "numBoxesInPallet" not in task_data
    assert task_data["plannedPalletCount"] == 1
    assert task_data["palletSsccReserve"] == 0
    assert task_data["palletNumbers"] == ["046070517921585754"]
    mock_get_sscc.assert_called_once_with(
        'https://sscc.example.test',
        '460705179',
        1,
        '0',
    )


@pytest.mark.parametrize("invalid_norm", [0, -1, "invalid"])
def test_pallet_assignment_rejects_invalid_explicit_norm(
    invalid_norm,
    mock_suz_config,
):
    with pytest.raises(ValueError, match="numBoxesInPallet"):
        _create_pallet_assignment(
            {
                "Quantity": "10",
                "numBoxesInPallet": invalid_norm,
            },
            mock_suz_config,
        )


@patch('xtrek.create_emission_task_sample.get_sscc_from_service')
def test_pallet_assignment_rejects_duplicate_sscc(
    mock_get_sscc,
    mock_suz_config,
):
    mock_get_sscc.return_value = [
        "046070517921585754",
        "046070517921585754",
    ]

    with pytest.raises(ValueError, match="duplicate pallet code"):
        _create_pallet_assignment(
            {
                "Quantity": "8",
                "numBoxesInPallet": 8,
            },
            mock_suz_config,
        )

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
