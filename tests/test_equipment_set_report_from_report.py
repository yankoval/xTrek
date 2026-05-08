import json
import os
from unittest.mock import MagicMock, patch
from pathlib import Path
import pytest
from xtrek.create_emission_task_sample import create_equipment_set_report_from_report

@patch('xtrek.create_emission_task_sample.load_config')
@patch('xtrek.create_emission_task_sample.get_storage')
@patch('xtrek.create_emission_task_sample.get_inn_by_gtin')
@patch('xtrek.create_emission_task_sample.OrganizationManager')
@patch('xtrek.create_emission_task_sample.TokenProcessor')
@patch('xtrek.create_emission_task_sample.NK')
@patch('xtrek.create_emission_task_sample._get_order_id_from_receipt')
@patch('xtrek.kinGenerator.KinReportGenerator')
def test_create_equipment_set_report_from_report_success(
    mock_generator_class, mock_get_order_id, mock_nk_class, mock_token_processor_class,
    mock_org_manager_class, mock_get_inn, mock_get_storage, mock_load_config
):
    # Setup config
    mock_load_config.return_value = {
        'production_orders_path': 'prod_orders',
        'kodes': 'kodes',
        'equipment_set_reports': 'set_reports',
        'equipment-tasks': 'eq_tasks',
        'equipment-reports': 'eq_reports',
        's3_config': None
    }

    # Setup storages
    mock_storage_prod = MagicMock()
    mock_storage_kodes = MagicMock()
    mock_storage_reports = MagicMock()
    mock_storage_tasks = MagicMock()
    mock_storage_eq_reports = MagicMock()

    def get_storage_side_effect(path, config):
        if 'prod_orders' in path: return mock_storage_prod
        if 'kodes' in path: return mock_storage_kodes
        if 'set_reports' in path: return mock_storage_reports
        if 'eq_tasks' in path: return mock_storage_tasks
        if 'eq_reports' in path: return mock_storage_eq_reports
        return MagicMock()
    mock_get_storage.side_effect = get_storage_side_effect

    # 1. Equipment task and report
    mock_storage_tasks.exists.return_value = True
    mock_storage_tasks.read_text.return_value = json.dumps({
        "task-export-signed-link": "http://example.com/report123.json"
    })
    mock_storage_eq_reports.exists.return_value = True
    mock_storage_eq_reports.read_text.return_value = json.dumps({
        "readyBox": [
            {"productNumbersFull": ["SET_CODE_1"]}
        ]
    })

    # 2. Production order
    mock_storage_prod.exists.return_value = True
    mock_storage_prod.read_text.return_value = json.dumps({
        "Gtin": "01234567890123",
        "PasportData": {"Batch_date_expired": "01.01.2025"}
    })

    # 3. NK and tokens
    mock_get_inn.return_value = "1234567890"
    mock_token_processor = mock_token_processor_class.return_value
    mock_token_processor.get_token_value_by_inn.return_value = "token"
    mock_nk = mock_nk_class.return_value
    mock_nk.get_set_by_gtin.return_value = {
        "result": [{
            "good_name": "Main Set",
            "set_gtins": [{"gtin": "111", "quantity": 1}]
        }]
    }
    mock_nk.feedProduct.return_value = {"result": [{"good_name": "Component 1"}]}

    # 5. Codes collection
    mock_get_order_id.return_value = "ORDER_COMP_1"
    mock_storage_kodes.exists.return_value = True
    mock_storage_kodes.read_text.return_value = json.dumps({"codes": ["COMP_CODE_1"]})

    # 6. Generator
    mock_generator = mock_generator_class.return_value
    report_file = "temp_report.json"
    mock_generator.generate_kin_report.return_value = report_file
    with open(report_file, 'w', encoding='utf-8') as f:
        json.dump({"readyBox": [{"boxNumber": "SET_CODE_1"}]}, f)

    # Call function
    result = create_equipment_set_report_from_report("PROD_123")

    assert result == "PROD_123"
    mock_storage_reports.upload.assert_called()

    # Cleanup
    if os.path.exists(report_file):
        os.unlink(report_file)

@patch('xtrek.create_emission_task_sample.load_config')
@patch('xtrek.create_emission_task_sample.get_storage')
@patch('xtrek.create_emission_task_sample.get_inn_by_gtin')
@patch('xtrek.create_emission_task_sample.OrganizationManager')
@patch('xtrek.create_emission_task_sample.TokenProcessor')
@patch('xtrek.create_emission_task_sample.NK')
@patch('xtrek.create_emission_task_sample._get_order_id_from_receipt')
@patch('xtrek.kinGenerator.KinReportGenerator')
def test_create_equipment_set_report_insufficient_codes(
    mock_generator_class, mock_get_order_id, mock_nk_class, mock_token_processor_class,
    mock_org_manager_class, mock_get_inn, mock_get_storage, mock_load_config
):
    # Setup config
    mock_load_config.return_value = {
        'production_orders_path': 'prod_orders',
        'kodes': 'kodes',
        'equipment_set_reports': 'set_reports',
        'equipment-tasks': 'eq_tasks',
        'equipment-reports': 'eq_reports',
        's3_config': None
    }

    # Setup storages
    mock_storage_prod = MagicMock()
    mock_storage_kodes = MagicMock()
    mock_storage_reports = MagicMock()
    mock_storage_tasks = MagicMock()
    mock_storage_eq_reports = MagicMock()

    def get_storage_side_effect(path, config):
        if 'prod_orders' in path: return mock_storage_prod
        if 'kodes' in path: return mock_storage_kodes
        if 'set_reports' in path: return mock_storage_reports
        if 'eq_tasks' in path: return mock_storage_tasks
        if 'eq_reports' in path: return mock_storage_eq_reports
        return MagicMock()
    mock_get_storage.side_effect = get_storage_side_effect

    # 1. Equipment task and report (REQUEST 2 SETS)
    mock_storage_tasks.exists.return_value = True
    mock_storage_tasks.read_text.return_value = json.dumps({
        "task-export-signed-link": "http://example.com/report123.json"
    })
    mock_storage_eq_reports.exists.return_value = True
    mock_storage_eq_reports.read_text.return_value = json.dumps({
        "readyBox": [
            {"productNumbersFull": ["SET_CODE_1"]},
            {"productNumbersFull": ["SET_CODE_2"]}
        ]
    })

    # 2. Production order
    mock_storage_prod.exists.return_value = True
    mock_storage_prod.read_text.return_value = json.dumps({
        "Gtin": "01234567890123",
        "PasportData": {"Batch_date_expired": "01.01.2025"}
    })

    # 3. NK and tokens
    mock_get_inn.return_value = "1234567890"
    mock_token_processor = mock_token_processor_class.return_value
    mock_token_processor.get_token_value_by_inn.return_value = "token"
    mock_nk = mock_nk_class.return_value
    mock_nk.get_set_by_gtin.return_value = {
        "result": [{
            "good_name": "Main Set",
            "set_gtins": [{"gtin": "111", "quantity": 1}]
        }]
    }
    mock_nk.feedProduct.return_value = {"result": [{"good_name": "Component 1"}]}

    # 5. Codes collection
    mock_get_order_id.return_value = "ORDER_COMP_1"
    mock_storage_kodes.exists.return_value = True
    mock_storage_kodes.read_text.return_value = json.dumps({"codes": ["COMP_CODE_1", "COMP_CODE_2"]})

    # 6. Generator (ONLY GENERATES 1 SET)
    mock_generator = mock_generator_class.return_value
    report_file = "temp_report_fail.json"
    mock_generator.generate_kin_report.return_value = report_file
    with open(report_file, 'w', encoding='utf-8') as f:
        json.dump({"readyBox": [{"boxNumber": "SET_CODE_1"}]}, f)

    # Call function and expect ValueError
    with pytest.raises(ValueError, match="Недостаточно кодов для создания 2 наборов"):
        create_equipment_set_report_from_report("PROD_123")

    # Cleanup
    if os.path.exists(report_file):
        os.unlink(report_file)
