import json
from unittest.mock import MagicMock, patch
from xtrek.create_emission_task_sample import create_virtual_tasks_from_equipment_report

@patch('xtrek.create_emission_task_sample.load_config')
@patch('xtrek.create_emission_task_sample.get_storage')
@patch('xtrek.create_emission_task_sample.create_virtual_production_tasks')
def test_create_virtual_tasks_from_equipment_report_cyrillic(mock_create_virtual, mock_get_storage, mock_load_config):
    mock_load_config.return_value = {
        'equipment-tasks': 's3://bucket/tasks/',
        'equipment-reports': 's3://bucket/reports/',
        's3_config': {}
    }

    mock_storage_tasks = MagicMock()
    mock_storage_reports = MagicMock()

    def get_storage_side_effect(path, config):
        if 'tasks' in path: return mock_storage_tasks
        if 'reports' in path: return mock_storage_reports
        return MagicMock()
    mock_get_storage.side_effect = get_storage_side_effect

    mock_storage_tasks.exists.return_value = True
    # "С" in "report-С.json" is Cyrillic, URL encoded as %D0%A1
    mock_storage_tasks.read_text.return_value = json.dumps({
        "task-export-signed-link": "https://storage.yandexcloud.net/bucket/reports/report-%D0%A1.json"
    })

    mock_storage_reports.exists.return_value = True
    mock_storage_reports.read_text.return_value = json.dumps({
        "readyBox": [{"productNumbersFull": ["code1", "code2"]}]
    })

    # The function should call exists() with decoded path
    create_virtual_tasks_from_equipment_report("order_123")

    # Check if storage_reports.exists was called with the decoded filename
    # report_path = f"{equipment_reports_path.rstrip('/')}/{report_filename}"
    # report_filename should be "report-С.json"
    expected_path = "s3://bucket/reports/report-С.json"
    mock_storage_reports.exists.assert_called_with(expected_path)
    mock_create_virtual.assert_called_with("order_123", qty=2)
