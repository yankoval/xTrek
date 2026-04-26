import json
import pytest
from unittest.mock import MagicMock, patch
from pathlib import Path
from xtrek.create_emission_task_sample import create_aggregation_report

@pytest.fixture
def mock_config():
    return {
        's3_config': None,
        'equipment-tasks': '/tmp/equipment-tasks',
        'equipment-reports': '/tmp/equipment-reports',
        'agg-tasks': '/tmp/agg-tasks'
    }

def test_create_aggregation_report_success(mock_config):
    task_uuid = "task-123"
    report_uuid = "report-456"
    gtin = "04640286999894"
    inn = "7733154124"

    task_data = {
        "id": report_uuid,
        "gtin": gtin
    }

    report_data = {
        "id": report_uuid,
        "readyBox": [
            {
                "boxNumber": "046070517911326848",
                "productNumbersFull": [
                    "0104680038240782215pQsAW\u001d930may",
                    "0104680038240782215QNTeB\u001d931IFR"
                ]
            }
        ]
    }

    with patch('xtrek.create_emission_task_sample.load_config', return_value=mock_config), \
         patch('xtrek.create_emission_task_sample.get_storage') as mock_get_storage, \
         patch('xtrek.create_emission_task_sample.get_inn_by_gtin', return_value=inn):

        mock_storage_tasks = MagicMock()
        mock_storage_reports = MagicMock()
        mock_storage_agg = MagicMock()

        def side_effect_get_storage(path, s3_config):
            if path == mock_config['equipment-tasks']:
                return mock_storage_tasks
            if path == mock_config['equipment-reports']:
                return mock_storage_reports
            if path == mock_config['agg-tasks']:
                return mock_storage_agg
            return MagicMock()

        mock_get_storage.side_effect = side_effect_get_storage

        # Mock task storage
        mock_storage_tasks.exists.return_value = True
        mock_storage_tasks.read_text.return_value = json.dumps(task_data)

        # Mock report storage
        mock_storage_reports.exists.return_value = True
        mock_storage_reports.read_text.return_value = json.dumps(report_data)

        # Mock agg storage
        uploaded_content = None
        def mock_upload(local_path, remote_path):
            nonlocal uploaded_content
            with open(local_path, 'r') as f:
                uploaded_content = json.load(f)
        mock_storage_agg.upload.side_effect = mock_upload

        result = create_aggregation_report(task_uuid)

        assert result == task_uuid

        # Verify upload was called with correct data
        assert mock_storage_agg.upload.called
        assert uploaded_content is not None
        assert uploaded_content['participantId'] == inn
        assert len(uploaded_content['aggregationUnits']) == 1
        unit = uploaded_content['aggregationUnits'][0]
        assert unit['unitSerialNumber'] == "00046070517911326848" # 20 digits
        assert unit['sntins'][0] == "0104680038240782215pQsAW" # Clean code
        assert unit['sntins'][1] == "0104680038240782215QNTeB" # Clean code

def test_create_aggregation_report_no_report(mock_config):
    task_uuid = "task-123"
    task_data = {"id": "report-456", "gtin": "123"}

    with patch('xtrek.create_emission_task_sample.load_config', return_value=mock_config), \
         patch('xtrek.create_emission_task_sample.get_storage') as mock_get_storage:

        mock_storage = MagicMock()
        mock_get_storage.return_value = mock_storage

        mock_storage.exists.side_effect = lambda path: task_uuid in path # Only task exists
        mock_storage.read_text.return_value = json.dumps(task_data)

        result = create_aggregation_report(task_uuid)
        assert result is None
