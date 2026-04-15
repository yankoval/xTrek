import json
import pytest
from unittest.mock import MagicMock, patch
from pathlib import Path
from create_emission_task_sample import create_aggregation_report

@pytest.fixture
def mock_config():
    return {
        's3_config': None,
        'equipment-tasks': '/tmp/equipment-tasks',
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

    with patch('create_emission_task_sample.load_config', return_value=mock_config), \
         patch('create_emission_task_sample.get_storage') as mock_get_storage, \
         patch('create_emission_task_sample.get_inn_by_gtin', return_value=inn):

        mock_storage = MagicMock()
        mock_get_storage.return_value = mock_storage

        # Mock exists and read_text for task and report
        mock_storage.exists.side_effect = lambda path: True

        def storage_read_text(path):
            if task_uuid in path:
                return json.dumps(task_data)
            if report_uuid in path:
                return json.dumps(report_data)
            return ""
        mock_storage.read_text.side_effect = storage_read_text

        uploaded_content = None
        def mock_upload(local_path, remote_path):
            nonlocal uploaded_content
            with open(local_path, 'r') as f:
                uploaded_content = json.load(f)

        mock_storage.upload.side_effect = mock_upload

        result = create_aggregation_report(task_uuid)

        assert result == task_uuid

        # Verify upload was called with correct data
        assert mock_storage.upload.called
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

    with patch('create_emission_task_sample.load_config', return_value=mock_config), \
         patch('create_emission_task_sample.get_storage') as mock_get_storage:

        mock_storage = MagicMock()
        mock_get_storage.return_value = mock_storage

        mock_storage.exists.side_effect = lambda path: task_uuid in path # Only task exists
        mock_storage.read_text.return_value = json.dumps(task_data)

        result = create_aggregation_report(task_uuid)
        assert result is None
