import pytest
import json
import os
from unittest.mock import MagicMock, patch
from xtrek.utils import check_aggregation_report

@patch('xtrek.utils.get_inn_by_gtin')
@patch('xtrek.utils.TokenProcessor')
@patch('xtrek.utils.get_storage')
@patch('xtrek.utils.HonestSignAPI')
@patch('xtrek.utils.NK')
@patch('xtrek.utils.load_config')
def test_check_aggregation_report_auto_discovery(
    mock_load_config, mock_nk_class, mock_api_class, mock_get_storage, mock_tp_class, mock_get_inn, tmp_path
):
    # Setup mock config
    mock_load_config.return_value = {
        's3_config': {'bucket': 'test-bucket'},
        'equipment-reports': 'reports'
    }

    # Setup mock storage
    mock_storage = MagicMock()
    mock_get_storage.return_value = mock_storage

    gtin = "04640286999931"
    data = {
        "readyBox": [
            {
                "boxNumber": f"01{gtin}21SERIAL\u001d93tail",
                "productNumbersFull": []
            }
        ]
    }
    mock_storage.read_text.return_value = json.dumps(data)

    # Mock GS1 processor and TokenProcessor
    mock_get_inn.return_value = "1234567890"
    mock_tp = mock_tp_class.return_value
    mock_tp.get_token_by_inn.return_value = {"Токен": "AUTO_TOKEN"}

    # Mock API and NK
    mock_api = mock_api_class.return_value
    mock_api.token = "AUTO_TOKEN"
    mock_nk = mock_nk_class.return_value

    # Call the function with only one parameter
    report_path = "uuid-123" # Should be resolved via mock_load_config
    result = check_aggregation_report(report_path)

    # Verify auto detection was called
    mock_get_inn.assert_called_with(gtin)
    mock_tp.get_token_by_inn.assert_called_with("1234567890")

    # Verify API and NK were initialized with the auto token
    mock_api_class.assert_called_with(token="AUTO_TOKEN")
    mock_nk_class.assert_called_with(token="AUTO_TOKEN")

    # Verify report was checked
    mock_storage.read_text.assert_called()
