import pytest
import json
import os
from unittest.mock import MagicMock, patch
from xtrek.utils import AggregationAnalyzer, check_aggregation_report, check_aggregation_reports

@pytest.fixture
def mock_api():
    return MagicMock()

@pytest.fixture
def mock_nk():
    return MagicMock()

@pytest.fixture
def analyzer(mock_api, mock_nk):
    return AggregationAnalyzer(mock_api, mock_nk)

def test_uniqueness_check(analyzer, tmp_path):
    # Create a file with duplicate codes
    file1 = tmp_path / "file1.json"

    data1 = {
        "readyBox": [
            {
                "boxNumber": "BOX1\u001d93tail",
                "productNumbersFull": ["CHILD1\u001d93tail", "CHILD2\u001d93tail"]
            },
            {
                "boxNumber": "BOX1\u001d93tail", # Duplicate box
                "productNumbersFull": ["CHILD2\u001d93tail", "CHILD3\u001d93tail"] # Duplicate child
            }
        ]
    }

    file1.write_text(json.dumps(data1))

    # We mock API calls to avoid network errors
    analyzer.check_statuses = MagicMock(return_value=[])
    # Disable min_sscc check for this test
    analyzer.min_sscc = 0

    errors = analyzer.check_report(str(file1))

    assert errors is not None
    assert "duplicateaggregation" in errors
    assert "BOX1" in errors["duplicateaggregation"]
    assert "duplicateattachment" in errors
    assert "CHILD2" in errors["duplicateattachment"]

def test_sscc_normalization(analyzer, tmp_path):
    file1 = tmp_path / "file1.json"
    # 18 digits SSCC
    sscc_18 = "123456789012345678"
    data = {
        "readyBox": [
            {
                "boxNumber": sscc_18,
                "productNumbersFull": []
            }
        ]
    }
    file1.write_text(json.dumps(data))

    # Mock API to return status for normalized SSCC
    def mock_check(codes):
        results = []
        for c in codes:
            if c == "00" + sscc_18:
                results.append({"cisInfo": {"cis": c, "status": "EMITTED"}})
            else:
                results.append({"cisInfo": {}})
        return results

    analyzer.check_statuses = MagicMock(side_effect=mock_check)
    analyzer.min_sscc = 0

    errors = analyzer.check_report(str(file1))

    assert errors is not None
    assert "alreadyregistered" in errors
    assert any(f"00{sscc_18}" in e for e in errors["alreadyregistered"])

@patch('xtrek.utils.get_inn_by_gtin')
@patch('xtrek.utils.TokenProcessor')
@patch('xtrek.utils.get_storage')
def test_auto_inn_detection(mock_get_storage, mock_tp_class, mock_get_inn, tmp_path):
    # Setup mock storage to return a file with a GTIN
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

    from xtrek.utils import main
    import sys
    from unittest.mock import patch as patch_args

    with patch_args.object(sys, 'argv', ['utils.py', str(tmp_path / "dummy.json")]):
        with patch('xtrek.utils.HonestSignAPI') as mock_api_class:
            with patch('xtrek.utils.NK') as mock_nk_class:
                with patch('xtrek.utils.check_aggregation_reports') as mock_check_reports:
                    mock_check_reports.return_value = {}
                    main()

                    # Verify auto detection was called
                    mock_get_inn.assert_called_with(gtin)
                    mock_tp.get_token_by_inn.assert_called_with("1234567890")

                    # Verify API was initialized with the auto token
                    mock_api_class.assert_called_with(token="AUTO_TOKEN")

def test_path_resolution():
    from xtrek.utils import resolve_file_path
    config = {
        "s3_config": {"bucket": "test-bucket"},
        "equipment-reports": "reports"
    }

    # Cases where it should resolve
    assert resolve_file_path("uuid-123", config) == "s3://test-bucket/reports/uuid-123.json"

    # S3 path in reports_path
    config_s3 = {
        "equipment-reports": "s3://other-bucket/other-reports"
    }
    assert resolve_file_path("uuid-123", config_s3) == "s3://other-bucket/other-reports/uuid-123.json"

    # Cases where it should NOT resolve
    assert resolve_file_path("s3://bucket/path", config) == "s3://bucket/path"
    assert resolve_file_path("file.json", config) == "file.json"
    assert resolve_file_path("folder/file", config) == "folder/file"
    assert resolve_file_path("uuid-123", None) == "uuid-123"

def test_status_logic(analyzer, tmp_path):
    file1 = tmp_path / "file1.json"
    # 01 + 14 digits + ...
    # GTIN_SET = 04640286999931
    # GTIN_PROD = 04630234040808
    data = {
        "readyBox": [
            {
                "boxNumber": "BOX_NEW\u001d93tail",
                "productNumbersFull": [
                    "010464028699993121SET_OK\u001d93tail",
                    "010464028699993121SET_BAD\u001d93tail",
                    "010463023404080821PROD_OK\u001d93tail",
                    "010463023404080821PROD_BAD\u001d93tail"
                ]
            },
            {
                "boxNumber": "BOX_EXISTING\u001d93tail",
                "productNumbersFull": []
            }
        ]
    }
    file1.write_text(json.dumps(data))

    # Mock NK
    def mock_feed(gtin):
        if gtin == "04640286999931": return {"result": [{"is_set": True}]}
        if gtin == "04630234040808": return {"result": [{"is_set": False}]}
        return None
    analyzer.nk.feedProduct.side_effect = mock_feed

    # Mock API statuses
    def mock_check(codes):
        results = []
        for c in codes:
            if "BOX_NEW" in c:
                results.append({"cisInfo": {}}) # Not found
            elif "BOX_EXISTING" in c:
                results.append({"cisInfo": {"cis": c, "status": "EMITTED"}}) # Found -> Error
            elif "SET_OK" in c:
                results.append({"cisInfo": {"cis": c, "status": "EMITTED"}}) # OK
            elif "SET_BAD" in c:
                results.append({"cisInfo": {"cis": c, "status": "APPLIED"}}) # Error for SET (APPLIED instead of EMITTED)
            elif "PROD_OK" in c:
                results.append({"cisInfo": {"cis": c, "status": "INTRODUCED"}}) # OK
            elif "PROD_BAD" in c:
                results.append({"cisInfo": {"cis": c, "status": "APPLIED"}}) # Error for PROD
        return results
    analyzer.check_statuses = MagicMock(side_effect=mock_check)
    analyzer.min_sscc = 0

    errors = analyzer.check_report(str(file1))

    assert errors is not None
    # 1. Box existing error
    assert "alreadyregistered" in errors
    assert any("BOX_EXISTING" in e for e in errors["alreadyregistered"])
    # 2. SET bad status error
    assert "wrongsetstatus" in errors
    assert any("SET_BAD" in e for e in errors["wrongsetstatus"])
    # 3. PROD bad status error
    assert "wrongunitstatus" in errors
    assert any("PROD_BAD" in e for e in errors["wrongunitstatus"])

    # Verify OK statuses didn't cause errors
    for err_list in errors.values():
        for e in err_list:
            assert "SET_OK" not in e
            assert "PROD_OK" not in e
            assert "BOX_NEW" not in e

def test_min_sscc_check(analyzer, tmp_path):
    file1 = tmp_path / "file1.json"
    data = {
        "readyBox": [
            {"boxNumber": "BOX1", "productNumbersFull": []},
            {"boxNumber": "BOX2", "productNumbersFull": []}
        ]
    }
    file1.write_text(json.dumps(data))

    analyzer.min_sscc = 5
    analyzer.check_statuses = MagicMock(return_value=[])

    errors = analyzer.check_report(str(file1))
    assert errors is not None
    assert "minssccinaggrep" in errors
    assert "Количество коробок 2 меньше 5" in errors["minssccinaggrep"]

    # Check with enough boxes
    analyzer.min_sscc = 2
    errors = analyzer.check_report(str(file1))
    assert errors is None or "minssccinaggrep" not in errors

@patch('xtrek.utils.get_storage')
def test_tag_setting(mock_get_storage, analyzer, tmp_path):
    mock_storage = MagicMock()
    mock_get_storage.return_value = mock_storage

    file1 = tmp_path / "file1.json"
    data = {"readyBox": [{"boxNumber": "DUPE", "productNumbersFull": []}, {"boxNumber": "DUPE", "productNumbersFull": []}]}
    file1.write_text(json.dumps(data))
    mock_storage.read_text.return_value = json.dumps(data)

    analyzer.min_sscc = 0
    analyzer.check_statuses = MagicMock(return_value=[])

    analyzer.check_report(str(file1))

    # Verify set_tags was called with 'check': 'duplicateaggregation'
    mock_storage.set_tags.assert_called_once()
    args, kwargs = mock_storage.set_tags.call_args
    assert args[0] == str(file1)
    assert args[1]['check'] == 'duplicateaggregation'
