import pytest
import json
from unittest.mock import MagicMock, patch
from xtrek.utils import AggregationAnalyzer

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
    # Create two files with duplicate codes
    file1 = tmp_path / "file1.json"
    file2 = tmp_path / "file2.json"

    data1 = {
        "readyBox": [
            {
                "boxNumber": "BOX1\u001d93tail",
                "productNumbersFull": ["CHILD1\u001d93tail", "CHILD2\u001d93tail"]
            }
        ]
    }
    data2 = {
        "readyBox": [
            {
                "boxNumber": "BOX1\u001d93tail", # Duplicate box
                "productNumbersFull": ["CHILD2\u001d93tail", "CHILD3\u001d93tail"] # Duplicate child
            }
        ]
    }

    file1.write_text(json.dumps(data1))
    file2.write_text(json.dumps(data2))

    # We mock API calls to avoid network errors
    analyzer.check_statuses = MagicMock(return_value=[])

    errors = analyzer.analyze([str(file1), str(file2)])

    assert errors is not None
    assert any("Дубликат кода агрегации: BOX1" in e for e in errors)
    assert any("Дубликат кода вложения: CHILD2" in e for e in errors)

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

    errors = analyzer.analyze([str(file1)])

    assert errors is not None
    assert any(f"Код агрегации 00{sscc_18} уже зарегистрирован" in e for e in errors)

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
                with patch('xtrek.utils.AggregationAnalyzer') as mock_analyzer_class:
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
        if gtin == "04640286999931": return {"is_set": True}
        if gtin == "04630234040808": return {"is_set": False}
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
                results.append({"cisInfo": {"cis": c, "status": "APPLIED"}}) # OK
            elif "SET_BAD" in c:
                results.append({"cisInfo": {"cis": c, "status": "INTRODUCED"}}) # Error for SET
            elif "PROD_OK" in c:
                results.append({"cisInfo": {"cis": c, "status": "INTRODUCED"}}) # OK
            elif "PROD_BAD" in c:
                results.append({"cisInfo": {"cis": c, "status": "APPLIED"}}) # Error for PROD
        return results
    analyzer.check_statuses = MagicMock(side_effect=mock_check)

    errors = analyzer.analyze([str(file1)])

    assert errors is not None
    # 1. Box existing error
    assert any("Код агрегации BOX_EXISTING уже зарегистрирован" in e for e in errors)
    # 2. SET bad status error
    assert any("Код вложения (НАБОР) 010464028699993121SET_BAD имеет статус INTRODUCED, ожидался APPLIED" in e for e in errors)
    # 3. PROD bad status error
    assert any("Код вложения (ТОВАР) 010463023404080821PROD_BAD имеет статус APPLIED, ожидался INTRODUCED" in e for e in errors)

    # Verify OK statuses didn't cause errors
    assert not any("SET_OK" in e for e in errors)
    assert not any("PROD_OK" in e for e in errors)
    assert not any("BOX_NEW" in e for e in errors)
