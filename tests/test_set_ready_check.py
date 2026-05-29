import json
import pytest
from unittest.mock import MagicMock, patch
from xtrek.utils import set_ready_check

@pytest.fixture
def mock_resources():
    config = {
        'production_orders_path': 'production_orders',
        'emission_receipts': 'emission_receipts',
        'utilisation_reports': 'utilisation_reports',
        'introduces': 'introduces',
        's3_config': {}
    }
    api = MagicMock()
    nk = MagicMock()
    return api, nk, config

def test_set_ready_check_success(mock_resources):
    api, nk, config = mock_resources
    production_order_id = "PROD123"
    path = f"reports/{production_order_id}.json"

    # Mock storage
    with patch('xtrek.utils.get_storage') as mock_get_storage:
        storage_prod = MagicMock()
        storage_receipts = MagicMock()
        storage_util = MagicMock()
        storage_intro = MagicMock()
        storage_rep = MagicMock()

        def side_effect(p, *args, **kwargs):
            if 'production_orders' in p: return storage_prod
            if 'emission_receipts' in p: return storage_receipts
            if 'utilisation_reports' in p: return storage_util
            if 'introduces' in p: return storage_intro
            return storage_rep

        mock_get_storage.side_effect = side_effect

        # 1. Production Order
        storage_prod.exists.return_value = True
        storage_prod.read_text.return_value = json.dumps({'Gtin': '12345678901234'})

        # Mock AggregationAnalyzer.is_set
        with patch('xtrek.utils.AggregationAnalyzer.is_set', return_value=True):

            # 2. Utilisation Report (found by production_order_id)
            storage_util.exists.return_value = True
            storage_util.read_text.return_value = json.dumps({'reportStatus': 'SUCCESS'})

            # 3. Emission Receipt for components
            storage_receipts.exists.side_effect = lambda p: True
            storage_receipts.read_text.side_effect = lambda p: json.dumps({'orderId': 'COMP_ORDER_123'})

            # 4. Components introduction
            nk.get_set_by_gtin.return_value = {
                'result': [{
                    'set_gtins': [{'gtin': '111', 'quantity': 1}, {'gtin': '222', 'quantity': 1}]
                }]
            }

            # Introduces status
            storage_intro.exists.return_value = True
            storage_intro.read_text.return_value = json.dumps({'status': 'CHECKED_OK'})

            # Mock _ensure_resources to return our mocks
            with patch('xtrek.utils._ensure_resources', return_value=(path, api, nk, config)):
                result = set_ready_check(path)

                assert result == "setReady"
                storage_rep.set_tags.assert_called_with(path, {'check': 'setReady'})

def test_set_ready_check_fail_not_set(mock_resources):
    api, nk, config = mock_resources
    path = "reports/PROD123.json"

    with patch('xtrek.utils.get_storage'):
        with patch('xtrek.utils.AggregationAnalyzer.is_set', return_value=False):
            with patch('xtrek.utils._ensure_resources', return_value=(path, api, nk, config)):
                # Mock production order reading
                with patch('xtrek.utils.get_storage') as mock_get_storage:
                    mock_storage = MagicMock()
                    mock_get_storage.return_value = mock_storage
                    mock_storage.exists.return_value = True
                    mock_storage.read_text.return_value = json.dumps({'Gtin': '12345678901234'})

                    result = set_ready_check(path)
                    assert result is None

def test_set_ready_check_fail_util_not_success(mock_resources):
    api, nk, config = mock_resources
    path = "reports/PROD123.json"

    with patch('xtrek.utils._ensure_resources', return_value=(path, api, nk, config)):
        with patch('xtrek.utils.get_storage') as mock_get_storage:
            storage = MagicMock()
            mock_get_storage.return_value = storage
            storage.exists.return_value = True

            # Sequence of read_text calls: prod order, util report
            storage.read_text.side_effect = [
                json.dumps({'Gtin': '12345678901234'}),
                json.dumps({'reportStatus': 'FAILED'})
            ]

            with patch('xtrek.utils.AggregationAnalyzer.is_set', return_value=True):
                result = set_ready_check(path)
                assert result is None
