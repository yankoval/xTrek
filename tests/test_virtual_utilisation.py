import json
import pytest
from unittest.mock import patch, MagicMock
from xtrek.create_emission_task_sample import create_virtual_utilisation_task
from xtrek.suz_api_models import ProductionOrder

@pytest.fixture
def mock_config():
    with patch('xtrek.create_emission_task_sample.load_config') as m:
        m.return_value = {
            'production_orders_path': 'local://prod_orders',
            'emission_receipts': 'local://receipts'
        }
        yield m

@pytest.fixture
def mock_storage():
    with patch('xtrek.create_emission_task_sample.get_storage') as m:
        mock_s = MagicMock()
        m.return_value = mock_s
        yield mock_s

def test_create_virtual_utilisation_task_virtual_true(mock_config, mock_storage):
    order_id = "suz-order-123"
    prod_order_id = "prod-order-456"

    # Mock _find_production_order_id_by_suz_order_id
    with patch('xtrek.create_emission_task_sample._find_production_order_id_by_suz_order_id') as mock_find:
        mock_find.return_value = prod_order_id

        # Mock production order content
        mock_storage.exists.return_value = True
        mock_storage.read_text.return_value = json.dumps({
            "virtual": True,
            "Article": "ART1",
            "Gtin": "GTIN1",
            "Quantity": "10",
            "PasportData": {}
        })

        # Mock create_utilisation_task
        with patch('xtrek.create_emission_task_sample.create_utilisation_task') as mock_create:
            mock_create.return_value = order_id

            result = create_virtual_utilisation_task(order_id, "group1")

            assert result == order_id
            mock_create.assert_called_once()

def test_create_virtual_utilisation_task_virtual_false(mock_config, mock_storage):
    order_id = "suz-order-123"
    prod_order_id = "prod-order-456"

    # Mock _find_production_order_id_by_suz_order_id
    with patch('xtrek.create_emission_task_sample._find_production_order_id_by_suz_order_id') as mock_find:
        mock_find.return_value = prod_order_id

        # Mock production order content
        mock_storage.exists.return_value = True
        mock_storage.read_text.return_value = json.dumps({
            "virtual": False,
            "Article": "ART1",
            "Gtin": "GTIN1",
            "Quantity": "10",
            "PasportData": {
                "Article": "ART1",
                "Product_article": "ART1"
            }
        })

        # Mock create_utilisation_task
        with patch('xtrek.create_emission_task_sample.create_utilisation_task') as mock_create:
            result = create_virtual_utilisation_task(order_id, "group1")

            assert isinstance(result, ProductionOrder)
            assert result.virtual is False
            assert result.Article == "ART1"
            mock_create.assert_not_called()
