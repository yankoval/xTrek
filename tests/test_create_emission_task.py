import pytest
import json
from unittest.mock import MagicMock, patch
from create_emission_task_sample import create_emission_task

@pytest.fixture
def mock_dependencies():
    with patch('create_emission_task_sample.load_config') as mock_load_config, \
         patch('create_emission_task_sample.get_storage') as mock_get_storage, \
         patch('create_emission_task_sample.get_inn_by_gtin') as mock_get_inn, \
         patch('create_emission_task_sample.OrganizationManager') as mock_om, \
         patch('create_emission_task_sample.TokenProcessor') as mock_tp, \
         patch('create_emission_task_sample.get_new_token') as mock_get_new_token, \
         patch('create_emission_task_sample.NK') as mock_nk, \
         patch('create_emission_task_sample.Path.unlink'):

        yield {
            'load_config': mock_load_config,
            'get_storage': mock_get_storage,
            'get_inn': mock_get_inn,
            'om': mock_om,
            'tp': mock_tp,
            'get_new_token': mock_get_new_token,
            'nk': mock_nk
        }

def test_create_emission_task_set(mock_dependencies):
    deps = mock_dependencies

    # Mock config
    deps['load_config'].return_value = {
        'production_orders_path': 's3://bucket/prod/',
        'emission_orders_path': 's3://bucket/emission/'
    }

    # Mock storage
    mock_storage = MagicMock()
    deps['get_storage'].return_value = mock_storage
    mock_storage.exists.return_value = True
    prod_order_data = {
        'Gtin': '12345678901234',
        'Quantity': '10',
        'PasportData': {'Product_PackQty': '5'}
    }
    mock_storage.read_text.return_value = json.dumps(prod_order_data)

    # Mock INN
    deps['get_inn'].return_value = '1234567890'

    # Mock Token
    deps['tp'].return_value.get_token_value_by_inn.return_value = 'fake-token'

    # Mock NK.feedProduct for SET
    mock_nk_instance = deps['nk'].return_value
    mock_nk_instance.feedProduct.return_value = {
        'result': [{'is_set': True}]
    }

    result = create_emission_task('order123_set', 'chemistry', 'contact')

    assert result == 'order123_set'
    # Verify cisType is SET
    args, kwargs = mock_storage.upload.call_args
    uploaded_file = args[0]
    with open(uploaded_file, 'r') as f:
        data = json.load(f)
        assert data['products'][0]['cisType'] == 'SET'

def test_create_emission_task_unit(mock_dependencies):
    deps = mock_dependencies

    # Mock config
    deps['load_config'].return_value = {
        'production_orders_path': 's3://bucket/prod/',
        'emission_orders_path': 's3://bucket/emission/'
    }

    # Mock storage
    mock_storage = MagicMock()
    deps['get_storage'].return_value = mock_storage
    mock_storage.exists.return_value = True
    prod_order_data = {
        'Gtin': '12345678901234',
        'Quantity': '10',
        'PasportData': {'Product_PackQty': '5'}
    }
    mock_storage.read_text.return_value = json.dumps(prod_order_data)

    # Mock INN
    deps['get_inn'].return_value = '1234567890'

    # Mock Token
    deps['tp'].return_value.get_token_value_by_inn.return_value = 'fake-token'

    # Mock NK.feedProduct for UNIT
    mock_nk_instance = deps['nk'].return_value
    mock_nk_instance.feedProduct.return_value = {
        'result': [{'is_set': False}]
    }

    result = create_emission_task('order123_unit', 'chemistry', 'contact')

    assert result == 'order123_unit'
    # Verify cisType is UNIT
    args, kwargs = mock_storage.upload.call_args
    uploaded_file = args[0]
    with open(uploaded_file, 'r') as f:
        data = json.load(f)
        assert data['products'][0]['cisType'] == 'UNIT'

def test_create_emission_task_nk_failure(mock_dependencies):
    deps = mock_dependencies

    # Mock config
    deps['load_config'].return_value = {
        'production_orders_path': 's3://bucket/prod/',
        'emission_orders_path': 's3://bucket/emission/'
    }

    # Mock storage
    mock_storage = MagicMock()
    deps['get_storage'].return_value = mock_storage
    mock_storage.exists.return_value = True
    prod_order_data = {
        'Gtin': '12345678901234',
        'Quantity': '10',
        'PasportData': {'Product_PackQty': '5'}
    }
    mock_storage.read_text.return_value = json.dumps(prod_order_data)

    # Mock INN
    deps['get_inn'].return_value = '1234567890'

    # Mock Token
    deps['tp'].return_value.get_token_value_by_inn.return_value = 'fake-token'

    # Mock NK.feedProduct failure
    mock_nk_instance = deps['nk'].return_value
    mock_nk_instance.feedProduct.return_value = None

    # We expect an exception now
    result = create_emission_task('order123_fail', 'chemistry', 'contact')
    assert result is None
