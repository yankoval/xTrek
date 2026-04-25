import unittest
from unittest.mock import patch, MagicMock, mock_open
import json
import os
from pathlib import Path
from xtrek.create_emission_task_sample import get_emission_kodes

class TestGetEmissionKodes(unittest.TestCase):
    def setUp(self):
        self.config = {
            'emissions_path': '/tmp/emissions',
            'kodes': '/tmp/kodes',
            's3_config': {}
        }
        self.order_id = "test-order-id"
        self.gtin = "12345678901234"
        self.oms_id = "test-oms-id"
        self.inn = "1234567890"

    @patch('xtrek.create_emission_task_sample.load_config')
    @patch('xtrek.create_emission_task_sample.get_storage')
    @patch('xtrek.create_emission_task_sample.OrganizationManager')
    @patch('xtrek.create_emission_task_sample.TokenProcessor')
    @patch('xtrek.create_emission_task_sample.SUZ')
    def test_get_emission_kodes_success(self, mock_suz, mock_token_processor, mock_org_manager, mock_get_storage, mock_load_config):
        mock_load_config.return_value = self.config

        # Mock storage for emissions
        mock_storage_emissions = MagicMock()
        mock_storage_emissions.exists.return_value = True
        mock_storage_emissions.read_text.return_value = json.dumps({
            'gtin': self.gtin,
            'omsId': self.oms_id
        })
        # Mock for S3 tagging check (not in LocalStorage)
        mock_storage_emissions._is_processed.return_value = False

        # Mock storage for kodes
        mock_storage_kodes = MagicMock()

        def side_effect_get_storage(path, config):
            if path == self.config['emissions_path']:
                return mock_storage_emissions
            if path == self.config['kodes']:
                return mock_storage_kodes
            return MagicMock()

        mock_get_storage.side_effect = side_effect_get_storage

        # Mock Org Manager
        mock_org = MagicMock()
        mock_org.inn = self.inn
        mock_org.oms_id = self.oms_id
        mock_org.connection_id = 'test-conid'
        mock_org_manager.return_value.list.return_value = [mock_org]

        # Mock Token Processor
        mock_token_processor.return_value.get_token_value_by_inn.return_value = 'test-token'

        # Mock SUZ API
        mock_suz_instance = mock_suz.return_value
        mock_suz_instance.order_status.return_value = [{
            'bufferStatus': 'ACTIVE',
            'availableCodes': 100
        }]
        mock_suz_instance.codes.return_value = {'codes': ['code1', 'code2'], 'blockId': 'block1'}

        # Execute
        result = get_emission_kodes(self.order_id)

        # Verify
        self.assertIsNotNone(result)
        self.assertEqual(len(result['codes']), 2)
        mock_storage_emissions.mark_processing.assert_called()
        mock_suz_instance.codes.assert_called_with(self.order_id, 100, self.gtin)
        mock_storage_kodes.upload.assert_called()
        mock_storage_emissions.mark_finished.assert_called()

    @patch('xtrek.create_emission_task_sample.load_config')
    @patch('xtrek.create_emission_task_sample.get_storage')
    @patch('xtrek.create_emission_task_sample.SUZ')
    def test_get_emission_kodes_too_many_codes(self, mock_suz, mock_get_storage, mock_load_config):
        mock_load_config.return_value = self.config

        mock_storage_emissions = MagicMock()
        mock_storage_emissions.exists.return_value = True
        mock_storage_emissions.read_text.return_value = json.dumps({
            'gtin': self.gtin,
            'omsId': self.oms_id
        })
        mock_get_storage.return_value = mock_storage_emissions

        # Mock SUZ API status with > 10000 codes
        mock_suz_instance = MagicMock()
        mock_suz_instance.order_status.return_value = [{
            'bufferStatus': 'ACTIVE',
            'availableCodes': 10001
        }]

        # We need to ensure SUZ init works in the function
        with patch('xtrek.create_emission_task_sample.OrganizationManager') as mock_om, \
             patch('xtrek.create_emission_task_sample.TokenProcessor') as mock_tp, \
             patch('xtrek.create_emission_task_sample.SUZ', return_value=mock_suz_instance):

            mock_org = MagicMock()
            mock_org.oms_id = self.oms_id
            mock_om.return_value.list.return_value = [mock_org]
            mock_tp.return_value.get_token_value_by_inn.return_value = 'token'

            # Execute and check for ValueError
            result = get_emission_kodes(self.order_id)
            self.assertIsNone(result) # It catches the exception and returns None

            mock_storage_emissions.mark_error.assert_called()

if __name__ == '__main__':
    unittest.main()
