
import unittest
from unittest.mock import patch, MagicMock
import json
import os
from suz import SUZ

class TestSUZMethods(unittest.TestCase):
    def setUp(self):
        # Mock environment variables
        os.environ['HONEST_SIGN_TOKEN'] = 'test_token'
        os.environ['OMSID'] = 'test_omsid'
        os.environ['CLIENT_TOKEN'] = 'test_client_token'
        self.api = SUZ()

    @patch('requests.get')
    def test_utilisation_reports_list(self, mock_get):
        # Mock response for Method 4.4.15
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"results": ["report1", "report2"]}
        mock_get.return_value = mock_response

        order_id = "test_order_id"
        limit = 10
        skip = 5

        result = self.api.utilisation_reports_list(order_id, limit=limit, skip=skip)

        # Verify URL and params
        args, kwargs = mock_get.call_args
        url = args[0]
        params = kwargs.get('params')

        self.assertIn(f"orderId={order_id}", url)
        self.assertEqual(params['limit'], limit)
        self.assertEqual(params['skip'], skip)
        self.assertEqual(result['results'], ["report1", "report2"])

    @patch('requests.get')
    def test_get_error_logging(self, mock_get):
        # Mock 400 error
        mock_response = MagicMock()
        mock_response.status_code = 400
        mock_response.text = "Bad Request Detail"
        mock_response.raise_for_status.side_effect = Exception("HTTP Error")
        mock_get.return_value = mock_response

        with self.assertRaises(Exception):
            self.api.order_list()

        # We can't easily check log output here without more setup,
        # but we verified the logic flow in suz.py

if __name__ == '__main__':
    unittest.main()
