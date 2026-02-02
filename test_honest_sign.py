import unittest
from unittest.mock import patch, MagicMock
import os
import sys

# Настройка путей для импорта trueapi.py
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.insert(0, current_dir)

try:
    from trueapi import HonestSignAPI
except ImportError as e:
    print(f"Ошибка импорта trueapi: {e}")
    sys.exit(1)

class TestHonestSignAPI(unittest.TestCase):

    def setUp(self):
        """Подготовка окружения перед каждым тестом"""
        # Патчим переменные среды, чтобы тесты по умолчанию проходили инициализацию
        self.env_patcher = patch.dict('os.environ', {
            'HONEST_SIGN_TOKEN': 'test_token',
            'OMSID': 'test_omsid',
            'CLIENT_TOKEN': 'test_client'
        })
        self.env_patcher.start()
        self.api = HonestSignAPI()

    def tearDown(self):
        self.env_patcher.stop()

    def test_init_minimal_params(self):
        """Проверка, что для инициализации достаточно только токена"""
        with patch.dict('os.environ', {'HONEST_SIGN_TOKEN': 'only_token'}, clear=True):
            api = HonestSignAPI()
            self.assertEqual(api.token, 'only_token')
            self.assertIsNone(api.omsId)
            self.assertIsNone(api.clientToken)

    def test_init_missing_token_still_raises(self):
        """Проверка, что отсутствие токена вызывает ошибку"""
        with patch.dict('os.environ', {}, clear=True):
            with self.assertRaises(ValueError):
                HonestSignAPI()

    @patch('requests.get')
    def test_get_balance_all_success(self, mock_get):
        """Тест успешного получения баланса"""
        mock_res = MagicMock()
        mock_res.status_code = 200
        mock_res.json.return_value = [
            {"organisationId": 123, "productGroupId": 1, "balance": 5000}
        ]
        mock_get.return_value = mock_res

        balances = self.api.get_balance_all()
        self.assertEqual(len(balances), 1)
        self.assertEqual(balances[0]['balance'], 5000)

    @patch('pyperclip.paste')
    def test_get_codes_from_clipboard(self, mock_paste):
        """Тест получения кодов из буфера"""
        mock_paste.return_value = "CODE1\nCODE2"
        codes = self.api.get_codes_from_clipboard()
        self.assertEqual(len(codes), 2)
        self.assertEqual(codes[0], "CODE1")

if __name__ == '__main__':
    unittest.main()