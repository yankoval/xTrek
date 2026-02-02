import unittest
from unittest.mock import patch, MagicMock
import os
import sys

# Добавляем текущую директорию в путь для импорта trueapi.py
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.append(current_dir)

# Импортируем из trueapi.py
try:
    from trueapi import HonestSignAPI
except ImportError as e:
    print(f"Ошибка: Не удалось найти trueapi.py в {current_dir}")
    print(f"Детали: {e}")
    sys.exit(1)

class TestHonestSignAPI(unittest.TestCase):

    def setUp(self):
        """Фиктивный токен для тестов"""
        self.token = "test_token_123"
        with patch.dict('os.environ', {'HONEST_SIGN_TOKEN': self.token}):
            self.api = HonestSignAPI()

    @patch('requests.get')
    def test_get_balance_all_success(self, mock_get):
        """Тест успешного запроса баланса"""
        mock_res = MagicMock()
        mock_res.status_code = 200
        mock_res.json.return_value = [
            {"organisationId": 777, "productGroupId": 1, "balance": 1234500}
        ]
        mock_get.return_value = mock_res

        result = self.api.get_balance_all()
        
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['balance'], 1234500)
        self.assertEqual(result[0]['organisationId'], 777)

    @patch('requests.post')
    def test_get_single_cis_info_error(self, mock_post):
        """Теst обработки ошибки API при проверке КМ"""
        mock_post.side_effect = Exception("API Error")
        
        result = self.api.get_single_cis_info("012345")
        
        self.assertIn('error', result)
        self.assertEqual(result['code'], "012345")

    @patch('pyperclip.paste')
    def test_get_codes_from_clipboard(self, mock_paste):
        """Тест очистки данных из буфера"""
        mock_paste.return_value = " CODE1 \n\n CODE2 "
        
        codes = self.api.get_codes_from_clipboard()
        
        self.assertEqual(codes, ["CODE1", "CODE2"])

if __name__ == '__main__':
    unittest.main()