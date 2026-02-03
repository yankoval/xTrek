import unittest
import json
import os
import sys
from unittest.mock import patch, mock_open

# Добавляем путь для импорта основного модуля
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import gs1_processor

class TestGS1Processor(unittest.TestCase):

    def test_prefix_extraction(self):
        """Тест логики 7-ми и 9-ти значных префиксов"""
        self.assertEqual(gs1_processor.get_gs1_prefix("4610117633945"), "4610117")
        self.assertEqual(gs1_processor.get_gs1_prefix("4670017921234"), "467001792")
        self.assertEqual(gs1_processor.get_gs1_prefix("4607051794405.0"), "4607051")

    def test_inn_from_filename(self):
        """Тест извлечения ИНН из имени"""
        self.assertEqual(gs1_processor.extract_inn_from_filename("owned_gtins_7733154124.xlsx"), "7733154124")

    @patch("gs1_processor.os.path.exists", return_value=True)
    def test_get_inn_by_gtin(self, mock_exists):
        """Тест функции поиска ИНН в базе"""
        fake_db = json.dumps({
            "4610117": "7733154124",
            "467001792": "7733154124",
            "4751042": "9718180660"
        })

        # Используем mock_open правильно импортированным
        with patch("gs1_processor.open", mock_open(read_data=fake_db)):
            # Проверка существующего 7-значного
            self.assertEqual(gs1_processor.get_inn_by_gtin("4610117000000"), "7733154124")
            # Проверка нашего особого 9-значного
            self.assertEqual(gs1_processor.get_inn_by_gtin("4670017929999"), "7733154124")
            # Проверка отсутствующего
            self.assertIsNone(gs1_processor.get_inn_by_gtin("4600000000000"))

    def test_csv_parsing_logic(self):
        """Проверка парсинга CSV структуры"""
        csv_content = "gtin,name\n4610117633945,Product1\n4670017921234,Product2"
        prefix_map = {}
        with patch("gs1_processor.open", mock_open(read_data=csv_content)):
            gs1_processor.parse_csv("fake.csv", "12345", prefix_map)

        self.assertEqual(prefix_map.get("4610117"), "12345")
        self.assertEqual(prefix_map.get("467001792"), "12345")

if __name__ == "__main__":
    unittest.main()