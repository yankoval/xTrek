"""
СКРИПТ: Трансформатор отчета агрегации (Оборудование -> Честный Знак)

ОПИСАНИЕ ФАЙЛОВ:
1. ВХОДНОЙ ФАЙЛ (Report): JSON от оборудования.
   - Ищет массив 'readyBox'.
   - Извлекает 'boxNumber' (SSCC) и 'productNumbersFull' (КИЗы с криптохвостом).

2. ВЫХОДНОЙ ФАЙЛ (Document): JSON стандарта ГИС МТ.
   - Поле 'participantId': ИНН участника (передается через аргумент --inn).
   - Поле 'sntins': КИЗы, очищенные от символа \u001d и всего, что за ним следует.
   - Поле 'unitSerialNumber': SSCC код упаковки.

ЗАПУСК:
python converter.py <входной_файл> <выходной_файл> --inn <ваш_инн>
Пример: python converter.py report.json upload.json --inn 7733154124
"""

import json
import argparse
import sys
import os

def transform_aggregation(input_path, output_path, inn_value):
    # 1. Проверка физического наличия файла
    if not os.path.exists(input_path):
        print(f"ОШИБКА: Файл '{input_path}' не найден.")
        sys.exit(1)

    try:
        print(f"--- Старт обработки (Вход: {input_path}) ---")
        
        with open(input_path, 'r', encoding='utf-8') as f:
            source_data = json.load(f)

        aggregation_units = []
        boxes = source_data.get('readyBox', [])
        
        # 2. Основной цикл трансформации данных
        for box in boxes:
            # Очистка кодов маркировки от криптохвостов (все после разделителя GS1)
            # Для агрегации в ЧЗ передается только GTIN + Серийный номер
            clean_codes = [
                code.split('\u001d')[0] for code in box.get('productNumbersFull', [])
            ]

            unit = {
                "sntins": clean_codes,
                "aggregationType": "AGGREGATION",
                "unitSerialNumber": box.get('boxNumber')
            }
            aggregation_units.append(unit)

        # 3. Сборка итогового документа (Формат Честного Знака)
        # В параметрах скрипта мы запрашиваем 'inn', но в JSON пишем 'participantId'
        result_doc = {
            "aggregationUnits": aggregation_units,
            "participantId": inn_value 
        }

        # 4. Сохранение в файл
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(result_doc, f, ensure_ascii=False, indent=4)
        
        print(f"ГОТОВО: Файл '{output_path}' сформирован успешно.")
        print(f"Статистика: {len(aggregation_units)} коробок, ИНН: {inn_value}")

    except json.JSONDecodeError:
        print("ОШИБКА: Входной файл содержит некорректный JSON.")
    except Exception as e:
        print(f"КРИТИЧЕСКАЯ ОШИБКА: {e}")
        sys.exit(1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Конвертация отчетов в формат Честного Знака.")
    
    # Обязательные аргументы
    parser.add_argument("input", help="Путь к файлу от оборудования")
    parser.add_argument("output", help="Путь к новому файлу агрегации")
    
    # Опциональный аргумент для ИНН (внутри кода мапится на participantId)
    parser.add_argument(
        "--inn", 
        dest="inn",
        default="7733154124", 
        help="ИНН участника (будет записан в поле participantId)"
    )

    args = parser.parse_args()

    # Передаем значение из аргумента --inn в логику формирования participantId
    transform_aggregation(args.input, args.output, args.inn)