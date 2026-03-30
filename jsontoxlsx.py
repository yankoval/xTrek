import json
import re
import sys
import xlsxwriter
from pathlib import Path

def parse_dm_code(full_code):
    """
    Извлекает из кода КИ (GTIN + Серийный номер), отсекая криптохвост и спецсимволы.
    """
    if not isinstance(full_code, str):
        return ""
    
    # Регулярное выражение для поиска GTIN (01 + 14 цифр) и Serial (21 + хвост до GS)
    # \u001d - символ разделителя GS
    match = re.search(r"01(\d{14})21([^\u001d]+)", full_code)
    
    if match:
        gtin = match.group(1)
        serial = match.group(2)
        return f"01{gtin}21{serial}"
    
    # Если формат не распознан, просто очищаем от GS и пробелов
    return full_code.replace('\u001d', '').strip()

def main():
    # Проверка аргументов командной строки
    if len(sys.argv) < 2:
        print("Использование: python script.py <входной_файл.json> [<выходной_файл.xlsx>]")
        return

    input_path = Path(sys.argv[1])
    
    # Если выходной файл не указан, меняем расширение .json на .xlsx
    if len(sys.argv) > 2:
        output_path = Path(sys.argv[2])
    else:
        output_path = input_path.with_suffix('.xlsx')

    if not input_path.exists():
        print(f"Ошибка: Файл {input_path} не найден.")
        return

    try:
        # Чтение JSON
        with open(input_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Получаем список кодов
        raw_codes = data.get("productNumbersFull", [])
        
        # Создаем Excel файл с помощью xlsxwriter
        workbook = xlsxwriter.Workbook(str(output_path))
        worksheet = workbook.add_worksheet()

        # Записываем заголовок
        worksheet.write(0, 0, "Номер КИ")

        # Записываем обработанные коды
        for index, code in enumerate(raw_codes, start=1):
            clean_code = parse_dm_code(code)
            worksheet.write(index, 0, clean_code)

        workbook.close()
        
        print(f"Готово!")
        print(f"Входной файл: {input_path}")
        print(f"Создан файл:  {output_path}")
        print(f"Обработано строк: {len(raw_codes)}")

    except Exception as e:
        print(f"Произошла ошибка: {e}")

if __name__ == "__main__":
    main()