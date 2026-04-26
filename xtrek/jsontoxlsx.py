import json
import re
import sys
import xlsxwriter
from pathlib import Path

def parse_dm_code(full_code):
    """
    Извлекает из кода КИ (GTIN + Серийный номер), отсекая криптохвост.
    """
    if not isinstance(full_code, str):
        return ""
    
    # Регулярное выражение: ищем 01 + 14 цифр и 21 + всё до разделителя GS (\u001d)
    match = re.search(r"01(\d{14})21([^\u001d]+)", full_code)
    
    if match:
        gtin = match.group(1)
        serial = match.group(2)
        return f"01{gtin}21{serial}"
    
    # Если формат не стандартный, просто убираем символ GS
    return full_code.replace('\u001d', '').strip()

def convert_json_to_xlsx(input_json, output_xlsx=None):
    """
    Основная процедура конвертации.
    :param input_json: путь к входному файлу (str или Path)
    :param output_xlsx: путь к выходному файлу (если None, меняется расширение входного)
    """
    input_path = Path(input_json)
    if output_xlsx:
        output_path = Path(output_xlsx)
    else:
        output_path = input_path.with_suffix('.xlsx')

    if not input_path.exists():
        raise FileNotFoundError(f"Файл не найден: {input_path}")

    # Читаем данные
    with open(input_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    raw_codes = data.get("productNumbersFull", [])
    
    # Создаем Excel
    workbook = xlsxwriter.Workbook(str(output_path))
    worksheet = workbook.add_worksheet()

    # Заголовок
    worksheet.write(0, 0, "Номер КИ")

    # Данные
    for index, code in enumerate(raw_codes, start=1):
        clean_code = parse_dm_code(code)
        worksheet.write(index, 0, clean_code)

    workbook.close()
    return str(output_path)

if __name__ == "__main__":
    # Логика для работы через командную строку
    if len(sys.argv) < 2:
        print("Использование: python json_converter.py <file.json> [<file.xlsx>]")
    else:
        in_file = sys.argv[1]
        out_file = sys.argv[2] if len(sys.argv) > 2 else None
        try:
            result = convert_json_to_xlsx(in_file, out_file)
            print(f"Успешно сконвертировано в: {result}")
        except Exception as e:
            print(f"Ошибка: {e}")