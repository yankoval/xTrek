import csv

import csv
from io import StringIO

def parse_quoted_line_with_semicolon(line):
    """
    Парсит одну строку CSV, заключенную в кавычки и заканчивающуюся точкой с запятой.
    
    Параметры:
    -----------
    line : str
        Строка для парсинга, например:
        "field1\tfield2\tfield3";' или '"0104657818650253215!""C8t93qTEa\t04657818650253\t""...
    
    Возвращает:
    -----------
    list
        Список полей, если парсинг успешен
    None
        Если произошла ошибка
    """
    
    # Проверяем, что строка не пустая
    if not line or not isinstance(line, str):
        print("Ошибка: пустая или некорректная строка")
        return None
    
    # 1. Создаем виртуальный файл из строки
    # Важно: передаем строку как есть, csv модуль сам обработает кавычки
    csv_file = StringIO(line)
    
    # 2. Создаем reader с правильными параметрами
    # Указываем, что разделитель полей - табуляция
    reader = csv.reader(
        csv_file,
        delimiter='\t',        # разделитель полей внутри строки
        quotechar='"',         # символ кавычки
        quoting=csv.QUOTE_ALL, # все поля обрамлены кавычками
        doublequote=True,      # две кавычки подряд означают одну кавычку в данных
        skipinitialspace=False # не пропускать пробелы после разделителя
    )
    
    try:
        # 3. Читаем строку
        # reader вернет список, даже если строка всего одна
        result = next(reader)
        
        # 4. Проверяем, что последнее поле не содержит точку с запятой
        # (она может быть частью данных, если не экранирована)
        if result and result[-1].endswith(';'):
            # Если точка с запятой в конце последнего поля - удаляем ее
            result[-1] = result[-1].rstrip(';')
            
            # Если после удаления остались кавычки - убираем их
            if result[-1].endswith('"') and result[-1].startswith('"'):
                result[-1] = result[-1][1:-1]
        
        return result
        
    except csv.Error as e:
        print(f"Ошибка парсинга CSV: {e}")
        print(f"Проблемная строка (первые 200 символов): {repr(line[:200])}")
        return None
        
    except StopIteration:
        print("Ошибка: не удалось прочитать строку")
        return None
        
    except Exception as e:
        print(f"Неожиданная ошибка: {e}")
        return None





data = []

file =  open(r'C:\Users\project\Downloads\тестовый для Фитокосметик SCV (1).csv', 'r', encoding='utf-8') 
# Создаем reader с правильными параметрами
reader1 = csv.reader(
    file,
    delimiter='\t',
    quotechar='"',
    quoting=csv.QUOTE_ALL,
    escapechar='\\'  # может потребоваться
)
codes = []
for row in reader1:
    for column in row:
        filds = parse_quoted_line_with_semicolon(column)
        # !!!!!!!!!!!!!!!  TODO: проверь нужен ли FNC1 в начале
        codes.append('\x1d'+filds[0])
        for fild in filds:
            print(fild)
