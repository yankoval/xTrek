import argparse
import sys
import json
import re


def json_serialize_string(s):
    """
    Сериализует строку по правилам JSON
    """
    # Используем json.dumps для правильной сериализации
    # Убираем обрамляющие кавычки, так как нам нужна только escaped строка
    serialized = json.dumps(s)
    return serialized[1:-1]  # Убираем обрамляющие кавычки


def process_gui_files(file1_path, file2_path, output_path, separator='93',
                      encoding1=None, encoding2=None, output_encoding='utf-8',
                      verbose=False, unicode_escape=False, json_serialize=False):
    """
    Обрабатывает два файла: удаляет из второго файла строки, содержащие подстроки из первого файла

    Args:
        file1_path: путь к первому файлу (образцу)
        file2_path: путь ко второму файлу (который обрабатываем)
        output_path: путь для результирующего файла
        separator: символ для разделения (по умолчанию '93')
        encoding1: кодировка первого файла
        encoding2: кодировка второго файла
        output_encoding: кодировка выходного файла
        verbose: вывод подробной информации
        unicode_escape: использовать Unicode-escape кодирование для выходного файла
        json_serialize: сериализовать подстроки для поиска по правилам JSON
    """
    try:
        # Автоопределение кодировки если не указана
        if encoding1 is None or encoding2 is None:
            try:
                import chardet

                def detect_encoding(file_path):
                    with open(file_path, 'rb') as f:
                        raw_data = f.read()
                        result = chardet.detect(raw_data)
                        detected_encoding = result['encoding'] or 'utf-8'
                        if verbose:
                            print(f"Определена кодировка {file_path}: {detected_encoding}")
                        return detected_encoding

                if encoding1 is None:
                    encoding1 = detect_encoding(file1_path)
                if encoding2 is None:
                    encoding2 = detect_encoding(file2_path)

            except ImportError:
                if verbose:
                    print("Библиотека chardet не установлена, используется utf-8")
                encoding1 = encoding1 or 'utf-8'
                encoding2 = encoding2 or 'utf-8'
        else:
            encoding1 = encoding1 or 'utf-8'
            encoding2 = encoding2 or 'utf-8'

        if unicode_escape:
            output_encoding = 'unicode-escape'

        if verbose:
            print(f"Кодировка первого файла: {encoding1}")
            print(f"Кодировка второго файла: {encoding2}")
            print(f"Кодировка выходного файла: {output_encoding}")
            print(f"Символ разделения: '{separator}'")
            if unicode_escape:
                print("Режим: Unicode-escape кодирование")
            if json_serialize:
                print("Режим: JSON-сериализация паттернов")

        # Читаем оба файла полностью
        with open(file1_path, 'r', encoding=encoding1) as file1:
            lines1 = [line.rstrip('\n\r') for line in file1.readlines()]

        with open(file2_path, 'r', encoding=encoding2) as file2:
            lines2 = [line.rstrip('\n\r') for line in file2.readlines()]

        # Создаем множество подстрок из первого файла для поиска
        search_patterns = set()

        # Извлекаем подстроки из первого файла
        for i, line1 in enumerate(lines1):
            # Ищем позицию символа разделения в первой строке
            pos = line1.find(separator)

            if pos != -1:
                # Берем часть до символа разделения включительно
                pattern = line1[:pos + len(separator)]
            else:
                # Если символа разделения нет, используем ВСЮ строку первого файла
                pattern = line1

            # Сериализуем по правилам JSON если включен режим
            if json_serialize:
                pattern = json_serialize_string(pattern)
                if verbose:
                    print(f"Сериализован паттерн из строки {i + 1}: '{line1}' -> '{pattern}'")
            else:
                if verbose:
                    print(f"Добавлен паттерн из строки {i + 1}: '{pattern}'")

            search_patterns.add(pattern)

        if verbose:
            print(f"Всего паттернов для поиска: {len(search_patterns)}")
            if json_serialize:
                print("Паттерны сериализованы по правилам JSON")

        # Создаем множество для быстрого поиска строк, которые нужно удалить
        lines_to_remove = set()

        # Ищем во втором файле строки, содержащие любую из подстрок
        for j, line2 in enumerate(lines2):
            for pattern in search_patterns:
                if pattern in line2:
                    lines_to_remove.add(j)
                    if verbose:
                        print(f"Найдено совпадение: паттерн '{pattern}' -> строка {j + 1} второго файла: '{line2}'")
                    break  # Прерываем поиск, если нашли хотя бы одно совпадение

        # Создаем результирующий файл без удаленных строк
        if output_encoding == 'unicode-escape':
            # Специальная обработка для unicode-escape с правильным форматом
            with open(output_path, 'w', encoding='utf-8') as output_file:
                removed_count = 0
                written_count = 0

                for j, line2 in enumerate(lines2):
                    if j in lines_to_remove:
                        removed_count += 1
                        if verbose:
                            print(f"Удалена строка {j + 1}: '{line2}'")
                    else:
                        # Кодируем строку в unicode-escape с правильным форматом (4-значные коды)
                        escaped_chars = []
                        for char in line2:
                            code_point = ord(char)
                            if code_point < 128:
                                # ASCII символы оставляем как есть
                                escaped_chars.append(char)
                            else:
                                # Unicode символы преобразуем в формат \uXXXX
                                escaped_chars.append(f"\\u{code_point:04X}")

                        escaped_line = ''.join(escaped_chars)
                        output_file.write(escaped_line + '\n')
                        written_count += 1
                        if verbose:
                            print(f"Сохранена строка {j + 1}: '{line2}' -> '{escaped_line}'")
        else:
            # Обычная запись с указанной кодировкой
            with open(output_path, 'w', encoding=output_encoding) as output_file:
                removed_count = 0
                written_count = 0

                for j, line2 in enumerate(lines2):
                    if j in lines_to_remove:
                        removed_count += 1
                        if verbose:
                            print(f"Удалена строка {j + 1}: '{line2}'")
                    else:
                        output_file.write(line2 + '\n')
                        written_count += 1
                        if verbose:
                            print(f"Сохранена строка {j + 1}: '{line2}'")

        # Вывод статистики
        print(f"Обработка завершена.")
        print(f"Всего строк во втором файле: {len(lines2)}")
        print(f"Удалено строк: {removed_count}")
        print(f"Сохранено строк: {written_count}")
        print(f"Паттернов для поиска: {len(search_patterns)}")
        if unicode_escape:
            print(f"Режим кодирования: Unicode-escape (формат \\uXXXX)")
        if json_serialize:
            print(f"Режим: JSON-сериализация паттернов")
        print(f"Кодировка выходного файла: {output_encoding}")
        print(f"Результат сохранен в: {output_path}")

    except FileNotFoundError as e:
        print(f"Ошибка: Файл не найден - {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Произошла ошибка: {e}", file=sys.stderr)
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        description='Удаление из второго файла строк, содержащих подстроки из первого файла',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Примеры использования:
  # Базовое использование
  python gui_processor.py file1.txt file2.txt output.txt

  # С JSON-сериализацией паттернов
  python gui_processor.py file1.txt file2.txt output.txt --json-serialize

  # С Unicode-escape и JSON-сериализацией
  python gui_processor.py file1.txt file2.txt output.txt --unicode-escape --json-serialize

  # С указанием символа разделения и JSON-сериализацией
  python gui_processor.py file1.txt file2.txt output.txt --separator "]" --json-serialize

  # С указанием кодировок и подробным выводом
  python gui_processor.py file1.txt file2.txt output.txt --encoding1 windows-1251 --encoding2 utf-8 --verbose --json-serialize

  # Показать справку
  python gui_processor.py --help
        '''
    )

    # Обязательные аргументы
    parser.add_argument('file1', help='Путь к первому файлу (образцу с паттернами)')
    parser.add_argument('file2', help='Путь ко второму файлу (который обрабатываем)')
    parser.add_argument('output', help='Путь для результирующего файла')

    # Опциональные аргументы
    parser.add_argument('-s', '--separator', default='93',
                        help='Символ для разделения (по умолчанию: 93)')
    parser.add_argument('--encoding1',
                        help='Кодировка первого файла (автоопределение если не указана)')
    parser.add_argument('--encoding2',
                        help='Кодировка второго файла (автоопределение если не указана)')
    parser.add_argument('--output-encoding', default='utf-8',
                        help='Кодировка выходного файла (по умолчанию: utf-8)')
    parser.add_argument('-u', '--unicode-escape', action='store_true',
                        help='Использовать Unicode-escape кодирование для выходного файла (формат \\uXXXX)')
    parser.add_argument('-j', '--json-serialize', action='store_true',
                        help='Сериализовать подстроки для поиска по правилам JSON')
    parser.add_argument('-v', '--verbose', action='store_true',
                        help='Вывод подробной информации о процессе')
    parser.add_argument('--version', action='version', version='GUI File Processor 4.0')

    # Парсим аргументы
    args = parser.parse_args()

    # Проверка конфликтующих опций
    if args.unicode_escape and args.output_encoding != 'utf-8':
        print("Предупреждение: опция --unicode-escape имеет приоритет над --output-encoding", file=sys.stderr)

    if args.verbose:
        print("Запуск обработки файлов...")
        print(f"Файл с паттернами: {args.file1}")
        print(f"Обрабатываемый файл: {args.file2}")
        print(f"Выходной файл: {args.output}")
        if args.json_serialize:
            print("Режим: JSON-сериализация паттернов")

    # Запускаем обработку
    process_gui_files(
        file1_path=args.file1,
        file2_path=args.file2,
        output_path=args.output,
        separator=args.separator,
        encoding1=args.encoding1,
        encoding2=args.encoding2,
        output_encoding=args.output_encoding,
        verbose=args.verbose,
        unicode_escape=args.unicode_escape,
        json_serialize=args.json_serialize
    )


if __name__ == "__main__":
    main()