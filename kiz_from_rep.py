import sys
import json
import uuid

def read_json_file(file_path):
    """Читает данные из JSON файла."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"Ошибка при чтении файла {file_path}: {e}")
        sys.exit(1)

def extract_and_flatten(data):
    """Извлекает productNumbersFull и делает список плоским."""
    ready_box = data.get('readyBox', [])
    # Извлекаем вложенные списки
    nested_list = [b.get('productNumbersFull') for b in ready_box if b.get('productNumbersFull')]
    # Выпрямляем структуру
    return [item for sublist in nested_list for item in sublist]

def wrap_to_structure(flat_list, oms_id="3b1ed9ae-a5d9-4458-9f02-596781bd1e41"):
    """
    Оборачивает плоский список в итоговый формат словаря.
    blockId генерируется автоматически (UUID).
    """
    return {
        "codes": flat_list,
        "blockId": str(uuid.uuid4()),  # Генерирует уникальный ID
        "omsId": oms_id
    }

def save_to_json(data, file_path):
    """Сохраняет данные в JSON файл."""
    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        print(f"Успех! Файл сохранен: {file_path}")
    except Exception as e:
        print(f"Ошибка при сохранении: {e}")
        sys.exit(1)

def main():
    # Проверка аргументов командной строки
    if len(sys.argv) < 3:
        print("Использование: python script.py <input.json> <output.json>")
        return

    input_path = sys.argv[1]
    output_path = sys.argv[2]

    # 1. Загрузка
    raw_data = read_json_file(input_path)

    # 2. Обработка (Извлечение + Flatten)
    flat_codes = extract_and_flatten(raw_data)

    # 3. Формирование финальной структуры
    final_structure = wrap_to_structure(flat_codes)

    # 4. Сохранение
    save_to_json(final_structure, output_path)

if __name__ == "__main__":
    main()