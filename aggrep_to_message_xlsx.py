import json
import openpyxl
from datetime import datetime

# Настройки и параметры из ТЗ
CONFIG = {
    "input_json": r"C:\Users\ivan\Downloads\81a8c228-932d-43f3-b4b8-c5262924dc52.json",
    "output_xlsx": r"C:\Users\ivan\Downloads\81a8c228-932d-43f3-b4b8-c5262924dc52.xlsx",
    "params": {
        "tn_ved": "3304990000",
        "prod_date": "01.04.2026",
        "doc_type": "CONFORMITY_DECLARATION",
        "doc_number": "ЕАЭС N RU Д-RU.РА07.В.95613/24",
        "doc_date": "03.09.2024"
    }
}


def transform_mark_code(full_code):
    """
    Отбрасывает крипточасть: всё, что идет после символа GS (\u001D).
    """
    if not full_code:
        return ""
    # Разделяем по символу GS и берем первую часть
    return full_code.split('\u001D')[0]


def export_aggregation_to_xlsx(config):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Запуск экспорта в XLSX...")

    # 1. Чтение JSON отчета
    try:
        with open(config["input_json"], 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        print(f"Ошибка чтения JSON: {e}")
        return

    ready_boxes = data.get("readyBox", [])
    if not ready_boxes:
        print("Данные для агрегации не найдены.")
        return

    # 2. Создание Excel файла
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Коды маркировки"

    # 3. Запись заголовков (согласно образцу)
    headers = [
        "Код маркировки",
        "Код ТН ВЭД",
        "Дата производства",
        "Вид разрешительного документа",
        "Номер разрешительного документа",
        "Дата разрешительного документа"
    ]
    ws.append(headers)

    # 4. Сбор кодов и заполнение строк
    extracted_count = 0
    p = config["params"]

    for box in ready_boxes:
        product_codes = box.get("productNumbersFull", [])

        for full_code in product_codes:
            # Чистим код от криптохвоста
            clean_code = transform_mark_code(full_code)

            # Формируем строку данных
            row = [
                clean_code,
                p["tn_ved"],
                p["prod_date"],
                p["doc_type"],
                p["doc_number"],
                p["doc_date"]
            ]
            ws.append(row)
            extracted_count += 1

    # Настройка ширины колонок для удобства (опционально)
    for column_cells in ws.columns:
        length = max(len(str(cell.value)) for cell in column_cells)
        ws.column_dimensions[column_cells[0].column_letter].width = length + 2

    # 5. Сохранение результата
    try:
        wb.save(config["output_xlsx"])
        print(f"Экспорт завершен успешно!")
        print(f"Извлечено кодов: {extracted_count}")
        print(f"Файл сохранен: {config['output_xlsx']}")
    except Exception as e:
        print(f"Ошибка при сохранении XLSX: {e}")


if __name__ == "__main__":
    export_aggregation_to_xlsx(CONFIG)