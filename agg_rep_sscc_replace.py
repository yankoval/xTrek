import json
import csv
from datetime import datetime
# Импортируем функцию согласно вашему требованию
from SSCC_Utils import get_sscc_from_service

# Все параметры вызова процедуры и пути к файлам
SETTINGS = {
    "sscc_url": "https://functions.yandexcloud.net/d4et2pvmtgp0oo5pk0bh",
    "sscc_prefix": "460705179",
    "sscc_extension": "0",
    "input_json": r"C:\Users\ivan\Downloads\81a8c228-932d-43f3-b4b8-c5262924dc52.json",
    "output_json": r"C:\Users\ivan\Downloads\81a8c228-932d-43f3-b4b8-c5262924dc52_new.json",
    "mapping_csv": "replacement_table.csv"
}


def replace_all_box_codes(config):
    """
    Процедура полной замены всех кодов коробов (boxNumber) в отчете.
    """
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Начало полной замены кодов...")

    # 1. Чтение JSON отчета
    try:
        with open(config["input_json"], 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        print(f"Ошибка при чтении JSON: {e}")
        return

    # Проверка наличия данных
    if "readyBox" not in data or not data["readyBox"]:
        print("В отчете нет данных в ключе 'readyBox'.")
        return

    ready_boxes = data["readyBox"]
    # 2. Считаем общее количество кодов для замены
    total_count = len(ready_boxes)
    print(f"Всего коробов в отчете: {total_count}. Запрашиваем новые коды...")

    # 3. Обращение к облачной функции через xTrek для получения новых кодов
    # Передаем параметры из настроек: url, префикс, количество, расширение
    new_sscc_list = get_sscc_from_service(
        function_url=config["sscc_url"],
        prefix=config["sscc_prefix"],
        count=total_count,
        extension=config["sscc_extension"]
    )

    if not new_sscc_list or len(new_sscc_list) < total_count:
        print(f"Ошибка: Сервис вернул {len(new_sscc_list) if new_sscc_list else 0} кодов вместо {total_count}.")
        return

    # 4. Процесс замены и формирование таблицы соответствия
    replacement_mapping = []

    for i in range(total_count):
        old_code = ready_boxes[i].get("boxNumber")
        new_code = new_sscc_list[i]

        # Сохраняем информацию для пользователя
        replacement_mapping.append({
            "Box_Number_in_Report": i,
            "Old_SSCC": old_code,
            "New_SSCC": new_code,
            "Box_Time": ready_boxes[i].get("boxTime", "")
        })

        # 5. Прямая замена кода в структуре JSON
        ready_boxes[i]["boxNumber"] = new_code

    # 6. Сохранение таблицы замен (CSV)
    try:
        with open(config["mapping_csv"], 'w', newline='', encoding='utf-8-sig') as f:
            # Используем ; как разделитель для автоматического открытия в Excel
            writer = csv.DictWriter(f, fieldnames=["Box_Number_in_Report", "Old_SSCC", "New_SSCC", "Box_Time"],
                                    delimiter=';')
            writer.writeheader()
            writer.writerows(replacement_mapping)
        print(f"Таблица замен сохранена: {config['mapping_csv']}")
    except Exception as e:
        print(f"Не удалось записать CSV: {e}")

    # 7. Сохранение обновленного JSON отчета
    try:
        with open(config["output_json"], 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        print(f"Новый отчет успешно сохранен: {config['output_json']}")
    except Exception as e:
        print(f"Ошибка при сохранении JSON: {e}")

    print("=== Процедура завершена. Все коды заменены на новые. ===")


if __name__ == "__main__":
    replace_all_box_codes(SETTINGS)