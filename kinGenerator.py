import json
from datetime import datetime, timedelta
import uuid
import random
# from google.colab import files
import os

class KinReportGenerator:
    def __init__(self):
        self.uploaded_files = {}

    def extract_short_code(self, full_code):
        """Извлечение короткого кода из полного кода маркировки"""
        try:
            if not full_code or not isinstance(full_code, str):
                return None

            # Разделяем код по разделителю GS (\u001D)
            parts = full_code.split('\u001D')
            if not parts:
                return None

            # Первая часть содержит GTIN и короткий код
            main_part = parts[0]

            # Ищем позицию идентификатора 21 (код товара)
            pos_21 = main_part.find('21')
            if pos_21 == -1:
                return None

            # Извлекаем 6 символов после '21'
            short_code_start = pos_21 + 2
            short_code_end = short_code_start + 6

            if short_code_end <= len(main_part):
                return main_part[short_code_start:short_code_end]
            else:
                return None

        except Exception as e:
            print(f"Ошибка при извлечении короткого кода: {e}")
            return None

    def load_files(self, file_names):
        """Загрузка файлов по списку имен"""
        print("Загрузка файлов...")

        for file_name in file_names:
            if os.path.exists(file_name):
                try:
                    with open(file_name, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    self.uploaded_files[file_name] = data
                    print(f"✓ Загружен файл: {file_name}")
                except Exception as e:
                    print(f"✗ Ошибка загрузки {file_name}: {e}")
            else:
                print(f"✗ Файл не найден: {file_name}")

        return len(self.uploaded_files) > 0

    def calculate_max_kits(self, kigu_codes, all_kit_codes):
        """Подсчет максимального количества наборов"""
        if not kigu_codes or not all_kit_codes:
            return 0

        max_from_kigu = len(kigu_codes)
        max_from_kits = min(len(codes) for codes in all_kit_codes) if all_kit_codes else 0

        return min(max_from_kigu, max_from_kits)

    def generate_kin_report(self, file_names, num_kits=None):
        """
        Основная процедура для генерации КИН отчета

        Args:
            file_names: список имен файлов для обработки
            num_kits: количество наборов для генерации (None - максимальное)

        Returns:
            str: имя созданного файла отчета или None при ошибке
        """

        # Загружаем файлы
        if not self.load_files(file_names):
            print("❌ Не удалось загрузить файлы")
            return None

        # Ищем основной файл с Hierarchy
        main_data = None
        kigu_gtin = None

        for filename, data in self.uploaded_files.items():
            if 'Hierarchy' in data:
                main_data = data
                # Извлекаем GTIN Kigu
                for level in data.get('Hierarchy', []):
                    if level['LevelType'] == 'Kigu':
                        for pack in level['Packs']:
                            kigu_gtin = pack['GTIN']
                break

        if not main_data or not kigu_gtin:
            print("❌ Не найден основной файл с описанием набора")
            return None

        print(f"Найден Kigu GTIN: {kigu_gtin}")

        # Предварительный расчет максимального количества наборов
        print("🔍 Подсчет доступных наборов...")

        # Находим файлы для расчета
        kigu_codes = []
        all_kit_codes = []

        # Ищем Kigu файл
        for filename, data in self.uploaded_files.items():
            if 'codes' in data and data.get('codes') and kigu_gtin in data['codes'][0]:
                kigu_codes = data['codes']
                break

        # Ищем Kit файлы
        kit_gtins = []
        for level in main_data.get('Hierarchy', []):
            if level['LevelType'] == 'Kit':
                for pack in level['Packs']:
                    kit_gtins.append(pack['GTIN'])

        for kit_gtin in kit_gtins:
            for filename, data in self.uploaded_files.items():
                if 'codes' in data and data.get('codes') and kit_gtin in data['codes'][0]:
                    all_kit_codes.append(data['codes'])
                    break

        max_kits = self.calculate_max_kits(kigu_codes, all_kit_codes)

        if max_kits == 0:
            print("❌ Недостаточно кодов для создания наборов")
            return None

        # Определяем количество наборов для генерации
        if num_kits is None:
            num_kits = max_kits
        elif num_kits > max_kits:
            print(f"❌ Нельзя сгенерировать больше {max_kits} наборов")
            return None

        print(f"Генерация {num_kits} наборов...")

        # Создаем отчет
        kin_report = self._create_report_data(kigu_gtin, num_kits)

        if not kin_report:
            return None

        # Сохраняем отчет
        output_filename = f"{kigu_gtin}_kin_report.json"

        try:
            with open(output_filename, 'w', encoding='utf-8') as f:
                json.dump(kin_report, f, ensure_ascii=False, indent=2)

            print(f"✅ КИН отчет успешно создан: {output_filename}")
            return output_filename

        except Exception as e:
            print(f"❌ Ошибка при сохранении отчета: {e}")
            return None

    def _create_report_data(self, kigu_gtin, num_kits):
        """Создание данных отчета"""

        # Поиск файла для Kigu
        kigu_data = None

        for filename, data in self.uploaded_files.items():
            if 'codes' in data and data.get('codes'):
                if data['codes'] and kigu_gtin in data['codes'][0]:
                    kigu_data = data
                    break

        if not kigu_data:
            print(f"❌ Не найден файл с кодами для Kigu GTIN: {kigu_gtin}")
            return None

        # Извлечение GTIN для Kit из основного файла
        kit_gtins = []
        for filename, data in self.uploaded_files.items():
            if 'Hierarchy' in data:
                for level in data.get('Hierarchy', []):
                    if level['LevelType'] == 'Kit':
                        for pack in level['Packs']:
                            kit_gtins.append(pack['GTIN'])

        # Поиск файлов для Kit
        kit_data_list = []

        for kit_gtin in kit_gtins:
            for filename, data in self.uploaded_files.items():
                if 'codes' in data and data.get('codes'):
                    if data['codes'] and kit_gtin in data['codes'][0]:
                        kit_data_list.append(data)
                        break

        if not kit_data_list:
            print("❌ Не найдены файлы для Kit продуктов")
            return None

        # Получаем коды
        kigu_codes = kigu_data.get('codes', [])
        if not kigu_codes:
            print("❌ В данных Kigu нет кодов коробок")
            return None

        # Собираем коды продуктов
        all_kit_codes = [kit_data.get('codes', []) for kit_data in kit_data_list]

        # Проверяем достаточно ли кодов
        max_available = self.calculate_max_kits(kigu_codes, all_kit_codes)
        if num_kits > max_available:
            print(f"❌ Недостаточно кодов для {num_kits} наборов")
            return None

        # Создание отчета
        start_time = datetime.now()
        ready_boxes = []

        for i in range(num_kits):
            product_numbers = []
            product_numbers_full = []

            # Берем коды из каждого Kit
            for kit_codes in all_kit_codes:
                if i < len(kit_codes):
                    full_code = kit_codes[i]
                    short_code = self.extract_short_code(full_code)

                    if short_code:
                        product_numbers.append(short_code)
                        product_numbers_full.append(full_code)

            # Номер коробки из Kigu
            box_number = kigu_codes[i]

            box = {
                "Number": i,
                "boxNumber": box_number,
                "boxAgregate": True,
                "boxTime": (start_time + timedelta(minutes=random.randint(2, 30))).isoformat(),
                "productNumbers": product_numbers,
                "productNumbersFull": product_numbers_full
            }

            ready_boxes.append(box)

        # Финальный отчет
        end_time = datetime.now()

        return {
            "id": str(uuid.uuid4()),
            "startTime": start_time.isoformat(),
            "endTime": end_time.isoformat(),
            "operators": [],
            "readyBox": ready_boxes,
            "sampleNumbers": [],
            "sampleNumbersFull": None,
            "defectiveCodes": None,
            "defectiveCodesFull": None,
            "emptyNumbers": None
        }

# Функция для вызова из внешних модулей
def generate_kin_report_from_files(file_names, num_kits=None):
    """
    Генерирует КИН отчет из указанных файлов

    Args:
        file_names: список путей к JSON файлам
        num_kits: количество наборов (None - максимальное доступное)

    Returns:
        str: путь к созданному файлу отчета или None при ошибке
    """
    generator = KinReportGenerator()
    return generator.generate_kin_report(file_names, num_kits)

# Пример использования в Google Colab
def main_colab():
    """Основная функция для использования в Colab"""
    print("=== Генератор КИН отчета ===")

    # Загрузка файлов через интерфейс Colab
    print("Загрузите файлы через интерфейс...")
    uploaded = files.upload()

    file_names = []
    for filename, content in uploaded.items():
        if filename.endswith('.json'):
            # Сохраняем файл на диск
            with open(filename, 'wb') as f:
                f.write(content)
            file_names.append(filename)
            print(f"✓ Загружен: {filename}")

    if not file_names:
        print("❌ Не загружено ни одного JSON файла")
        return

    # Генерация отчета
    report_file = generate_kin_report_from_files(file_names)

    if report_file:
        # Сохраняем результат
        try:
            with open(report_file, 'w', encoding='utf-8') as f:
                data = json.dumps(f)
            print(f"✓ Сохранен файл: {report_file}")
        except Exception as e:
            print(f"✗ Ошибка загрузки {report_file}: {e}")
        # Скачиваем результат
        files.download(report_file)
        print(f"✅ Отчет готов: {report_file}")

# Пример использования из другого модуля
if __name__ == "__main__":
    # Вариант 1: Использование в Colab
    # main_colab()

    # Вариант 2: Использование как модуля
    files_list = [
        "набор_04640286990808.json",          # Основной файл с Hierarchy
        "04630234043762.json", # Файл с кодами Kigu
        "04640286990808.json", # Файл с кодами Kit 1
        "04751042821837.json"  # Файл с кодами Kit 2
    ]

    # Генерация максимального количества наборов
    report_filename = generate_kin_report_from_files(files_list)

    # Или указать конкретное количество
    # report_filename = generate_kin_report_from_files(files_list, 50)