import json
from datetime import datetime, timedelta
import uuid
import random
from google.colab import files
import os
import logging

# Настройка логгера
def setup_logger():
    """Настройка логгера для минимального вывода информации"""
    logger = logging.getLogger('KinReportGenerator')
    logger.setLevel(logging.INFO)
    
    # Если логгер уже имеет обработчики, не добавляем новые
    if not logger.handlers:
        # Форматировщик с минимальной информацией
        formatter = logging.Formatter('%(message)s')
        
        # Обработчик для вывода в консоль
        handler = logging.StreamHandler()
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    
    return logger

# Инициализация логгера
logger = setup_logger()

class KinReportGenerator:
    def __init__(self):
        self.uploaded_files = {}
        logger.info("Инициализация KinReportGenerator")

    def extract_short_code(self, full_code):
        """Извлечение короткого кода из полного кода маркировки"""
        try:
            if not full_code or not isinstance(full_code, str):
                logger.warning("Передан пустой или некорректный полный код маркировки")
                return None

            # Разделяем код по разделителю GS (\u001D)
            parts = full_code.split('\u001D')
            if not parts:
                logger.warning("Не удалось разделить полный код по разделителю GS")
                return None

            # Первая часть содержит GTIN и короткий код
            main_part = parts[0]

            # Ищем позицию идентификатора 21 (код товара)
            pos_21 = main_part.find('21')
            if pos_21 == -1:
                logger.warning("В полной маркировке не найден идентификатор '21'")
                return None

            # Извлекаем 6 символов после '21'
            short_code_start = pos_21 + 2
            short_code_end = short_code_start + 6

            if short_code_end <= len(main_part):
                short_code = main_part[short_code_start:short_code_end]
                return short_code
            else:
                logger.warning("Не удалось извлечь 6 символов после '21'. Код слишком короткий")
                return None

        except Exception as e:
            logger.error(f"Ошибка при извлечении короткого кода: {e}")
            return None

    def load_json_file(self, file_path):
        """
        Загрузка JSON файла с поддержкой utf-8-sig для обработки BOM
        """
        try:
            # Пытаемся загрузить с utf-8-sig (для файлов с BOM)
            with open(file_path, 'r', encoding='utf-8-sig') as f:
                return json.load(f)
        except UnicodeDecodeError:
            # Если не сработало, пробуем обычный utf-8
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                logger.error(f"Ошибка при загрузке с utf-8 файла {file_path}: {e}")
                return None
        except json.JSONDecodeError as e:
            logger.error(f"Ошибка парсинга JSON в файле {file_path}: {e}")
            return None
        except Exception as e:
            logger.error(f"Неожиданная ошибка при загрузке {file_path}: {e}")
            return None

    def load_files(self, file_names):
        """Загрузка файлов по списку имен с поддержкой utf-8-sig"""
        logger.info("Загрузка файлов...")

        successful_loads = 0
        for file_name in file_names:
            if os.path.exists(file_name):
                data = self.load_json_file(file_name)
                if data is not None:
                    self.uploaded_files[file_name] = data
                    logger.info(f"Загружен файл: {file_name}")
                    successful_loads += 1
                else:
                    logger.error(f"Ошибка загрузки {file_name}")
                    return False
            else:
                logger.error(f"Файл не найден: {file_name}")
                return False

        logger.info(f"Успешно загружено файлов: {successful_loads}/{len(file_names)}")
        return successful_loads > 0

    def validate_files_completeness(self, main_data, kigu_gtin, kit_gtins):
        """
        Проверка, что загружены все необходимые файлы с кодами
        согласно описанию набора
        """
        logger.info("Проверка комплектности файлов...")

        missing_files = []

        # Проверяем наличие файла для Kigu
        kigu_found = False
        for filename, data in self.uploaded_files.items():
            if 'codes' in data and data.get('codes'):
                if data['codes'] and kigu_gtin in data['codes'][0]:
                    kigu_found = True
                    logger.info(f"Найден файл для Kigu GTIN: {kigu_gtin}")
                    break

        if not kigu_found:
            missing_files.append(f"Kigu GTIN: {kigu_gtin}")

        # Проверяем наличие файлов для всех Kit GTIN
        found_kit_gtins = []
        for kit_gtin in kit_gtins:
            kit_found = False
            for filename, data in self.uploaded_files.items():
                if 'codes' in data and data.get('codes'):
                    if data['codes'] and kit_gtin in data['codes'][0]:
                        kit_found = True
                        found_kit_gtins.append(kit_gtin)
                        logger.info(f"Найден файл для Kit GTIN: {kit_gtin}")
                        break

            if not kit_found:
                missing_files.append(f"Kit GTIN: {kit_gtin}")

        # Выводим отчет о проверке
        if missing_files:
            logger.error("Отсутствуют файлы для следующих GTIN:")
            for missing in missing_files:
                logger.error(f"   - {missing}")

            # Показываем какие файлы вообще загружены
            logger.info("Загруженные файлы с кодами:")
            code_files = [f for f, d in self.uploaded_files.items() if 'codes' in d and d.get('codes')]
            for code_file in code_files:
                if self.uploaded_files[code_file]['codes']:
                    first_code = self.uploaded_files[code_file]['codes'][0]
                    # Извлекаем GTIN из первого кода (01{GTIN}21...)
                    gtin_start = first_code.find('01') + 2
                    gtin_end = first_code.find('21', gtin_start)
                    if gtin_start > 1 and gtin_end > gtin_start:
                        file_gtin = first_code[gtin_start:gtin_end]
                        logger.info(f"   - {code_file} -> GTIN: {file_gtin}")

            return False

        logger.info("Все необходимые файлы найдены:")
        logger.info(f"   - Kigu: {kigu_gtin}")
        for kit_gtin in kit_gtins:
            logger.info(f"   - Kit: {kit_gtin}")

        return True

    def calculate_max_kits(self, kigu_codes, all_kit_codes):
        """Подсчет максимального количества наборов"""
        if not kigu_codes or not all_kit_codes:
            logger.warning("Отсутствуют коды Kigu или Kit для подсчета наборов")
            return 0

        max_from_kigu = len(kigu_codes)
        max_from_kits = min(len(codes) for codes in all_kit_codes) if all_kit_codes else 0

        return min(max_from_kigu, max_from_kits)

    def get_file_for_gtin(self, gtin):
        """Поиск файла с кодами для указанного GTIN"""
        for filename, data in self.uploaded_files.items():
            if 'codes' in data and data.get('codes'):
                if data['codes'] and gtin in data['codes'][0]:
                    return data
        logger.warning(f"Не найден файл для GTIN {gtin}")
        return None

    def generate_kin_report(self, file_names, num_kits=None):
        """
        Основная процедура для генерации КИН отчета
        """
        logger.info("Запуск генерации КИН отчета...")

        # Загружаем файлы
        if not self.load_files(file_names):
            logger.error("Не удалось загрузить файлы")
            return None

        # Ищем основной файл с Hierarchy
        main_data = None
        kigu_gtin = None
        kit_gtins = []

        for filename, data in self.uploaded_files.items():
            if 'Hierarchy' in data:
                main_data = data
                logger.info(f"Найден основной файл с Hierarchy: {filename}")
                # Извлекаем GTIN Kigu и Kit
                for level in data.get('Hierarchy', []):
                    if level['LevelType'] == 'Kigu':
                        for pack in level['Packs']:
                            kigu_gtin = pack['GTIN']
                    elif level['LevelType'] == 'Kit':
                        for pack in level['Packs']:
                            kit_gtin = pack['GTIN']
                            kit_gtins.append(kit_gtin)
                break

        if not main_data:
            logger.error("Не найден основной файл с описанием набора")
            return None

        if not kigu_gtin:
            logger.error("В основном файле не найден GTIN для Kigu")
            return None

        if not kit_gtins:
            logger.error("В основном файле не найдены GTIN для Kit")
            return None

        logger.info("Описание набора:")
        logger.info(f"   - Kigu GTIN: {kigu_gtin}")
        logger.info(f"   - Kit GTIN: {', '.join(kit_gtins)}")

        # Проверяем наличие всех необходимых файлов
        if not self.validate_files_completeness(main_data, kigu_gtin, kit_gtins):
            logger.error("Не все необходимые файлы с кодами загружены")
            return None

        # Получаем данные из файлов
        kigu_data = self.get_file_for_gtin(kigu_gtin)
        if not kigu_data:
            logger.error(f"Не удалось получить данные для Kigu GTIN: {kigu_gtin}")
            return None

        kit_data_list = []
        for kit_gtin in kit_gtins:
            kit_data = self.get_file_for_gtin(kit_gtin)
            if kit_data:
                kit_data_list.append(kit_data)
            else:
                logger.error(f"Не удалось получить данные для Kit GTIN: {kit_gtin}")
                return None

        # Получаем коды
        kigu_codes = kigu_data.get('codes', [])
        if not kigu_codes:
            logger.error("В файле Kigu нет кодов коробок")
            return None

        all_kit_codes = [kit_data.get('codes', []) for kit_data in kit_data_list]

        # Проверяем, что во всех файлах есть коды
        for i, kit_codes in enumerate(all_kit_codes):
            if not kit_codes:
                logger.error(f"В файле для Kit GTIN {kit_gtins[i]} нет кодов")
                return None

        # Подсчет максимального количества наборов
        max_kits = self.calculate_max_kits(kigu_codes, all_kit_codes)

        if max_kits == 0:
            logger.error("Недостаточно кодов для создания наборов")
            logger.error(f"   Коды Kigu: {len(kigu_codes)}")
            for i, kit_codes in enumerate(all_kit_codes):
                logger.error(f"   Коды Kit {kit_gtins[i]}: {len(kit_codes)}")
            return None

        logger.info("Доступно кодов:")
        logger.info(f"   - Kigu ({kigu_gtin}): {len(kigu_codes)} коробок")
        for i, kit_codes in enumerate(all_kit_codes):
            logger.info(f"   - Kit ({kit_gtins[i]}): {len(kit_codes)} продуктов")
        logger.info(f"Максимально можно создать: {max_kits} наборов")

        # Определяем количество наборов для генерации
        if num_kits is None:
            num_kits = max_kits
        elif num_kits > max_kits:
            logger.warning(f"Запрошено {num_kits} наборов, но доступно только {max_kits}")
            num_kits = max_kits

        logger.info(f"Генерация {num_kits} наборов...")

        # Создаем отчет
        kin_report = self._create_report_data(kigu_gtin, kit_gtins, num_kits)

        if not kin_report:
            logger.error("Не удалось создать данные отчета")
            return None

        # Сохраняем отчет
        output_filename = f"{kigu_gtin}_kin_report.json"

        try:
            with open(output_filename, 'w', encoding='utf-8') as f:
                json.dump(kin_report, f, ensure_ascii=False, indent=2)

            logger.info(f"КИН отчет успешно создан: {output_filename}")
            logger.info(f"Сгенерировано наборов: {num_kits}")
            logger.info(f"Всего продуктов в отчете: {sum(len(box['productNumbers']) for box in kin_report['readyBox'])}")
            return output_filename

        except Exception as e:
            logger.error(f"Ошибка при сохранении отчета: {e}")
            return None

    def _create_report_data(self, kigu_gtin, kit_gtins, num_kits):
        """Создание данных отчета"""
        logger.info(f"Создание данных отчета для {num_kits} наборов...")

        # Получаем данные из файлов
        kigu_data = self.get_file_for_gtin(kigu_gtin)
        if not kigu_data:
            logger.error("Не удалось получить данные Kigu для создания отчета")
            return None

        kit_data_list = []
        for kit_gtin in kit_gtins:
            kit_data = self.get_file_for_gtin(kit_gtin)
            if kit_data:
                kit_data_list.append(kit_data)
            else:
                logger.error(f"Не удалось получить данные Kit ({kit_gtin}) для создания отчета")
                return None

        # Получаем коды
        kigu_codes = kigu_data.get('codes', [])
        all_kit_codes = [kit_data.get('codes', []) for kit_data in kit_data_list]

        if not kigu_codes or any(not codes for codes in all_kit_codes):
            logger.error("Отсутствуют коды для создания данных отчета")
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
            if i < len(kigu_codes):
                box_number = kigu_codes[i]
            else:
                logger.error(f"Недостаточно кодов Kigu для создания набора {i}")
                break

            box = {
                "Number": i,
                "boxNumber": box_number,
                "boxAgregate": True,
                "boxTime": (start_time + timedelta(minutes=random.randint(2, 30))).isoformat(),
                "productNumbers": product_numbers,
                "productNumbersFull": product_numbers_full
            }

            ready_boxes.append(box)

        logger.info(f"Создание данных отчета завершено. Сгенерировано {len(ready_boxes)} наборов")

        return {
            "id": str(uuid.uuid4()),
            "startTime": start_time.isoformat(),
            "endTime": datetime.now().isoformat(),
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
    """
    logger.info("Вызов generate_kin_report_from_files")
    generator = KinReportGenerator()
    return generator.generate_kin_report(file_names, num_kits)

# Функция для использования в Google Colab
def upload_and_process_files_colab():
    """Функция для загрузки файлов в Colab и обработки"""
    logger.info("=== Генератор КИН отчета ===")

    # Загрузка файлов через интерфейс Colab
    logger.info("Загрузите файлы через интерфейс...")
    uploaded = files.upload()

    file_names = []
    for filename, content in uploaded.items():
        if filename.endswith('.json'):
            with open(filename, 'wb') as f:
                f.write(content)
            file_names.append(filename)
            logger.info(f"Загружен: {filename}")

    if not file_names:
        logger.warning("Не загружено ни одного JSON файла")
        return None

    # Генерация отчета
    report_file = generate_kin_report_from_files(file_names)

    if report_file:
        files.download(report_file)
        logger.info(f"Отчет готов: {report_file}")
        return report_file
    else:
        logger.error("Не удалось создать отчет")
        return None

if __name__ == "__main__":
    upload_and_process_files_colab()
