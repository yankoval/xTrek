import json
import csv
import logging
import re
import requests
import time
from datetime import datetime

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def format_gs1_date(date_str):
    """Преобразует дату из формата YYYY-MM-DD в YYMMDD."""
    try:
        return datetime.strptime(date_str, '%Y-%m-%d').strftime('%y%m%d')
    except Exception as e:
        logger.error(f"Ошибка преобразования даты {date_str}: {e}")
        raise

def get_sscc_from_service(function_url, prefix, count, extension=None):
    """Запрашивает SSCC коды у внешнего сервиса с замером времени отклика."""
    count = int(count)
    logger.info(f"Запрос {count} кодов SSCC (Prefix: {prefix}, Ext: {extension})")
    payload = {"prefix": prefix, "count": count}
    if extension is not None:
        payload["extension"] = extension

    start_time = time.perf_counter()
    try:
        response = requests.post(function_url, json=payload, timeout=15)
        response.raise_for_status()

        latency = (time.perf_counter() - start_time) * 1000
        data = response.json()
        codes = data.get("ssccs", [])

        logger.info(f"Ответ от сервиса получен за {latency:.2f} мс.")

        if len(codes) < count:
            error_msg = f"Критическая ошибка: сервис вернул {len(codes)} кодов вместо {count}."
            logger.error(error_msg)
            raise ValueError(error_msg)

        return codes
    except requests.exceptions.RequestException as e:
        logger.error(f"Ошибка сети/сервиса через {(time.perf_counter() - start_time)*1000:.2f} мс: {e}")
        return []

def generate_gs1_csv(json_path, output_path, sscc_path=None,
                     column_name='C1', sscc_url=None,
                     sscc_prefix=None, sscc_extension=None,
                     gs1_template="00{sscc}"):
    """
    Генерация CSV для DataMatrix.
    Все ключи JSON обязательны. Отсутствие ключа вызывает прерывание с ошибкой.
    """
    logger.info("=== Запуск процедуры генерации ===")

    # 1. Загрузка данных из JSON
    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        # Прямое обращение к ключам (вызовет KeyError, если ключа нет)
        target_quantity = int(data['Quantity'])
        p_data = data['PasportData']

        # Подготовка полей AI (Обязательные ключи)
        gtin = str(p_data['Product_PackBarcode']).zfill(14)
        batch = str(p_data['Batch_BN_1С_full']) # Обязательный ключ

        d_prod = format_gs1_date(p_data['Batch_date_production'])
        d_pack = format_gs1_date(p_data['Batch_date_packing'])
        d_exp = format_gs1_date(p_data['Batch_date_expired'])

        logger.info(f"JSON успешно валидирован. Партия: {batch}, Кол-во: {target_quantity}")

    except KeyError as e:
        logger.error(f"КРИТИЧЕСКАЯ ОШИБКА: В JSON отсутствует обязательный ключ: {e}")
        return
    except Exception as e:
        logger.error(f"Ошибка при обработке JSON: {e}")
        return

    # 2. Получение SSCC
    sscc_list = []

    if sscc_path:
        try:
            with open(sscc_path, 'r', encoding='utf-8') as f:
                raw_lines = [l.strip() for l in f.readlines() if l.strip() and not l.startswith('[')]

            if not raw_lines:
                logger.error("Файл SSCC пуст.")
                return

            if re.match(r'^\d{18,20}$', raw_lines[0]):
                sscc_list = [l for l in raw_lines if re.match(r'^\d{18,20}$', l)]
            else:
                start_idx = next((i for i, line in enumerate(raw_lines) if column_name in line), -1)
                if start_idx == -1:
                    logger.error(f"Заголовок '{column_name}' не найден во входном файле.")
                    return

                reader = csv.DictReader(raw_lines[start_idx:], delimiter=' ')
                sscc_list = [row[column_name].strip() for row in reader if row.get(column_name)]

            logger.info(f"Файл прочитан. Получено кодов: {len(sscc_list)}")
        except Exception as e:
            logger.error(f"Ошибка чтения файла SSCC: {e}")
            return
    else:
        if not all([sscc_url, sscc_prefix]):
            logger.error("Параметры API не заданы.")
            return
        try:
            sscc_list = get_sscc_from_service(sscc_url, sscc_prefix, target_quantity, sscc_extension)
        except ValueError:
            return

    if not sscc_list:
        logger.error("Список SSCC пуст. Процедура остановлена.")
        return

    # 3. Запись выходного CSV
    try:
        GS = '\x1d'
        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([column_name])

            for sscc in sscc_list:
                gs1_string = gs1_template.format(
                    sscc=sscc,
                    gtin=gtin,
                    batch=batch,
                    GS=GS,
                    d_prod=d_prod,
                    d_pack=d_pack,
                    d_exp=d_exp
                )
                writer.writerow([gs1_string])

        logger.info(f"Файл успешно создан: {output_path}")
    except Exception as e:
        logger.error(f"Ошибка записи результата: {e}")

if __name__ == "__main__":
    generate_gs1_csv(
        json_path='BN000806463.json',
        output_path='result_datamatrix.csv',
        sscc_path=None,
        column_name='C1',
        sscc_url="https://functions.yandexcloud.net/YOUR_ID",
        sscc_prefix="460705179",
        sscc_extension="0"
    )