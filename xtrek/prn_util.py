import os
import json
import logging
import argparse
import tempfile
from pathlib import Path
import shutil

from .storage import get_storage
from .config_loader import load_config
import amica.amica_generator as amica_generator

generate_amica_vdf = amica_generator.generate_amica_vdf

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def convert_json_to_raw_csv(input_path, output_path=None):
    """
    Конвертация JSON Честный Знак в RAW CSV (без экранирования),
    аналогично логике из emission_to_csv.py.
    """
    if not output_path:
        base, _ = os.path.splitext(input_path)
        output_path = f"{base}.csv"

    try:
        with open(input_path, 'r', encoding='utf-8') as j_file:
            data = json.load(j_file)

        codes = data.get("codes", [])

        if not codes:
            logger.warning(f"В файле {input_path} не найдено поле 'codes'.")
            return None

        # Открываем файл для прямой записи текста
        with open(output_path, 'w', encoding='utf-8', newline='') as f:
            # Записываем заголовок
            f.write("C1\n")

            # Записываем каждый код "как есть" без обработчиков CSV
            for code in codes:
                f.write(f"{code}\n")

        logger.info(f"CSV файл успешно создан: {output_path}")
        return output_path

    except Exception as e:
        logger.error(f"Ошибка при конвертации JSON в CSV: {e}")
        return None

def generate_prn_files(key: str, vdf_template_name: str = "32x32_20x20.VDF"):
    """
    Основная процедура создания файлов задания на печать.
    key: имя файла эмиссии без расширения .json
    """
    try:
        config = load_config('suz_worker_config')
        s3_config = config.get('s3_config')

        # Пути из конфигурации
        kodes_path = config.get('kodes')
        prn_tasks_path = config.get('prn_tasks')
        prn_templates_path = config.get('prn_templates')

        if not all([kodes_path, prn_tasks_path, prn_templates_path]):
            logger.error("[!] В конфигурации отсутствуют необходимые пути (kodes, prn_tasks, prn_templates)")
            return None

        # Инициализация хранилищ
        storage_kodes = get_storage(kodes_path, s3_config)
        storage_templates = get_storage(prn_templates_path, s3_config)
        storage_tasks = get_storage(prn_tasks_path, s3_config)

        # Пути к файлам в S3
        json_s3_path = f"{kodes_path.rstrip('/')}/{key}.json"
        vdf_template_s3_path = f"{prn_templates_path.rstrip('/')}/{vdf_template_name}"
        amica_json_s3_path = f"{prn_templates_path.rstrip('/')}/amica.json"
        mapping_json_s3_path = f"{prn_templates_path.rstrip('/')}/mapping-empty.json"

        # Создаем временную директорию
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # 1. Скачиваем исходный JSON
            logger.info(f"[*] Скачивание {json_s3_path}...")
            local_json = temp_path / f"{key}.json"
            if not storage_kodes.exists(json_s3_path):
                logger.error(f"[!] Файл {json_s3_path} не найден в S3")
                return None
            storage_kodes.download(json_s3_path, str(local_json))

            # 2. Скачиваем шаблоны и конфиги
            local_template = temp_path / vdf_template_name
            local_amica_json = temp_path / "amica.json"
            local_mapping = temp_path / "mapping.json"

            logger.info(f"[*] Скачивание шаблона {vdf_template_s3_path}...")
            if not storage_templates.exists(vdf_template_s3_path):
                logger.error(f"[!] Шаблон {vdf_template_s3_path} не найден")
                return None
            storage_templates.download(vdf_template_s3_path, str(local_template))

            logger.info(f"[*] Скачивание конфигурации {amica_json_s3_path}...")
            if not storage_templates.exists(amica_json_s3_path):
                logger.error(f"[!] Файл конфигурации {amica_json_s3_path} не найден")
                return None
            storage_templates.download(amica_json_s3_path, str(local_amica_json))

            logger.info(f"[*] Скачивание маппинга {mapping_json_s3_path}...")
            if not storage_templates.exists(mapping_json_s3_path):
                # Если mapping-empty.json не найден, создаем пустой маппинг
                logger.warning(f"[*] Файл маппинга {mapping_json_s3_path} не найден. Используем пустой маппинг.")
                with open(local_mapping, 'w', encoding='utf-8') as f:
                    json.dump([], f)
            else:
                storage_templates.download(mapping_json_s3_path, str(local_mapping))

            # 3. Конвертируем JSON -> CSV
            logger.info("[*] Конвертация JSON в CSV...")
            local_csv = temp_path / f"{key}.csv"
            if not convert_json_to_raw_csv(str(local_json), str(local_csv)):
                return None

            # 4. Генерация VDF
            logger.info("[*] Генерация VDF файла...")
            local_vdf = temp_path / f"{key}.vdf"
            generate_amica_vdf(
                base_template_path=str(local_template),
                new_csv_path=str(local_csv),
                static_json_path=str(local_amica_json),
                mapping_json_path=str(local_mapping),
                output_vdf_path=str(local_vdf)
            )

            # Проверяем, что VDF был создан (имя могло измениться по маске, но по умолчанию оно такое же)
            # В amica_generator.py по умолчанию output_vdf_path используется как база
            if not local_vdf.exists():
                # Пробуем найти любой .vdf во временной папке
                vdf_files = list(temp_path.glob("*.vdf"))
                if vdf_files:
                    local_vdf = vdf_files[0]
                else:
                    logger.error("[!] VDF файл не был создан")
                    return None

            # 5. Загружаем CSV и VDF в целевой бакет
            dest_csv_s3 = f"{prn_tasks_path.rstrip('/')}/{key}.csv"
            dest_vdf_s3 = f"{prn_tasks_path.rstrip('/')}/{local_vdf.name}"

            logger.info(f"[*] Загрузка CSV в {dest_csv_s3}...")
            storage_tasks.upload(str(local_csv), dest_csv_s3)

            logger.info(f"[*] Загрузка VDF в {dest_vdf_s3}...")
            storage_tasks.upload(str(local_vdf), dest_vdf_s3)

            logger.info(f"[+++] Процедура успешно завершена для {key}")
            return key

    except Exception as e:
        logger.error(f"[!] Ошибка в generate_prn_files: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return None

def main():
    parser = argparse.ArgumentParser(description="Генерация файлов задания на печать (PRN) на основе кодов эмиссии")
    parser.add_argument("key", help="Ключ объекта эмиссии (имя файла без .json)")
    parser.add_argument("--template", default="32x32_20x20.VDF", help="Имя файла шаблона VDF")
    parser.add_argument("--config", help="Путь к файлу конфигурации suz_worker_config")

    args = parser.parse_args()

    if args.config:
        os.environ['suz_worker_config'] = args.config

    result = generate_prn_files(args.key, args.template)
    if result:
        print(f"Success: {result}")
    else:
        print("Failed")
        exit(1)

if __name__ == "__main__":
    main()
