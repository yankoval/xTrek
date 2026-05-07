import os
import json
import logging
import argparse
from typing import List, Dict, Any, Optional, Set
from collections import Counter

from .storage import get_storage
from .trueapi import HonestSignAPI
from .nkapi import NK
from .tokens import TokenProcessor

# Setup logging
logger = logging.getLogger("xtrek.utils")
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(name)s: %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

def cut_crypto_tail(code: str) -> str:
    """Обрезает криптохвост кода (разделитель \u001d)."""
    return code.split('\u001d')[0]

def get_gtin_from_code(code: str) -> Optional[str]:
    """Извлекает GTIN из кода (01 + 14 цифр)."""
    if code.startswith('01') and len(code) >= 16:
        return code[2:16]
    return None

class AggregationAnalyzer:
    def __init__(self, api: HonestSignAPI, nk: NK):
        self.api = api
        self.nk = nk
        self.gtin_cache = {}

    def is_set(self, gtin: str) -> bool:
        if gtin in self.gtin_cache:
            return self.gtin_cache[gtin]

        info = self.nk.feedProduct(gtin)
        if not info:
            logger.error(f"GTIN {gtin} не найден в Национальном Каталоге")
            return None

        is_set = info.get('is_set', False)
        self.gtin_cache[gtin] = is_set
        return is_set

    def check_statuses(self, codes: List[str]) -> List[Dict[str, Any]]:
        """Проверка статусов кодов пачками по 1000."""
        results = []
        for i in range(0, len(codes), 1000):
            batch = codes[i:i+1000]
            try:
                batch_results = self.api.get_list_cis_info(batch)
                if isinstance(batch_results, list):
                    results.extend(batch_results)
                else:
                    logger.error(f"Ошибка API при проверке пачки: {batch_results}")
            except Exception as e:
                logger.error(f"Исключение при проверке пачки: {e}")
        return results

    def analyze(self, file_paths: List[str], s3_config: Optional[Dict] = None) -> Optional[List[str]]:
        errors = []
        all_box_codes = Counter()
        all_child_codes = Counter()

        file_data = []

        # 1. Загрузка и первичный анализ (уникальность)
        for path in file_paths:
            storage = get_storage(path, s3_config)
            try:
                content = storage.read_text(path)
                data = json.loads(content)
            except Exception as e:
                err = f"Ошибка чтения файла {path}: {e}"
                logger.error(err)
                errors.append(err)
                continue

            ready_boxes = data.get('readyBox', [])
            if not ready_boxes:
                logger.warning(f"Файл {path} не содержит readyBox или пуст")
                continue

            file_boxes = []
            file_children = []

            for box in ready_boxes:
                box_code = box.get('boxNumber')
                if box_code:
                    clean_box = cut_crypto_tail(box_code)
                    all_box_codes[clean_box] += 1
                    file_boxes.append(clean_box)

                children = box.get('productNumbersFull') or []
                for child in children:
                    clean_child = cut_crypto_tail(child)
                    all_child_codes[clean_child] += 1
                    file_children.append(clean_child)

            file_data.append({
                'path': path,
                'boxes': file_boxes,
                'children': file_children
            })

        # Проверка уникальности
        for code, count in all_box_codes.items():
            if count > 1:
                errors.append(f"Дубликат кода агрегации: {code} (встречается {count} раз)")
        for code, count in all_child_codes.items():
            if count > 1:
                errors.append(f"Дубликат кода вложения: {code} (встречается {count} раз)")

        # 2. Проверка статусов в True API и анализ по правилам
        for item in file_data:
            path = item['path']
            boxes = item['boxes']
            children = item['children']

            logger.info(f"--- Анализ файла: {path} ---")

            # Проверка агрегатов
            if boxes:
                box_results = self.check_statuses(boxes)
                box_statuses = Counter()
                # API возвращает список. Сопоставляем по requestedCis если нужно, но тут просто идем по результатам
                for res in box_results:
                    cis_info = res.get('cisInfo', {})
                    status = cis_info.get('status')
                    code = cis_info.get('cis') or res.get('requestedCis')
                    if status:
                        box_statuses[status] += 1
                        # "коды SSCC не должны быть зарегестрированны в суз"
                        # Если статус есть, значит код уже "зарегистрирован" (эмитирован или более)
                        errors.append(f"Ошибка: Код агрегации {code} уже зарегистрирован в ГИС МТ (Статус: {status})")
                    else:
                        box_statuses['NOT_FOUND'] += 1

                logger.info(f"Статусы агрегатов: {dict(box_statuses)}")

            # Проверка вложений
            if children:
                child_results = self.check_statuses(children)
                child_statuses = Counter()

                for res in child_results:
                    cis_info = res.get('cisInfo', {})
                    status = cis_info.get('status', 'NOT_FOUND')
                    code = cis_info.get('cis') or res.get('requestedCis')
                    child_statuses[status] += 1

                    gtin = get_gtin_from_code(code)
                    if not gtin:
                        logger.warning(f"Не удалось извлечь GTIN из кода вложения: {code}")
                        continue

                    is_set_flag = self.is_set(gtin)
                    if is_set_flag is None:
                        errors.append(f"Ошибка: GTIN {gtin} для кода {code} не найден в Нац.Каталоге")
                        continue

                    if is_set_flag:
                        # Набор: должен быть APPLIED
                        if status != 'APPLIED':
                            errors.append(f"Ошибка: Код вложения (НАБОР) {code} имеет статус {status}, ожидался APPLIED")
                    else:
                        # Товар: должен быть INTRODUCED
                        if status != 'INTRODUCED':
                            errors.append(f"Ошибка: Код вложения (ТОВАР) {code} имеет статус {status}, ожидался INTRODUCED")

                logger.info(f"Статусы вложений: {dict(child_statuses)}")

        return errors if errors else None

def main():
    parser = argparse.ArgumentParser(description="Анализ отчетов оборудования об агрегации")
    parser.add_argument('files', nargs='+', help="Список JSON файлов (локальных или S3)")
    parser.add_argument('--inn', help="ИНН организации для авторизации")
    parser.add_argument('--token', help="Прямая передача токена True API")
    parser.add_argument('--suz_worker_config', type=str, help='Путь к конфигурационному файлу suz_worker (для S3)')
    parser.add_argument('--debug', action='store_true', help="Включить DEBUG логирование")

    args = parser.parse_args()

    if args.debug:
        logger.setLevel(logging.DEBUG)

    s3_config = None
    if args.suz_worker_config:
        try:
            with open(args.suz_worker_config, 'r', encoding='utf-8') as f:
                s3_config = json.load(f).get('s3_config')
        except Exception as e:
            logger.error(f"Ошибка загрузки конфигурации S3: {e}")

    # Авторизация
    token = args.token
    if not token and args.inn:
        base_path = os.path.dirname(os.path.abspath(__file__))
        orgs_dir = os.path.join(base_path, 'my_orgs')
        tp = TokenProcessor(orgs_dir=orgs_dir)
        token_data = tp.get_token_by_inn(args.inn)
        if token_data:
            token = token_data.get('Токен')
            logger.info(f"Получен токен для ИНН {args.inn}")

    if not token:
        token = os.getenv("TRUE_API_TOKEN")

    if not token:
        logger.error("Не указан токен. Используйте --inn, --token или переменную TRUE_API_TOKEN")
        return

    api = HonestSignAPI(token=token)
    nk = NK(token=token)
    analyzer = AggregationAnalyzer(api, nk)

    errors = analyzer.analyze(args.files, s3_config=s3_config)

    if errors:
        logger.error("--- ОБНАРУЖЕНЫ ОШИБКИ ---")
        for err in errors:
            print(err)
    else:
        logger.info("Проверка завершена успешно, ошибок не обнаружено.")

if __name__ == "__main__":
    main()
