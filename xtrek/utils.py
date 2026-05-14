import os
import json
import logging
import argparse
import tempfile
from typing import List, Dict, Any, Optional, Set
from collections import Counter, defaultdict

from .storage import get_storage
from .trueapi import HonestSignAPI
from .nkapi import NK
from .tokens import TokenProcessor
from .gs1_processor import get_inn_by_gtin
from .config_loader import load_config

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

def normalize_sscc(code: str) -> str:
    """Добавляет префикс 00 к 18-значным SSCC кодам."""
    if len(code) == 18 and code.isdigit():
        return '00' + code
    return code

def get_gtin_from_code(code: str) -> Optional[str]:
    """Извлекает GTIN из кода (01 + 14 цифр)."""
    if code.startswith('01') and len(code) >= 16:
        return code[2:16]
    return None

MIN_SSCC_IN_AGG_REP_DEFAULT = 10

class AggregationAnalyzer:
    def __init__(self, api: HonestSignAPI, nk: NK, config: Optional[Dict] = None):
        self.api = api
        self.nk = nk
        self.config = config or {}
        self.gtin_cache = {}
        self.min_sscc = self.config.get('MIN_SSCC_IN_AGG_REP', MIN_SSCC_IN_AGG_REP_DEFAULT)

    def is_set(self, gtin: str) -> Optional[bool]:
        if gtin in self.gtin_cache:
            return self.gtin_cache[gtin]

        product = self.nk.feedProduct(gtin)
        if not product:
            product = self.nk.get_set_by_gtin(gtin)

        if not product or not product.get("result"):
            logger.error(f"GTIN {gtin} не найден в Национальном Каталоге")
            return None

        item = product["result"][0]
        is_set = item.get('is_set', False)
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
                    error_msg = batch_results.get('error', 'Unknown error')
                    logger.error(f"Ошибка API при проверке пачки из {len(batch)} кодов: {error_msg}")
            except Exception as e:
                logger.error(f"Исключение при проверке пачки: {e}")
        return results

    def check_report(self, path: str, s3_config: Optional[Dict] = None) -> Optional[Dict[str, List[str]]]:
        """Проверяет один отчет об агрегации и устанавливает тег check."""
        errors = defaultdict(list)
        storage = get_storage(path, s3_config or self.config.get('s3_config'))

        try:
            content = storage.read_text(path)
            data = json.loads(content)
        except Exception as e:
            logger.error(f"Ошибка чтения файла {path}: {e}")
            return {'filereaderror': [str(e)]}

        ready_boxes = data.get('readyBox', [])

        # Проверка на минимальное количество коробок
        if len(ready_boxes) < self.min_sscc:
            errors['minssccinaggrep'].append(f"Количество коробок {len(ready_boxes)} меньше {self.min_sscc}")

        box_codes_counter = Counter()
        child_codes_counter = Counter()

        clean_boxes = []
        clean_children = []

        for box in ready_boxes:
            box_code = box.get('boxNumber')
            if box_code:
                clean_box = normalize_sscc(cut_crypto_tail(box_code))
                box_codes_counter[clean_box] += 1
                clean_boxes.append(clean_box)

            children = box.get('productNumbersFull') or []
            for child in children:
                clean_child = cut_crypto_tail(child)
                child_codes_counter[clean_child] += 1
                clean_children.append(clean_child)

        # Проверка уникальности внутри файла
        for code, count in box_codes_counter.items():
            if count > 1:
                errors['duplicateaggregation'].append(code)
        for code, count in child_codes_counter.items():
            if count > 1:
                errors['duplicateattachment'].append(code)

        # Проверка статусов в ГИС МТ
        if clean_boxes:
            box_results = self.check_statuses(clean_boxes)
            for res in box_results:
                cis_info = res.get('cisInfo', {})
                status = cis_info.get('status')
                code = cis_info.get('cis') or res.get('requestedCis')
                if status:
                    errors['alreadyregistered'].append(f"{code} (Статус: {status})")

        if clean_children:
            child_results = self.check_statuses(clean_children)
            for res in child_results:
                cis_info = res.get('cisInfo', {})
                status = cis_info.get('status', 'NOT_FOUND')
                code = cis_info.get('cis') or res.get('requestedCis')

                gtin = get_gtin_from_code(code)
                if not gtin:
                    continue

                is_set_flag = self.is_set(gtin)
                if is_set_flag is None:
                    errors['gtinnotfound'].append(gtin)
                    continue

                if is_set_flag:
                    if status != 'EMITTED':
                        errors['wrongsetstatus'].append(f"{code} (Статус: {status})")
                else:
                    if status != 'INTRODUCED':
                        errors['wrongunitstatus'].append(f"{code} (Статус: {status})")

        result = dict(errors) if errors else None

        # Установка тега check
        tag_value = ""
        if result:
            tag_value = "-".join(sorted(result.keys()))

        try:
            storage.set_tags(path, {'check': tag_value})
        except Exception as e:
            logger.error(f"Не удалось установить тег check для {path}: {e}")

        return result

def check_aggregation_report(path: str, api: HonestSignAPI, nk: NK, config: Optional[Dict] = None) -> Optional[Dict[str, List[str]]]:
    """Функция для проверки одного отчета об агрегации."""
    analyzer = AggregationAnalyzer(api, nk, config)
    return analyzer.check_report(path)

def check_aggregation_reports(paths: List[str], api: HonestSignAPI, nk: NK, config: Optional[Dict] = None) -> Dict[str, Optional[Dict[str, List[str]]]]:
    """Функция-обертка для проверки списка отчетов."""
    analyzer = AggregationAnalyzer(api, nk, config)
    results = {}
    for path in paths:
        results[path] = analyzer.check_report(path)
    return results

def resolve_file_path(path: str, config: Dict) -> str:
    """Разрешает путь к файлу, добавляя префикс из конфига если нужно."""
    if not config:
        return path

    # Если путь не содержит s3://, не имеет расширения и не содержит разделителей папок
    if not path.startswith('s3://') and '.' not in os.path.basename(path) and '/' not in path and '\\' not in path:
        reports_path = config.get('equipment-reports')
        if reports_path:
            if reports_path.startswith('s3://'):
                return f"{reports_path.rstrip('/')}/{path}.json"

            s3_config = config.get('s3_config', {})
            bucket = s3_config.get('bucket')
            if bucket:
                return f"s3://{bucket}/{reports_path.lstrip('/')}/{path}.json"

    return path

def main():
    parser = argparse.ArgumentParser(description="Анализ отчетов оборудования об агрегации")
    parser.add_argument('files', nargs='+', help="Список JSON файлов (локальных или S3)")
    parser.add_argument('--inn', help="ИНН организации для авторизации")
    parser.add_argument('--token', help="Прямая передача токена True API")
    parser.add_argument('--suz_worker_config', type=str, help='Путь к конфигурационному файлу suz_worker (для S3)')
    parser.add_argument('--debug', action='store_true', help="Включить DEBUG логирование")
    parser.add_argument('--full', action='store_true', help="Выводить полный список проблемных кодов на экран")

    args = parser.parse_args()

    if args.debug:
        logger.setLevel(logging.DEBUG)

    if args.suz_worker_config:
        os.environ['suz_worker_config'] = args.suz_worker_config

    config = load_config('suz_worker_config')
    s3_config = config.get('s3_config')

    # Разрешение путей к файлам
    resolved_files = [resolve_file_path(f, config) for f in args.files]

    # Авторизация
    token = args.token

    # Если токен не задан, попробуем поискать в переменных окружения
    if not token:
        token = os.getenv("TRUE_API_TOKEN")

    if not token and args.inn:
        base_path = os.path.dirname(os.path.abspath(__file__))
        orgs_dir = os.path.join(base_path, 'my_orgs')
        tp = TokenProcessor(orgs_dir=orgs_dir)
        token_data = tp.get_token_by_inn(args.inn)
        if token_data:
            token = token_data.get('Токен')
            logger.info(f"Получен токен для ИНН {args.inn}")

    # Если токен не задан, попробуем определить ИНН по первому найденному GTIN в файлах
    if not token and not args.inn:
        logger.info("Попытка автоматического определения ИНН по GTIN из файлов...")
        detected_inn = None
        for path in resolved_files:
            storage = get_storage(path, s3_config)
            try:
                content = storage.read_text(path)
                data = json.loads(content)
                ready_boxes = data.get('readyBox', [])
                for box in ready_boxes:
                    # Пробуем извлечь GTIN из коробки или вложений
                    codes_to_check = [box.get('boxNumber')] + (box.get('productNumbersFull') or [])
                    for code in codes_to_check:
                        if code:
                            gtin = get_gtin_from_code(cut_crypto_tail(code))
                            if gtin:
                                detected_inn = get_inn_by_gtin(gtin)
                                if detected_inn:
                                    logger.info(f"Автоматически определен ИНН: {detected_inn} (по GTIN {gtin})")
                                    break
                    if detected_inn: break
            except Exception:
                continue
            if detected_inn: break

        if detected_inn:
            base_path = os.path.dirname(os.path.abspath(__file__))
            orgs_dir = os.path.join(base_path, 'my_orgs')
            tp = TokenProcessor(orgs_dir=orgs_dir)
            token_data = tp.get_token_by_inn(detected_inn)
            if token_data:
                token = token_data.get('Токен')
                logger.info(f"Получен токен для автоматически определенного ИНН {detected_inn}")

    if not token:
        logger.error("Не указан токен. Используйте --inn, --token или переменную TRUE_API_TOKEN")
        return

    api = HonestSignAPI(token=token)
    nk = NK(token=token)

    results = check_aggregation_reports(resolved_files, api, nk, config)

    all_errors_details = {}

    for path, errors in results.items():
        tag_value = "-".join(sorted(errors.keys())) if errors else ""
        if len(resolved_files) > 1:
            print(f"{path}: {tag_value}")
        else:
            print(tag_value)

        if errors:
            all_errors_details[path] = errors

    if all_errors_details:
        # Сохранение во временный файл
        try:
            with tempfile.NamedTemporaryFile(mode='w', prefix='agg_errors_', suffix='.json', delete=False, encoding='utf-8') as tf:
                json.dump(all_errors_details, tf, ensure_ascii=False, indent=2)
                temp_path = tf.name

            logger.info(f"Полный список проблем сохранен в: {temp_path}")
        except Exception as e:
            logger.error(f"Не удалось создать временный файл: {e}")

        if args.full:
            print("\n--- ДЕТАЛЬНЫЙ ОТЧЕТ ОБ ОШИБКАХ ---")
            print(json.dumps(all_errors_details, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()
