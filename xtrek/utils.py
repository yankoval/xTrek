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
        all_introduced = True
        status_checked = False

        if clean_boxes:
            box_results = self.check_statuses(clean_boxes)
            for res in box_results:
                status_checked = True
                cis_info = res.get('cisInfo', {})
                status = cis_info.get('status')
                code = cis_info.get('cis') or res.get('requestedCis')
                if status:
                    errors['alreadyregistered'].append(f"{code} (Статус: {status})")
                    if status != 'INTRODUCED':
                        all_introduced = False
                else:
                    all_introduced = False

        if clean_children:
            child_results = self.check_statuses(clean_children)
            for res in child_results:
                status_checked = True
                cis_info = res.get('cisInfo', {})
                status = cis_info.get('status', 'NOT_FOUND')
                code = cis_info.get('cis') or res.get('requestedCis')

                if status != 'INTRODUCED':
                    all_introduced = False

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

        # Если все коды в статусе INTRODUCED, то отчет считается завершенным
        if status_checked and all_introduced:
            result = {'finished': ['All codes are INTRODUCED']}
        else:
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

# Глобальный кеш для ресурсов
_RESOURCES_CACHE = {
    'config': None,
    'api': {}, # token -> api
    'nk': {},  # token -> nk
    'last_token': None,
}

def _ensure_resources(path: str, api: Optional[HonestSignAPI] = None, nk: Optional[NK] = None, config: Optional[Dict] = None):
    """Обеспечивает наличие API, NK и конфига, выполняя автодетекцию если нужно."""
    if config is None:
        if _RESOURCES_CACHE['config'] is None:
            _RESOURCES_CACHE['config'] = load_config('suz_worker_config')
        config = _RESOURCES_CACHE['config']

    resolved_path = resolve_file_path(path, config)

    # Если переданы и API и NK, просто возвращаем их
    if api and nk:
        return resolved_path, api, nk, config

    # Попытка найти токен
    token = None
    if api:
        token = api.token
    elif nk:
        token = nk.token # У NK тоже есть атрибут token

    if not token:
        token = os.getenv("TRUE_API_TOKEN")

    # Если токен все еще не найден, пробуем использовать последний успешно определенный
    if not token:
        token = _RESOURCES_CACHE['last_token']

    if not token:
        # Автодетекция ИНН по файлу
        s3_config = config.get('s3_config')
        try:
            storage = get_storage(resolved_path, s3_config)
            if not storage.exists(resolved_path):
                 logger.warning(f"Файл {resolved_path} не найден для автодетекции ИНН")
            else:
                content = storage.read_text(resolved_path)
                data = json.loads(content)
                ready_boxes = data.get('readyBox', [])
                detected_inn = None
                for box in ready_boxes:
                    codes_to_check = [box.get('boxNumber')] + (box.get('productNumbersFull') or [])
                    for code in codes_to_check:
                        if code:
                            gtin = get_gtin_from_code(cut_crypto_tail(code))
                            if gtin:
                                detected_inn = get_inn_by_gtin(gtin)
                                if detected_inn:
                                    logger.info(f"Автоматически определен ИНН {detected_inn} по GTIN {gtin} из файла {resolved_path}")
                                    break
                    if detected_inn: break

                if not detected_inn:
                    logger.warning(f"Не удалось извлечь GTIN или сопоставить ИНН для файла {resolved_path}")
        except json.JSONDecodeError as e:
            logger.error(f"Ошибка парсинга JSON в файле {resolved_path} при автодетекции: {e}")
        except Exception as e:
            logger.error(f"Ошибка при чтении файла для автодетекции ИНН {resolved_path}: {e}")
            detected_inn = None

        if detected_inn:
            base_path = os.path.dirname(os.path.abspath(__file__))
            orgs_dir = os.path.join(base_path, 'my_orgs')
            tp = TokenProcessor(orgs_dir=orgs_dir)
            token_data = tp.get_token_by_inn(detected_inn)
            if token_data:
                token = token_data.get('Токен')
                _RESOURCES_CACHE['last_token'] = token

    if not token:
        raise ValueError(f"Не удалось определить токен для проверки {resolved_path}. "
                         f"Укажите токен явно или обеспечьте наличие ИНН в базе для GTIN из файла.")

    if not api:
        if token not in _RESOURCES_CACHE['api']:
            host = config.get('true_api_host')
            _RESOURCES_CACHE['api'][token] = HonestSignAPI(token=token, host=host)
        api = _RESOURCES_CACHE['api'][token]

    if not nk:
        if token not in _RESOURCES_CACHE['nk']:
            host = config.get('nk_api_host')
            _RESOURCES_CACHE['nk'][token] = NK(token=token, host=host)
        nk = _RESOURCES_CACHE['nk'][token]

    return resolved_path, api, nk, config

def check_aggregation_report(path: str, api: Optional[HonestSignAPI] = None, nk: Optional[NK] = None, config: Optional[Dict] = None) -> Optional[Dict[str, List[str]]]:
    """Функция для проверки одного отчета об агрегации."""
    resolved_path, api, nk, config = _ensure_resources(path, api, nk, config)
    analyzer = AggregationAnalyzer(api, nk, config)
    return analyzer.check_report(resolved_path)

def check_aggregation_reports(paths: List[str], api: Optional[HonestSignAPI] = None, nk: Optional[NK] = None, config: Optional[Dict] = None) -> Dict[str, Optional[Dict[str, List[str]]]]:
    """Функция-обертка для проверки списка отчетов."""
    results = {}
    for path in paths:
        try:
            resolved_path, current_api, current_nk, current_config = _ensure_resources(path, api, nk, config)
            analyzer = AggregationAnalyzer(current_api, current_nk, current_config)
            results[resolved_path] = analyzer.check_report(resolved_path)
        except Exception as e:
            logger.error(f"Ошибка при проверке {path}: {e}")
            results[path] = {'error': [str(e)]}
    return results

def resolve_file_path(path: str, config: Dict) -> str:
    """Разрешает путь к файлу, добавляя префикс из конфига если нужно."""
    if not config:
        return path

    # Если путь не содержит s3://, не заканчивается на .json и не содержит разделителей папок
    if not path.startswith('s3://') and not path.lower().endswith('.json') and '/' not in path and '\\' not in path:
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

    # Подготовка API и NK если переданы токен или ИНН
    api = None
    nk = None
    token = args.token or os.getenv("TRUE_API_TOKEN")

    if not token and args.inn:
        base_path = os.path.dirname(os.path.abspath(__file__))
        orgs_dir = os.path.join(base_path, 'my_orgs')
        tp = TokenProcessor(orgs_dir=orgs_dir)
        token_data = tp.get_token_by_inn(args.inn)
        if token_data:
            token = token_data.get('Токен')
            logger.info(f"Получен токен для ИНН {args.inn}")

    if token:
        api = HonestSignAPI(token=token)
        nk = NK(token=token)

    results = check_aggregation_reports(args.files, api, nk)

    all_errors_details = {}

    for path, errors in results.items():
        tag_value = "-".join(sorted(errors.keys())) if errors else ""
        if len(results) > 1:
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
