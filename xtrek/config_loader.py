import os
import json
import logging
from typing import Dict, Any

logger = logging.getLogger("ConfigLoader")

def load_config(env_name: str = 'TOKENS_CONFIG') -> Dict[str, Any]:
    """
    Загружает и объединяет конфигурацию из всех доступных источников.
    Порядок приоритета (от низкого к высокому):
    1. config.json
    2. suz_worker_config.json
    3. tokens_config.json
    4. переменная suz_worker_config (файл или JSON)
    5. переменная env_name (файл или JSON)
    """
    merged_config = {}

    # 1. Сбор путей к файлам (от низкого приоритета к высокому)
    candidates = ['config.json', 'suz_worker_config.json', 'tokens_config.json']
    file_paths = []

    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        for c in candidates:
            file_paths.append(os.path.join(script_dir, c))
    except: pass

    for c in candidates:
        file_paths.append(c)

    # Загружаем файлы в порядке приоритета
    checked_paths = set()
    for path in file_paths:
        abs_path = os.path.abspath(path)
        if abs_path in checked_paths: continue
        checked_paths.add(abs_path)

        if os.path.exists(path):
            try:
                with open(path, 'r', encoding='utf-8-sig') as f:
                    data = json.load(f)
                    merged_config.update(data)
                    logger.info(f"Конфигурация дополнена из файла: {path}")
            except Exception as e:
                logger.error(f"Ошибка при чтении {path}: {e}")

    # 2. Переменные окружения (имеют высший приоритет)
    for env in ['suz_worker_config', env_name]:
        val = os.environ.get(env)
        if not val: continue

        # Если путь к файлу
        if val.endswith('.json') and os.path.exists(val):
            try:
                with open(val, 'r', encoding='utf-8-sig') as f:
                    data = json.load(f)
                    merged_config.update(data)
                    logger.info(f"Конфигурация дополнена из файла (env {env}): {val}")
            except Exception as e:
                logger.error(f"Ошибка при чтении {val}: {e}")
        else:
            # Возможно сам JSON
            try:
                data = json.loads(val)
                merged_config.update(data)
                logger.info(f"Конфигурация дополнена из JSON (env {env})")
            except:
                # Если не JSON и не существующий файл - игнорируем
                pass

    if not merged_config:
        logger.warning(f"Конфигурация не найдена. Проверены файлы: {list(checked_paths)}")

    return merged_config
