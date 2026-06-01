import os
import json
import logging
from typing import Dict, Any

logger = logging.getLogger("ConfigLoader")

def load_config(env_name: str = 'TOKENS_CONFIG') -> Dict[str, Any]:
    """
    Загружает конфигурацию из файла.
    """
    config_candidates = []

    # 1. Переменные окружения
    for env in [env_name, 'suz_worker_config']:
        val = os.environ.get(env)
        if val:
            if val.endswith('.json') and os.path.exists(val):
                config_candidates.append(val)
            elif not val.endswith('.json'):
                try:
                    config = json.loads(val)
                    logger.info(f"Конфигурация загружена из переменной окружения {env}")
                    return config
                except:
                    pass

    # 2. Файлы
    candidates = ['tokens_config.json', 'suz_worker_config.json', 'config.json']

    for c in candidates:
        config_candidates.append(c)

    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        for c in candidates:
            config_candidates.append(os.path.join(script_dir, c))
    except Exception:
        pass

    checked_paths = []
    for path in config_candidates:
        abs_path = os.path.abspath(path)
        if abs_path in checked_paths: continue
        checked_paths.append(abs_path)

        if os.path.exists(path):
            try:
                with open(path, 'r', encoding='utf-8-sig') as f:
                    config = json.load(f)
                    logger.info(f"Конфигурация загружена из: {path}")
                    return config
            except Exception as e:
                logger.error(f"Ошибка при чтении файла конфигурации {path}: {e}")

    logger.warning(f"Файл конфигурации не найден. Проверены пути: {checked_paths}")
    return {}
