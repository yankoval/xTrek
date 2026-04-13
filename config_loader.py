import os
import json
import logging
from typing import Dict, Any

logger = logging.getLogger("ConfigLoader")

def load_config(env_name: str = 'TOKENS_CONFIG') -> Dict[str, Any]:
    """
    Загружает конфигурацию из файла.
    Порядок поиска:
    1. Переменная окружения env_name (по умолчанию TOKENS_CONFIG)
    2. tokens_config.json в текущей директории
    3. config.json в текущей директории
    4. tokens_config.json в директории скрипта
    5. config.json в директории скрипта
    """
    config_candidates = []

    # 1. Переменная окружения
    env_config = os.environ.get(env_name)
    if env_config:
        config_candidates.append(env_config)

    # 2 & 3. В текущей директории
    config_candidates.append('tokens_config.json')
    config_candidates.append('config.json')

    # 4 & 5. В директории скрипта
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        config_candidates.append(os.path.join(script_dir, 'tokens_config.json'))
        config_candidates.append(os.path.join(script_dir, 'config.json'))
    except Exception:
        pass

    for path in config_candidates:
        if os.path.exists(path):
            try:
                with open(path, 'r', encoding='utf-8') as f:
                    config = json.load(f)
                    logger.info(f"Конфигурация загружена из: {path}")
                    return config
            except Exception as e:
                logger.error(f"Ошибка при чтении файла конфигурации {path}: {e}")

    logger.warning("Файл конфигурации не найден. Будут использованы значения по умолчанию.")
    return {}
