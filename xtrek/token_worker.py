import time
import logging
import os
import json
from pathlib import Path

# Импорты ваших модулей
from .tokens import TokenProcessor
from .org_manager import OrganizationManager
from .config_loader import load_config

# Импортируем ваш метод получения токена
try:
    from .crpt_auth import get_new_token as refresh_token
except ImportError as e:
    logging.error(f"Критическая ошибка импорта crpt_auth: {e}")
    raise e

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger("TokenWorker")

class TokenRefreshWorker:
    def __init__(self):
        self.config = load_config()
        # Путь к базе организаций
        org_storage = Path(__file__).parent / "my_orgs"
        self.org_manager = OrganizationManager(str(org_storage))
        self.tp = TokenProcessor(org_manager=self.org_manager)
        self.interval = self.config.get('tokens_update_interval', 600)

    def check_and_refresh(self):
        logger.info("--- Запуск цикла проверки токенов (JWT + Auth/СУЗ) ---")
        
        # Синхронизация данных
        if hasattr(self.tp, '_sync_from_s3'):
            self.tp._sync_from_s3()
        self.tp.read_tokens_file()
        self.tp.process_tokens()

        if hasattr(self.org_manager, '_sync_from_s3'):
            self.org_manager._sync_from_s3()
        self.org_manager.sync_from_disk()
        
        organizations = self.org_manager.list()

        if not organizations:
            logger.warning("Список организаций пуст.")
            return

        for org in organizations:
            inn = str(org.inn) if org.inn else None
            conid = str(org.connection_id) if org.connection_id else None
            name = org.name

            if not inn:
                logger.debug(f"Пропуск {name}: отсутствует ИНН")
                continue

            # --- БЛОК 1: Мониторинг JWT (Простой токен) ---
            # Передаем conid=None, чтобы TokenProcessor искал именно "чистый" JWT для ИНН
            jwt_token = self.tp.get_token_value_by_inn(inn, conid=None)
            
            if jwt_token:
                logger.info(f"[{name}] JWT: Актуален")
            else:
                logger.warning(f"[{name}] JWT: Требуется обновление (mode='jwt')...")
                try:
                    new_jwt = refresh_token(inn, mode='jwt')
                    if new_jwt:
                        # Сохраняем без conid (как основной токен организации)
                        self.tp.save_token(new_jwt, conid=None)
                        logger.info(f"[{name}] JWT: Успешно обновлен")
                    else:
                        logger.error(f"[{name}] JWT: Ошибка получения (проверьте подпись)")
                except Exception as e:
                    logger.error(f"Ошибка при обновлении JWT для {name}: {e}")

            # --- БЛОК 2: Мониторинг Auth (Токен СУЗ через Connection ID) ---
            if conid:
                # Ищем токен именно для этой связки ИНН + ConnectionID
                auth_token = self.tp.get_token_value_by_inn(inn, token_type='auth', conid=conid)
                
                if auth_token:
                    logger.info(f"[{name}] Auth (СУЗ): Актуален")
                else:
                    logger.warning(f"[{name}] Auth (СУЗ): Требуется обновление (mode='auth')...")
                    try:
                        # Вызываем с указанием conid и режима auth
                        new_auth = refresh_token(inn, conid=conid, mode='auth')
                        if new_auth:
                            # Сохраняем с привязкой к conid
                            self.tp.save_token(new_auth, conid=conid)
                            logger.info(f"[{name}] Auth (СУЗ): Успешно обновлен")
                        else:
                            logger.error(f"[{name}] Auth: Ошибка получения (проверьте подпись)")
                    except Exception as e:
                        logger.error(f"Ошибка при обновлении Auth для {name}: {e}")
            else:
                logger.debug(f"[{name}] Auth: Пропуск (нет ConnectionID)")

    def start(self, interval: int = None):
        if interval is None:
            interval = self.interval
        logger.info("Воркер мониторинга запущен (Интервал: %s сек).", interval)
        try:
            while True:
                self.check_and_refresh()
                logger.info("Ожидание следующей итерации...")
                time.sleep(interval)
        except KeyboardInterrupt:
            logger.info("Воркер остановлен пользователем.")

if __name__ == "__main__":
    worker = TokenRefreshWorker()
    worker.start()
