"""One-shot master token refresh, suitable for a systemd timer or cron."""
import argparse
import logging
from pathlib import Path

from .tokens import TokenProcessor
from .org_manager import OrganizationManager
from .config_loader import load_config
from .crpt_auth import get_new_token as refresh_token

logger = logging.getLogger("TokenWorker")


class TokenRefreshWorker:
    def __init__(self):
        self.config = load_config()
        self.refresh_before_expiry = int(
            self.config.get('tokens_refresh_before_expiry_seconds', 1800)
        )
        if self.refresh_before_expiry <= 0:
            raise ValueError("tokens_refresh_before_expiry_seconds must be positive")
        org_storage = Path(__file__).parent / "my_orgs"
        self.org_manager = OrganizationManager(str(org_storage))
        self.tp = TokenProcessor(org_manager=self.org_manager, tokens_read_only=False)

    def check_and_refresh(self):
        """Refresh only missing/expiring tokens; return False on any failure."""
        logger.info("--- Проверка токенов в режиме мастера ---")
        # Never publish over S3 using an unavailable or stale source.
        self.tp._sync_from_s3(required=True)
        organizations = self.org_manager.list()
        allowed_inns = self.config.get('tokens_allowed_inns')
        if allowed_inns is not None:
            if not isinstance(allowed_inns, list) or not allowed_inns:
                raise ValueError("tokens_allowed_inns must be a non-empty list")
            allowed_inns = {str(inn) for inn in allowed_inns}
            available_inns = {str(org.inn) for org in organizations}
            if allowed_inns - available_inns:
                logger.error("Не найдены рабочие ИНН: %s", sorted(allowed_inns - available_inns))
                return False
            organizations = [org for org in organizations if str(org.inn) in allowed_inns]
        if not organizations:
            logger.error("Список организаций пуст")
            return False

        current = refreshed = failed = 0
        for org in organizations:
            inn = str(org.inn) if org.inn else None
            conid = str(org.connection_id) if org.connection_id else None
            if not inn:
                logger.error("Организация %s: отсутствует ИНН", org.name)
                failed += 1
                continue

            specs = [('JWT', 'jwt', None)]
            if conid:
                specs.append(('auth', 'auth', conid))
            for token_type, mode, connection in specs:
                try:
                    token = self.tp.get_token_value_by_inn(
                        inn, token_type=token_type, conid=connection
                    )
                    remaining = self.tp.get_token_remaining_seconds(
                        inn, token_type=token_type, conid=connection
                    )
                    if token and remaining is not None and remaining > self.refresh_before_expiry:
                        current += 1
                        logger.info("ИНН %s, %s: актуален, осталось %.0f сек", inn, mode, remaining)
                        continue

                    logger.info("ИНН %s, %s: требуется обновление", inn, mode)
                    new_token = refresh_token(inn, conid=connection, mode=mode)
                    if not new_token:
                        raise RuntimeError("Не удалось получить новый токен")
                    self.tp.save_token(new_token, conid=connection)
                    if self.tp.get_token_value_by_inn(
                        inn, token_type=token_type, conid=connection
                    ) != new_token:
                        raise RuntimeError("Сохранённый токен не прошёл проверку активности и ИНН")
                    refreshed += 1
                    logger.info("ИНН %s, %s: успешно обновлён", inn, mode)
                except Exception as exc:
                    failed += 1
                    logger.error("ИНН %s, %s: ошибка обновления: %s", inn, mode, exc)

        logger.info("Цикл завершён: актуальны=%s, обновлены=%s, ошибки=%s", current, refreshed, failed)
        return failed == 0


def main(argv=None):
    parser = argparse.ArgumentParser(description="Однократное обновление истекающих токенов в режиме мастера")
    parser.add_argument('--once', action='store_true', help="Выполнить один цикл (поведение по умолчанию)")
    parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
    try:
        return 0 if TokenRefreshWorker().check_and_refresh() else 1
    except Exception as exc:
        logger.error("Цикл обновления токенов не выполнен: %s", exc)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
