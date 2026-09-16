"""One-shot master token refresh, suitable for a systemd timer or cron."""
import argparse
import logging
from pathlib import Path

from .tokens import TokenProcessor
from .org_manager import OrganizationManager
from .config_loader import load_config
from .crpt_auth import issue_token

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
        with self.tp.writer_lock():
            return self._check_and_refresh_locked()

    def _check_and_refresh_locked(self):
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

        purposes = self.config.get('tokens_purposes', ['true_api', 'suz'])
        if (not isinstance(purposes, list) or not purposes
                or any(p not in {'true_api', 'suz'} for p in purposes)):
            raise ValueError('tokens_purposes must be a non-empty list of true_api/suz')

        current = refreshed = failed = 0
        for org in organizations:
            inn = str(org.inn) if org.inn else None
            conid = str(org.connection_id) if org.connection_id else None
            if not inn:
                logger.error("Организация %s: отсутствует ИНН", org.name)
                failed += 1
                continue

            specs = [('true_api', None)] if 'true_api' in purposes else []
            if conid and 'suz' in purposes:
                specs.append(('suz', conid))
            for purpose, connection in specs:
                issuance_attempted = False
                try:
                    environment = self.config.get('tokens_environment', 'production')
                    oms = org.oms_id if purpose == 'suz' else None
                    scope = dict(conid=connection, oms_id=oms, environment=environment)
                    token_format = self.tp.true_api_format(inn) if purpose == 'true_api' else 'UUID'
                    remaining = self.tp.remaining_for(inn, purpose, **scope)
                    if remaining is not None and remaining > self.refresh_before_expiry:
                        current += 1
                        logger.info("ИНН %s, %s: актуален, осталось %.0f сек", inn, purpose, remaining)
                        continue
                    # Validate the selected organization/OMS before any issuance.
                    self.tp._scope(inn, purpose, **scope)
                    issuance_attempted = True
                    record = issue_token(inn, purpose=purpose, token_format=token_format,
                                         connection_id=connection, oms_id=oms, config=self.config)
                    self.tp.save_record(record)
                    if self.tp.get_token_value_for(inn, purpose, **scope) != record.token:
                        raise RuntimeError("Saved token is not active in its scope")
                    refreshed += 1
                    logger.info("ИНН %s, %s: проверен и опубликован", inn, purpose)
                except Exception as exc:
                    failed += 1
                    if purpose == 'suz' and issuance_attempted:
                        logger.error("СУЗ: выдача или публикация не подтверждена; прежний токен подключения мог быть отозван")
                    logger.error("ИНН %s, %s: ошибка обновления (%s)", inn, purpose, type(exc).__name__)

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
