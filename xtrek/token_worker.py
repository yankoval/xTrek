"""One-shot master token refresh, suitable for a systemd timer or cron."""
import argparse
import logging
import hashlib
import json
import math
import os
import tempfile
from pathlib import Path

from .tokens import TokenProcessor
from .org_manager import OrganizationManager
from .config_loader import load_config
from .crpt_auth import issue_token
from .token_runtime import current_runtime, master_runtime, MasterStopped, DeadlineExpired

logger = logging.getLogger("TokenWorker")


class TokenRefreshWorker:
    def __init__(self):
        self.config = load_config()
        runtime = current_runtime()
        if runtime:
            runtime.configure(self.config)
        self.scope_seconds = float(self.config.get("tokens_master_scope_seconds", 240))
        self.publish_reserve = float(self.config.get("tokens_master_publish_reserve_seconds", 45))
        if not (math.isfinite(self.scope_seconds) and math.isfinite(self.publish_reserve)
                and 0 < self.publish_reserve < self.scope_seconds
                and self.scope_seconds + 20 < float(self.config.get("tokens_master_cycle_seconds", 420))):
            raise ValueError("Scope and publication budgets must fit the master cycle")
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
        with master_runtime(self.config):
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

        current = refreshed = failed = deferred = 0
        pending = []
        runtime = current_runtime()
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
                    pending.append((inn, purpose, connection, oms, scope, token_format, remaining))
                except Exception as exc:
                    failed += 1
                    logger.error("ИНН %s, %s: ошибка обновления (%s)", inn, purpose, type(exc).__name__)

        if pending:
            path, attempts = self._load_attempts()
            # Unattempted/least recently attempted scopes get a turn before a
            # repeatedly failing issuer. Expiry breaks ties within that queue.
            pending.sort(key=lambda item: (attempts.get(self._attempt_key(item), 0),
                                           item[6] if item[6] is not None else float('-inf')))
        for index, item in enumerate(pending):
            runtime.check()
            if runtime.remaining() < self.scope_seconds + 20:
                deferred = len(pending) - index
                break
            inn, purpose, connection, oms, scope, token_format, _ = item
            attempts[self._attempt_key(item)] = max(attempts.values(), default=0) + 1
            self._save_attempts(path, attempts)
            try:
                with runtime.budget(self.scope_seconds):
                    runtime.assert_owner()
                    with runtime.budget(self.scope_seconds - self.publish_reserve):
                        record = issue_token(inn, purpose=purpose, token_format=token_format,
                                             connection_id=connection, oms_id=oms, config=self.config)
                    runtime.assert_owner()
                    self.tp.save_record(record)
                    if self.tp.get_token_value_for(inn, purpose, **scope) != record.token:
                        raise RuntimeError("Saved token is not active in its scope")
                    refreshed += 1
                    logger.info("ИНН %s, %s: проверен и опубликован", inn, purpose)
            except (Exception, DeadlineExpired) as exc:
                failed += 1
                if purpose == 'suz':
                    logger.error("СУЗ: выдача или публикация не подтверждена; прежний токен подключения мог быть отозван")
                logger.error("ИНН %s, %s: ошибка обновления (%s)", inn, purpose, type(exc).__name__)
                runtime.check()

        logger.info("Цикл завершён: актуальны=%s, обновлены=%s, ошибки=%s, отложены=%s",
                    current, refreshed, failed, deferred)
        return failed == 0 and deferred == 0

    @staticmethod
    def _attempt_key(item):
        return json.dumps(item[:4], separators=(',', ':'))

    def _load_attempts(self):
        registry = self.config.get('tokens_registry_path') or self.config.get('tokens_path', 'default')
        digest = hashlib.sha256(registry.encode()).hexdigest()
        directory = Path(self.config.get('tokens_master_local_lock_dir') or Path.home() / '.cache' / 'xtrek')
        path = directory / ('attempts-' + digest + '.json')
        try:
            data = json.loads(path.read_text())
            if not isinstance(data, dict) or any(not isinstance(v, (int, float)) or not math.isfinite(v)
                                                 for v in data.values()):
                raise ValueError('Invalid attempt state')
        except FileNotFoundError:
            data = {}
        return path, data

    @staticmethod
    def _save_attempts(path, attempts):
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = None
        try:
            with tempfile.NamedTemporaryFile(mode='w', dir=path.parent, delete=False) as stream:
                temporary = stream.name
                json.dump(attempts, stream)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, path)
        finally:
            if temporary and os.path.exists(temporary):
                os.unlink(temporary)


def main(argv=None):
    parser = argparse.ArgumentParser(description="Однократное обновление истекающих токенов в режиме мастера")
    parser.add_argument('--once', action='store_true', help="Выполнить один цикл (поведение по умолчанию)")
    parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
    try:
        with master_runtime():
            return 0 if TokenRefreshWorker().check_and_refresh() else 1
    except (Exception, MasterStopped) as exc:
        logger.error("Цикл обновления токенов не выполнен: %s", exc)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
