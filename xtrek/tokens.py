import json
import base64
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional
import re
import os
import logging
import tempfile
import threading
from pathlib import Path
from zoneinfo import ZoneInfo
from .org_manager import OrganizationManager
from .storage import get_storage
from .config_loader import load_config

# Настройка логирования
logger = logging.getLogger("TokenProcessor")

home_dir = Path.home()


file_path = Path(home_dir,'tokens.json')
class TokenProcessor:
    """
    Класс для обработки токенов из JSON файла
    """

    _command_snapshots = {}
    _snapshot_lock = threading.Lock()

    def __init__(self, file_path: str = '', orgs_dir: str = 'my_orgs', org_manager: Optional[OrganizationManager] = None,
                 tokens_read_only: Optional[bool] = None):
        """
        Инициализация процессора токенов

        Args:
            file_path (str): Путь к JSON файлу с токенами
            orgs_dir (str): Путь к директории с организациями
            org_manager (OrganizationManager, optional): Существующий менеджер организаций
        """
        self.config = load_config()

        self.s3_config = self.config.get('s3_config')
        self.tokens_path = self.config.get('tokens_path')
        self.file_path = file_path if file_path else Path(home_dir, 'tokens.json')
        configured_read_only = self.config.get('tokens_read_only', True)
        if tokens_read_only is None:
            tokens_read_only = configured_read_only
        if isinstance(tokens_read_only, str):
            tokens_read_only = tokens_read_only.strip().lower() not in {'0', 'false', 'no', 'off'}
        self.tokens_read_only = bool(tokens_read_only)

        self.tokens = []
        self.processed_tokens = []
        self._tokens_loaded = False
        # Сначала инициализируем менеджер организаций, так как он может понадобиться при обработке токенов
        if org_manager:
            self.org_manager = org_manager
        else:
            if not os.path.isabs(orgs_dir):
                base_path = os.path.dirname(os.path.abspath(__file__))
                orgs_dir = os.path.join(base_path, orgs_dir)
            self.org_manager = OrganizationManager(orgs_dir)

        logger.debug(
            "Конфигурация TokenProcessor: tokens_path=%s, s3_configured=%s, tokens_read_only=%s",
            self.tokens_path, bool(self.s3_config), self.tokens_read_only,
        )

        if self.tokens_path and self.tokens_path.startswith('s3://'):
            self.storage = get_storage(self.tokens_path, self.s3_config)
            logger.debug("Инициализировано S3-хранилище токенов")
            if self.tokens_read_only:
                self._load_command_snapshot()
            else:
                self._sync_from_s3(required=False)
        else:
            self.storage = None
            if self.tokens_read_only:
                raise RuntimeError("В клиентском режиме tokens_path должен указывать на объект S3")
            logger.debug("Используется локальное хранилище токенов")

        # Если данные еще не загружены (например, не было S3 синхронизации), загружаем сейчас
        if not self.processed_tokens:
            self.read_tokens_file()
            self.process_tokens()

    @staticmethod
    def _validate_tokens(data):
        if not isinstance(data, list):
            raise ValueError("tokens.json должен содержать JSON-массив")
        for index, item in enumerate(data):
            if not isinstance(item, dict) or not isinstance(item.get('Токен'), str) or not item['Токен']:
                raise ValueError(f"Некорректная запись tokens.json с индексом {index}")
        return data

    def _download_tokens(self):
        if not self.storage or not self.tokens_path:
            raise RuntimeError("S3-хранилище токенов не настроено")
        temporary_path = None
        try:
            with tempfile.NamedTemporaryFile(prefix='xtrek-tokens-', suffix='.json', delete=False) as tmp:
                temporary_path = Path(tmp.name)
            self.storage.download(self.tokens_path, temporary_path)
            with open(temporary_path, 'r', encoding='utf-8-sig') as file:
                data = json.load(file)
            return self._validate_tokens(data)
        finally:
            if temporary_path:
                temporary_path.unlink(missing_ok=True)

    def _apply_tokens(self, data):
        self.tokens = [item.copy() for item in data]
        self._tokens_loaded = True
        self.process_tokens()

    def _load_command_snapshot(self, force=False):
        """Загружает единый снимок S3 на время текущего процесса CLI."""
        snapshot_key = str(self.tokens_path)
        try:
            with self._snapshot_lock:
                if force or snapshot_key not in self._command_snapshots:
                    self._command_snapshots[snapshot_key] = self._download_tokens()
                snapshot = self._command_snapshots[snapshot_key]
            self._apply_tokens(snapshot)
            logger.debug("Снимок токенов загружен из S3")
        except Exception as exc:
            logger.error("Не удалось получить актуальные токены из S3: %s", exc)
            raise RuntimeError("Актуальные токены из S3 недоступны; выполнение xTrek запрещено") from exc

    def refresh_from_source(self):
        """Принудительно перечитывает S3, например после отказа авторизации."""
        if self.tokens_read_only:
            self._load_command_snapshot(force=True)
            logger.info("Токены повторно загружены из S3 после отказа авторизации")
        else:
            self._sync_from_s3(required=True)

    def refresh_token_value(self, inn: str, token_type: str = 'JWT', conid: Optional[str] = None) -> str:
        """Перечитывает источник и возвращает заменившийся активный токен."""
        self.refresh_from_source()
        token = self.get_token_value_by_inn(inn, token_type=token_type, conid=conid)
        if not token:
            raise RuntimeError(f"Активный токен типа {token_type} для ИНН {inn} отсутствует в S3")
        return token

    def _sync_from_s3(self, required=False):
        if not self.storage or not self.tokens_path:
            if required:
                raise RuntimeError("S3-хранилище токенов не настроено")
            return False
        try:
            data = self._download_tokens()
            self._apply_tokens(data)
            logger.debug("Токены загружены из S3")
            return True
        except Exception as exc:
            if required:
                logger.error("Не удалось получить актуальные токены из S3: %s", exc)
                raise RuntimeError("Актуальные токены из S3 недоступны") from exc
            logger.debug("Серверный режим не загрузил tokens.json из S3: %s", exc)
            return False

    def _sync_to_s3(self):
        if self.tokens_read_only:
            raise PermissionError("Публикация токенов запрещена в клиентском режиме")
        if not self.storage or not self.tokens_path:
            return
        try:
            self.storage.upload(self.file_path, self.tokens_path)
        except Exception as exc:
            logger.error("Новый комплект токенов не опубликован в S3: %s", exc)
            raise RuntimeError("Не удалось опубликовать tokens.json в S3") from exc

    def _write_tokens_file_atomic(self):
        target = Path(self.file_path)
        target.parent.mkdir(parents=True, exist_ok=True)
        temporary_path = None
        try:
            with tempfile.NamedTemporaryFile(
                mode='w', encoding='utf-8', dir=target.parent,
                prefix=f'.{target.name}.', suffix='.tmp', delete=False,
            ) as tmp:
                json.dump(self.tokens, tmp, indent=4, ensure_ascii=False)
                tmp.flush()
                os.fsync(tmp.fileno())
                temporary_path = Path(tmp.name)
            os.replace(temporary_path, target)
            temporary_path = None
        finally:
            if temporary_path:
                temporary_path.unlink(missing_ok=True)

    def _maybe_sync_from_s3(self, force=False):
        """Совместимый вызов: клиент уже использует снимок команды, сервер синхронизируется явно."""
        if force:
            self.refresh_from_source()

    def get_jwt_token_value_by_inn(self, inn: str) -> Optional[str]:
        """Обертка для получения JWT токена по ИНН"""
        return self.get_token_value_by_inn(inn, token_type='JWT')

    def get_uuid_token_value_by_inn(self, inn: str) -> Optional[str]:
        """Обертка для получения UUID токена по ИНН"""
        return self.get_token_value_by_inn(inn, token_type='UUID')

    def get_token_remaining_seconds(self, inn: str, token_type: str = 'JWT', conid: Optional[str] = None) -> Optional[float]:
        """Возвращает оставшееся время лучшего токена; None означает отсутствие срока."""
        normalized_type = 'UUID' if token_type.lower() in {'auth', 'uuid'} else token_type.upper()
        candidates = [
            token for token in self.processed_tokens
            if str(token.get('inn', '')) == str(inn)
            and token.get('ТипТокена') == normalized_type
            and (conid is None or str(token.get('Идентификатор')) == str(conid))
        ]
        expiries = []
        for token in candidates:
            try:
                expiry = self._token_expiry(token)
            except (ValueError, TypeError, OverflowError):
                continue
            if expiry is not None:
                expiries.append(expiry)
        if not expiries:
            return None
        return (max(expiries) - datetime.now(timezone.utc)).total_seconds()

    def get_token_value_by_inn(self, inn: str, token_type: str = 'JWT', conid: Optional[str] = None) -> Optional[str]:
        """Возвращает строку активного токена из снимка текущей команды."""
        return self._find_active_token(inn, token_type, conid)

    @staticmethod
    def _parse_expiry(value: str) -> Optional[datetime]:
        if not value or value == '0001-01-01T00:00:00':
            return None
        parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
        if parsed.tzinfo is None:
            # Исторический tokens.json сохраняет московское локальное время без offset.
            parsed = parsed.replace(tzinfo=ZoneInfo('Europe/Moscow'))
        return parsed.astimezone(timezone.utc)

    def _token_expiry(self, token: Dict[str, Any]) -> Optional[datetime]:
        # Для JWT только claim exp является авторитетным. Поле ДействуетДо могло
        # быть записано в локальном времени и не должно продлевать JWT.
        if token.get('ТипТокена') == 'JWT':
            exp_timestamp = token.get('exp_timestamp')
            if exp_timestamp is None:
                return None
            return datetime.fromtimestamp(float(exp_timestamp), tz=timezone.utc)
        return self._parse_expiry(token.get('ДействуетДо', ''))

    def _is_token_active(self, token: Dict[str, Any], current_time: Optional[datetime] = None) -> bool:
        current_time = current_time or datetime.now(timezone.utc)
        try:
            expiry = self._token_expiry(token)
        except (ValueError, TypeError, OverflowError):
            return False
        return expiry is not None and expiry >= current_time

    def _find_active_token(self, inn: str, token_type: str = 'JWT', conid: Optional[str] = None) -> Optional[str]:
        """Внутренний метод для поиска активного токена в памяти"""
        # Синонимы для UUID
        if token_type in ['auth', 'uuid']:
            token_type = 'UUID'

        # Получаем все токены для данного ИНН (из уже загруженных в память)
        inn_set = {str(inn)}
        tokens = [t for t in self.processed_tokens if t.get('inn') and str(t.get('inn')) in inn_set]

        if not tokens:
            return None

        # Фильтруем по типу
        tokens_of_type = [t for t in tokens if t.get('ТипТокена') == token_type]
        if not tokens_of_type:
            return None

        # Если указан conid, фильтруем по нему (Идентификатор)
        if conid:
            tokens_of_type = [t for t in tokens_of_type if str(t.get('Идентификатор')) == str(conid)]
            if not tokens_of_type:
                return None

        # Фильтруем активные токены (вычисляем активность на месте)
        active_tokens_of_type = []
        for t in tokens_of_type:
            if self._is_token_active(t):
                active_tokens_of_type.append(t)

        if not active_tokens_of_type:
            return None

        # Если нашли активные токены, выбираем самый свежий по 'ДействуетДо'
        if len(active_tokens_of_type) > 1:
            try:
                active_tokens_of_type.sort(key=lambda x: self._token_expiry(x) or datetime.min.replace(tzinfo=timezone.utc), reverse=True)
            except Exception:
                pass

        return active_tokens_of_type[0].get('Токен')
        
    def read_tokens_file(self) -> List[Dict[str, Any]]:
        """
        Читает JSON файл с токенами. Если файл не найден или пуст, инициализирует пустой список.

        Returns:
            List[Dict[str, Any]]: Список токенов из файла
        """
        if self.tokens_read_only:
            return self.tokens
        try:
            # Преобразуем в Path если это строка
            p = Path(self.file_path)
            if not p.exists():
                self.tokens = []
                self._tokens_loaded = True
                return self.tokens

            with open(p, 'r', encoding='utf-8-sig') as file:
                content = file.read().strip()
                if not content:
                    self.tokens = []
                    self._tokens_loaded = True
                    return self.tokens
                data = json.loads(content)

            if not isinstance(data, list):
                self.tokens = []
                self._tokens_loaded = True
                return self.tokens

            self.tokens = data
            self._tokens_loaded = True
            return self.tokens

        except Exception:
            self.tokens = []
            self._tokens_loaded = True
            return self.tokens

    def _is_jwt_token(self, token: str) -> bool:
        """
        Определяет, является ли токен JWT

        Args:
            token (str): Значение токена

        Returns:
            bool: True если JWT, False если нет
        """
        if not token or not isinstance(token, str):
            return False

        # JWT обычно начинается с 'eyJ' (base64url encoded JSON)
        # и имеет структуру header.payload.signature разделенную точками
        return (token.startswith('eyJ') and
                len(token.split('.')) == 3 and
                len(token) > 100)

    def _is_uuid_token(self, token: str) -> bool:
        """
        Определяет, является ли токен UUID формата

        Args:
            token (str): Значение токена

        Returns:
            bool: True если UUID, False если нет
        """
        if not token or not isinstance(token, str):
            return False

        # Проверяем формат UUID: 8-4-4-4-12 hex цифры
        uuid_pattern = r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        return bool(re.match(uuid_pattern, token.lower()))

    def _decode_jwt_payload(self, token: str) -> Dict[str, Any]:
        """
        Декодирует payload часть JWT токена

        Args:
            token (str): JWT токен

        Returns:
            Dict[str, Any]: Декодированный payload

        Raises:
            ValueError: Если токен не может быть декодирован
        """
        try:
            # Разделяем токен на части
            parts = token.split('.')
            if len(parts) != 3:
                raise ValueError("Неверный формат JWT токена")

            # Декодируем payload (вторая часть)
            payload_encoded = parts[1]

            # Добавляем padding если необходимо для корректного base64 декодирования
            padding = 4 - len(payload_encoded) % 4
            if padding != 4:
                payload_encoded += '=' * padding

            # Декодируем из base64url
            payload_decoded = base64.urlsafe_b64decode(payload_encoded)

            # Преобразуем из JSON
            payload = json.loads(payload_decoded)

            return payload

        except Exception as e:
            raise ValueError(f"Ошибка декодирования JWT токена: {e}")

    def _extract_jwt_fields(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Извлекает нужные поля из декодированного JWT payload

        Args:
            payload (Dict[str, Any]): Декодированный JWT payload

        Returns:
            Dict[str, Any]: Извлеченные поля
        """
        extracted_fields = {}

        # Извлекаем нужные поля с проверкой на существование
        fields_to_extract = [
            'user_status', 'full_name', 'scope', 'inn', 'pid', 'id', 'exp'
        ]

        for field in fields_to_extract:
            value = payload.get(field)

            # Для некоторых полей может потребоваться дополнительная обработка
            if field == 'scope' and isinstance(value, list):
                # scope часто хранится как список
                extracted_fields[field] = value
            elif field == 'exp' and value:
                # Преобразуем timestamp в читаемую дату
                try:
                    expiry_date = datetime.fromtimestamp(value, tz=timezone.utc)
                    extracted_fields[field] = expiry_date.isoformat()
                    extracted_fields['exp_timestamp'] = value  # Сохраняем и оригинальный timestamp
                except (ValueError, TypeError):
                    extracted_fields[field] = str(value)
            else:
                extracted_fields[field] = value

        return extracted_fields

    def process_tokens(self) -> List[Dict[str, Any]]:
        """
        Обрабатывает все токены: определяет тип, декодирует JWT и добавляет поля

        Returns:
            List[Dict[str, Any]]: Обработанные токены с дополнительными полями
        """
        if not self._tokens_loaded:
            self.read_tokens_file()

        self.processed_tokens = []

        for token_data in self.tokens:
            # Создаем копию исходных данных
            processed_token = token_data.copy()

            # Получаем значение токена
            token_value = token_data.get('Токен', '')

            # Определяем тип токена
            if self._is_jwt_token(token_value):
                token_type = 'JWT'
                processed_token['ТипТокена'] = token_type

                try:
                    # Декодируем JWT payload
                    payload = self._decode_jwt_payload(token_value)

                    # Извлекаем нужные поля
                    jwt_fields = self._extract_jwt_fields(payload)

                    # Добавляем извлеченные поля в структуру токена
                    processed_token.update(jwt_fields)

                    # Добавляем сам payload для возможного дальнейшего использования
                    processed_token['_jwt_payload'] = payload

                except ValueError as e:
                    # Если не удалось декодировать, сохраняем ошибку
                    processed_token['ТипТокена'] = 'JWT (ошибка декодирования)'
                    processed_token['ОшибкаДекодирования'] = str(e)

            elif self._is_uuid_token(token_value):
                token_type = 'UUID'
                processed_token['ТипТокена'] = token_type

                # Если ИНН нет, пробуем найти через OrganizationManager по Идентификатору (connection_id)
                if not processed_token.get('inn'):
                    identifier = processed_token.get('Идентификатор')
                    if identifier:
                        org = self.org_manager.find(connection_id=str(identifier))
                        if org and org.inn:
                            processed_token['inn'] = org.inn

            else:
                token_type = 'НЕИЗВЕСТНО'
                processed_token['ТипТокена'] = token_type

            self.processed_tokens.append(processed_token)

        return self.processed_tokens

    def get_active_tokens(self) -> List[Dict[str, Any]]:
        """
        Возвращает список активных (не истекших) токенов

        Returns:
            List[Dict[str, Any]]: Список активных токенов
        """
        if not self.processed_tokens:
            self.process_tokens()

        active_tokens = []
        current_time = datetime.now(timezone.utc)

        for token in self.processed_tokens:
            if self._is_token_active(token, current_time):
                token['Активен'] = True
                active_tokens.append(token)
            else:
                token['Активен'] = False

        return active_tokens

    def get_token_by_inn(self, inn: str) -> Optional[Dict[str, Any]]:
        """
        Находит токен по полю ИНН. Предпочтение отдается активным токенам.
        Если активный токен не найден, пытается синхронизироваться с S3.

        Args:
            inn (str): ИНН для поиска

        Returns:
            Optional[Dict[str, Any]]: Найденный токен или None
        """
        token = self._find_best_token_in_memory(inn)
        return token

    def _find_best_token_in_memory(self, inn: str) -> Optional[Dict[str, Any]]:
        """Внутренний метод для поиска активного токена в памяти."""
        if not self.processed_tokens:
            self.process_tokens()

        active_tokens = self.get_active_tokens()
        for token in active_tokens:
            token_inn = token.get('inn')
            if token_inn and str(token_inn) == str(inn):
                return token

        return None

    def get_tokens_by_inn_list(self, inn_list: List[str]) -> List[Dict[str, Any]]:
        """
        Находит все токены для списка ИНН

        Args:
            inn_list (List[str]): Список ИНН для поиска

        Returns:
            List[Dict[str, Any]]: Список найденных токенов
        """
        if not self.processed_tokens:
            self.process_tokens()

        found_tokens = []
        inn_set = set(str(inn) for inn in inn_list)

        for token in self.processed_tokens:
            token_inn = token.get('inn')
            if token_inn and str(token_inn) in inn_set:
                found_tokens.append(token)

        return found_tokens

    def print_summary(self) -> None:
        """
        Выводит сводную информацию о токенах
        """
        if not self.processed_tokens:
            self.process_tokens()

        logger.info("=" * 60)
        logger.info("СВОДНАЯ ИНФОРМАЦИЯ О ТОКЕНАХ")
        logger.info("=" * 60)

        # Статистика по типам токенов
        token_types = {}
        for token in self.processed_tokens:
            token_type = token.get('ТипТокена', 'НЕИЗВЕСТНО')
            token_types[token_type] = token_types.get(token_type, 0) + 1

        logger.info(f"Общее количество токенов: {len(self.processed_tokens)}")
        logger.info("Распределение по типам:")
        for token_type, count in token_types.items():
            logger.info(f"  {token_type}: {count}")

        # Активные токены
        active_tokens = self.get_active_tokens()
        logger.info(f"Активных токенов: {len(active_tokens)}")

        # Токены с INN
        tokens_with_inn = [t for t in self.processed_tokens if t.get('inn')]
        logger.info(f"Токенов с ИНН: {len(tokens_with_inn)}")

        # Уникальные INN
        unique_inns = set(str(t.get('inn')) for t in tokens_with_inn if t.get('inn'))
        logger.info(f"Уникальных ИНН: {len(unique_inns)}")

        # Сроки действия
        expired_tokens = len(self.processed_tokens) - len(active_tokens)
        logger.info(f"Истекших токенов: {expired_tokens}")

        logger.info("=" * 60)

    def save_token(self, token_value: str, conid: Optional[str] = None):
        """
        Сохраняет или обновляет токен в базе tokens.json.
        При сохранении удаляет все старые токены для того же ИНН и типа (и conid для UUID).
        """
        if self.tokens_read_only:
            logger.warning("Запрещённая попытка сохранить токен в клиентском режиме")
            raise PermissionError("save_token() запрещён при tokens_read_only=true")

        # 1. Синхронизация перед сохранением для получения актуального состояния
        if self.storage and self.tokens_path:
            # S3 — источник истины. Нельзя публиковать поверх него состояние,
            # если актуальный объект перед изменением прочитать не удалось.
            self._sync_from_s3(required=True)
        else:
            self.read_tokens_file()
            self.process_tokens()

        now = datetime.now(ZoneInfo('Europe/Moscow')).replace(tzinfo=None)
        # Формат 2026-04-07T17:07:12
        start_time = now.strftime("%Y-%m-%dT%H:%M:%S")
        end_time = (now + timedelta(hours=10)).strftime("%Y-%m-%dT%H:%M:%S")

        # Определяем параметры нового токена
        new_token_type = 'НЕИЗВЕСТНО'
        new_token_inn = None
        new_token_identifier = None

        if self._is_jwt_token(token_value):
            new_token_type = 'JWT'
            try:
                payload = self._decode_jwt_payload(token_value)
                fields = self._extract_jwt_fields(payload)
                new_token_inn = str(fields.get('inn')) if fields.get('inn') else None
                new_token_identifier = str(fields.get('pid')) if fields.get('pid') else None
            except Exception as e:
                logger.error(f"Ошибка при разборе нового JWT: {e}")
        elif self._is_uuid_token(token_value):
            new_token_type = 'UUID'
            new_token_identifier = str(conid) if conid else None
            if new_token_identifier:
                org = self.org_manager.find(connection_id=new_token_identifier)
                if org and org.inn:
                    new_token_inn = str(org.inn)

        if not new_token_identifier:
            logger.warning("Не удалось определить идентификатор для сохранения токена.")
            return

        new_entry = {
            "Идентификатор": new_token_identifier,
            "Токен": token_value,
            "ДействуетС": start_time,
            "ДействуетДо": end_time,
            "ТокенОбновления": ""
        }

        # Определяем токены для удаления
        tokens_to_remove_values = set()
        for token_data in self.processed_tokens:
            should_remove = False
            token_inn = str(token_data.get('inn')) if token_data.get('inn') else None
            token_type = token_data.get('ТипТокена')
            token_id = str(token_data.get('Идентификатор'))
            token_value = token_data.get('Токен')

            # Логика удаления:
            if token_inn and new_token_inn and token_inn == new_token_inn and token_type == new_token_type:
                if new_token_type == 'JWT':
                    # Для JWT удаляем всё с тем же ИНН
                    should_remove = True
                elif new_token_type == 'UUID' and token_id == new_token_identifier:
                    # Для UUID (Auth) удаляем с тем же ИНН и тем же Conid
                    should_remove = True
            elif token_id == new_token_identifier:
                # Если ИНН не определен, удаляем по Идентификатору (старое поведение)
                should_remove = True

            if should_remove and token_value:
                tokens_to_remove_values.add(token_value)

        # Фильтруем оригинальный список self.tokens
        self.tokens = [t for t in self.tokens if t.get('Токен') not in tokens_to_remove_values]

        self.tokens.append(new_entry)

        try:
            self._write_tokens_file_atomic()
            self._sync_to_s3()
            logger.info("Новый токен сохранён и комплект tokens.json опубликован в S3")
        except Exception as e:
            logger.error("Ошибка сохранения нового комплекта токенов: %s", e)
            raise

        # Обновляем внутреннее состояние
        self.process_tokens()

    def print_detailed_info(self, max_tokens: int = None) -> None:
        """
        Выводит детальную информацию о токенах

        Args:
            max_tokens (int, optional): Максимальное количество токенов для вывода
        """
        if not self.processed_tokens:
            self.process_tokens()

        tokens_to_display = self.processed_tokens
        if max_tokens and max_tokens < len(tokens_to_display):
            tokens_to_display = tokens_to_display[:max_tokens]

        for i, token in enumerate(tokens_to_display, 1):
            logger.info(f"{'='*60}")
            logger.info(f"ТОКЕН #{i}")
            logger.info(f"{'='*60}")

            # Основная информация
            logger.info(f"Идентификатор: {token.get('Идентификатор', 'Нет данных')}")
            logger.info(f"Тип токена: {token.get('ТипТокена', 'Нет данных')}")

            # Значение токена намеренно не журналируется.

            # Срок действия
            expiry_str = token.get('ДействуетДо', '')
            logger.info(f"Действует до: {expiry_str if expiry_str else 'Нет данных'}")

            # Статус активности
            active_tokens = self.get_active_tokens()
            is_active = token in active_tokens
            logger.info(f"Активен: {'ДА' if is_active else 'НЕТ'}")

            # Декодированные поля JWT (если есть)
            if token.get('ТипТокена') == 'JWT':
                logger.info("Декодированные поля JWT:")
                logger.info(f"  INN: {token.get('inn', 'Нет данных')}")
                logger.info(f"  Имя: {token.get('full_name', 'Нет данных')}")
                logger.info(f"  Статус пользователя: {token.get('user_status', 'Нет данных')}")
                logger.info(f"  PID: {token.get('pid', 'Нет данных')}")
                logger.info(f"  ID: {token.get('id', 'Нет данных')}")
                logger.info(f"  Срок действия (exp): {token.get('exp', 'Нет данных')}")

                # Scope
                scope = token.get('scope')
                if scope:
                    if isinstance(scope, list) and len(scope) > 0:
                        logger.info(f"  Scope: {', '.join(scope[:3])}{'...' if len(scope) > 3 else ''}")
                    else:
                        logger.info(f"  Scope: {scope}")


# Пример использования
def main():
    # Настройка логирования для примера
    logging.basicConfig(level=logging.INFO)

    # Путь к файлу с токенами
    #file_path = "tokens.json"

    try:
        # Создаем процессор токенов
        processor = TokenProcessor(file_path)

        # 1. Чтение и обработка токенов
        logger.info("Чтение и обработка токенов...")
        processed_tokens = processor.process_tokens()
        logger.info(f"Обработано токенов: {len(processed_tokens)}")

        # 2. Вывод сводной информации
        processor.print_summary()

        # 3. Получение активных токенов
        logger.info("АКТИВНЫЕ ТОКЕНЫ:")

        active_tokens = processor.get_active_tokens()
        for i, token in enumerate(active_tokens, 1):
            logger.info(f"{i}. ID: {token.get('Идентификатор')}, "
                  f"INN: {token.get('inn', 'Н/Д')}, "
                  f"Имя: {token.get('full_name', 'Н/Д')}")

        # 4. Поиск токена по INN
        logger.info("ПОИСК ТОКЕНА ПО INN:")

        # Пример поиска по INN из вашего файла
        inn_to_find = "9723161905"
        found_token = processor.get_token_by_inn(inn_to_find)

        if found_token:
            logger.info(f"Найден токен для ИНН {inn_to_find}:")
            logger.info(f"  Идентификатор: {found_token.get('Идентификатор')}")
            logger.info(f"  Имя: {found_token.get('full_name')}")
            logger.info(f"  Активен: {'Да' if found_token in active_tokens else 'Нет'}")
        else:
            logger.info(f"Токен для ИНН {inn_to_find} не найден")

        # 5. Поиск по нескольким INN
        logger.info("ПОИСК ПО НЕСКОЛЬКИМ INN:")

        inn_list = ["9723161905", "9718180660", "несуществующий_инн"]
        tokens_by_inn = processor.get_tokens_by_inn_list(inn_list)

        logger.info(f"Найдено токенов для списка ИНН: {len(tokens_by_inn)}")
        for token in tokens_by_inn:
            logger.info(f"  - INN: {token.get('inn')}, ID: {token.get('Идентификатор')}")

        # 6. Детальная информация о первых N токенах
        logger.info("ДЕТАЛЬНАЯ ИНФОРМАЦИЯ (первые 3 токена):")

        processor.print_detailed_info(max_tokens=3)

    except Exception as e:
        logger.error(f"Ошибка при обработке токенов: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
