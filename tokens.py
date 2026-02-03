import json
import base64
from datetime import datetime
from typing import List, Dict, Any, Optional
import re
from pathlib import Path

home_dir = Path.home()


file_path = Path(home_dir,'tokens.json')
class TokenProcessor:
    """
    Класс для обработки токенов из JSON файла
    """

    def __init__(self, file_path: str = ''):
        """
        Инициализация процессора токенов

        Args:
            file_path (str): Путь к JSON файлу с токенами
        """
        self.file_path = file_path if file_path else Path(home_dir,'tokens.json')
        self.tokens = []
        self.processed_tokens = []
        self.read_tokens_file()
        self.process_tokens()

    def read_tokens_file(self) -> List[Dict[str, Any]]:
        """
        Читает JSON файл с токенами

        Returns:
            List[Dict[str, Any]]: Список токенов из файла

        Raises:
            FileNotFoundError: Если файл не найден
            json.JSONDecodeError: Если некорректный JSON формат
        """
        try:
            with open(self.file_path, 'r', encoding='utf-8-sig') as file:
                data = json.load(file)

            if not isinstance(data, list):
                raise ValueError("JSON файл должен содержать массив объектов")

            self.tokens = data
            return self.tokens

        except FileNotFoundError:
            raise FileNotFoundError(f"Файл не найден: {self.file_path}")
        except json.JSONDecodeError as e:
            raise ValueError(f"Некорректный JSON формат: {e}")

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
                    expiry_date = datetime.fromtimestamp(value)
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
        if not self.tokens:
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
        current_time = datetime.now()

        for token in self.processed_tokens:
            # Проверяем несколько возможных источников информации о сроке действия

            # 1. Проверяем поле 'ДействуетДо'
            expiry_str = token.get('ДействуетДо', '')
            is_active_by_expiry = False

            if expiry_str and expiry_str != '0001-01-01T00:00:00':
                try:
                    expiry_date = datetime.fromisoformat(expiry_str.replace('Z', '+00:00'))
                    if expiry_date >= current_time:
                        is_active_by_expiry = True
                except ValueError:
                    # Если не удалось разобрать дату, пропускаем этот способ проверки
                    pass

            # 2. Проверяем поле 'exp' из JWT (если есть)
            exp_timestamp = token.get('exp_timestamp')
            is_active_by_jwt = False

            if exp_timestamp:
                try:
                    expiry_date = datetime.fromtimestamp(exp_timestamp)
                    if expiry_date >= current_time:
                        is_active_by_jwt = True
                except (ValueError, TypeError):
                    pass

            # 3. Если есть хотя бы один признак активности, считаем токен активным
            if is_active_by_expiry or is_active_by_jwt:
                token['Активен'] = True
                active_tokens.append(token)
            else:
                # Если срок истек или не определен, проверяем по умолчанию
                # Для токенов без указания срока действия считаем их активными
                if not expiry_str or expiry_str == '0001-01-01T00:00:00':
                    if not exp_timestamp:  # И нет JWT exp
                        token['Активен'] = True  # Считаем активным по умолчанию
                        active_tokens.append(token)
                    else:
                        token['Активен'] = False
                else:
                    token['Активен'] = False

        return active_tokens

    def get_token_by_inn(self, inn: str) -> Optional[Dict[str, Any]]:
        """
        Находит токен по полю INN

        Args:
            inn (str): ИНН для поиска

        Returns:
            Optional[Dict[str, Any]]: Найденный токен или None
        """
        if not self.processed_tokens:
            self.process_tokens()

        for token in self.processed_tokens:
            token_inn = token.get('inn')

            # Проверяем совпадение INN (как строка)
            if token_inn and str(token_inn) == str(inn):
                return token

        return None

    def get_tokens_by_inn_list(self, inn_list: List[str]) -> List[Dict[str, Any]]:
        """
        Находит все токены для списка INN

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

        print("=" * 60)
        print("СВОДНАЯ ИНФОРМАЦИЯ О ТОКЕНАХ")
        print("=" * 60)

        # Статистика по типам токенов
        token_types = {}
        for token in self.processed_tokens:
            token_type = token.get('ТипТокена', 'НЕИЗВЕСТНО')
            token_types[token_type] = token_types.get(token_type, 0) + 1

        print(f"\nОбщее количество токенов: {len(self.processed_tokens)}")
        print("\nРаспределение по типам:")
        for token_type, count in token_types.items():
            print(f"  {token_type}: {count}")

        # Активные токены
        active_tokens = self.get_active_tokens()
        print(f"\nАктивных токенов: {len(active_tokens)}")

        # Токены с INN
        tokens_with_inn = [t for t in self.processed_tokens if t.get('inn')]
        print(f"Токенов с INN: {len(tokens_with_inn)}")

        # Уникальные INN
        unique_inns = set(str(t.get('inn')) for t in tokens_with_inn if t.get('inn'))
        print(f"Уникальных INN: {len(unique_inns)}")

        # Сроки действия
        expired_tokens = len(self.processed_tokens) - len(active_tokens)
        print(f"Истекших токенов: {expired_tokens}")

        print("\n" + "=" * 60)

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
            print(f"\n{'='*60}")
            print(f"ТОКЕН #{i}")
            print(f"{'='*60}")

            # Основная информация
            print(f"Идентификатор: {token.get('Идентификатор', 'Нет данных')}")
            print(f"Тип токена: {token.get('ТипТокена', 'Нет данных')}")

            # Сокращенный токен
            token_value = token.get('Токен', '')
            if token_value:
                if len(token_value) > 50:
                    print(f"Токен: {token_value[:50]}...")
                else:
                    print(f"Токен: {token_value}")

            # Срок действия
            expiry_str = token.get('ДействуетДо', '')
            print(f"Действует до: {expiry_str if expiry_str else 'Нет данных'}")

            # Статус активности
            active_tokens = self.get_active_tokens()
            is_active = token in active_tokens
            print(f"Активен: {'ДА' if is_active else 'НЕТ'}")

            # Декодированные поля JWT (если есть)
            if token.get('ТипТокена') == 'JWT':
                print("\nДекодированные поля JWT:")
                print(f"  INN: {token.get('inn', 'Нет данных')}")
                print(f"  Имя: {token.get('full_name', 'Нет данных')}")
                print(f"  Статус пользователя: {token.get('user_status', 'Нет данных')}")
                print(f"  PID: {token.get('pid', 'Нет данных')}")
                print(f"  ID: {token.get('id', 'Нет данных')}")
                print(f"  Срок действия (exp): {token.get('exp', 'Нет данных')}")

                # Scope
                scope = token.get('scope')
                if scope:
                    if isinstance(scope, list) and len(scope) > 0:
                        print(f"  Scope: {', '.join(scope[:3])}{'...' if len(scope) > 3 else ''}")
                    else:
                        print(f"  Scope: {scope}")


# Пример использования
def main():
    # Путь к файлу с токенами
    #file_path = "tokens.json"

    try:
        # Создаем процессор токенов
        processor = TokenProcessor(file_path)

        # 1. Чтение и обработка токенов
        print("Чтение и обработка токенов...")
        processed_tokens = processor.process_tokens()
        print(f"Обработано токенов: {len(processed_tokens)}")

        # 2. Вывод сводной информации
        processor.print_summary()

        # 3. Получение активных токенов
        print("\n" + "="*60)
        print("АКТИВНЫЕ ТОКЕНЫ:")
        print("="*60)

        active_tokens = processor.get_active_tokens()
        for i, token in enumerate(active_tokens, 1):
            print(f"\n{i}. ID: {token.get('Идентификатор')}, "
                  f"INN: {token.get('inn', 'Н/Д')}, "
                  f"Имя: {token.get('full_name', 'Н/Д')}")

        # 4. Поиск токена по INN
        print("\n" + "="*60)
        print("ПОИСК ТОКЕНА ПО INN:")
        print("="*60)

        # Пример поиска по INN из вашего файла
        inn_to_find = "9723161905"
        found_token = processor.get_token_by_inn(inn_to_find)

        if found_token:
            print(f"\nНайден токен для INN {inn_to_find}:")
            print(f"  Идентификатор: {found_token.get('Идентификатор')}")
            print(f"  Имя: {found_token.get('full_name')}")
            print(f"  Активен: {'Да' if found_token in active_tokens else 'Нет'}")
        else:
            print(f"\nТокен для INN {inn_to_find} не найден")

        # 5. Поиск по нескольким INN
        print("\n" + "="*60)
        print("ПОИСК ПО НЕСКОЛЬКИМ INN:")
        print("="*60)

        inn_list = ["9723161905", "9718180660", "несуществующий_инн"]
        tokens_by_inn = processor.get_tokens_by_inn_list(inn_list)

        print(f"\nНайдено токенов для списка INN: {len(tokens_by_inn)}")
        for token in tokens_by_inn:
            print(f"  - INN: {token.get('inn')}, ID: {token.get('Идентификатор')}")

        # 6. Детальная информация о первых N токенах
        print("\n" + "="*60)
        print("ДЕТАЛЬНАЯ ИНФОРМАЦИЯ (первые 3 токена):")
        print("="*60)

        processor.print_detailed_info(max_tokens=3)

    except Exception as e:
        print(f"Ошибка при обработке токенов: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()