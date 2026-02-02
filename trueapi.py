import os
import sys
import requests
import pyperclip
import logging
import urllib3
import argparse
from typing import List, Dict, Any, Generator

# Настройка путей
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    from tokens import TokenProcessor
except ImportError:
    TokenProcessor = None

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("HonestSign")

class HonestSignAPI:
    def __init__(self, token: str = None, omsId: str = None, clientToken: str = None):
        """
        Инициализация True API.
        Обязателен только основной токен. omsId и clientToken — опционально.
        """
        # Токен - единственный обязательный параметр
        self.token = token or os.getenv('HONEST_SIGN_TOKEN')
        if not self.token:
            raise ValueError("Токен не найден (установите HONEST_SIGN_TOKEN или передайте параметром)")

        # omsId - необязателен
        self.omsId = omsId or os.getenv('OMSID')
        if not self.omsId:
            logger.debug("omsId не задан")

        # clientToken - необязателен
        self.clientToken = clientToken or os.getenv('CLIENT_TOKEN')
        if not self.clientToken:
            logger.debug("clientToken не задан")

        self.host = "https://markirovka.crpt.ru"
        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
            "Accept": "application/json"
        }

    def get_balance_all(self) -> List[Dict[str, Any]]:
        """Получить баланс по всем товарным группам"""
        url = f"{self.host}/api/v3/true-api/elk/product-groups/balance/all"
        try:
            logger.info("Запрос баланса по всем ТГ...")
            response = requests.get(url, headers=self.headers, verify=False)
            logger.debug(f"RAW GET | Status: {response.status_code} | Body: {response.text}")
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Ошибка при получении баланса: {e}")
            return []

    def get_single_cis_info(self, code: str) -> Dict[str, Any]:
        """Получить информацию о коде маркировки"""
        url = f"{self.host}/api/v3/true-api/cises/info"
        try:
            response = requests.post(url, json=[code], headers=self.headers, verify=False)
            logger.debug(f"RAW POST | Status: {response.status_code} | Body: {response.text}")
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.warning(f"Ошибка запроса кода {code}: {e}")
            return {"code": code, "error": str(e)}

    def get_codes_from_clipboard(self) -> List[str]:
        clipboard_text = pyperclip.paste()
        return [line.strip() for line in clipboard_text.split('\n') if line.strip()]

    def process_codes_iteratively(self) -> Generator[Dict[str, Any], None, None]:
        for code in self.get_codes_from_clipboard():
            yield self.get_single_cis_info(code)

def main():
    parser = argparse.ArgumentParser(description="True API Честный Знак")
    parser.add_argument('-t', '--token', dest='token', type=str, help='Bearer токен')
    parser.add_argument('-ct', '--client_token', dest='client_token', type=str, help='Client Token (опц.)')
    parser.add_argument('-oid', '--omsid', dest='omsId', type=str, help='OMS ID (опц.)')
    parser.add_argument('--find-token-by-inn', type=str, help='Поиск по ИНН')
    parser.add_argument('--balance', action='store_true', help='Вывести баланс')
    parser.add_argument('--cises', action='store_true', help='Инфо о кодах из буфера')
    parser.add_argument('--debug', action='store_true', help='Включить DEBUG')

    args = parser.parse_args()
    if args.debug:
        logger.setLevel(logging.DEBUG)

    try:
        token, omsId, client_token = args.token, args.omsId, args.client_token
        
        # Автопоиск токена по ИНН
        token_inn = args.find_token_by_inn or os.getenv("FIND_TOKEN_BY_INN")
        if token_inn and TokenProcessor:
            tp = TokenProcessor()
            token_data = tp.get_token_by_inn(token_inn)
            if token_data:
                meta = ['user_status', 'full_name', 'scope', 'inn', 'pid', 'id', 'exp']
                logger.info('Токен: ' + ' '.join([str(token_data.get(k, '-')) for k in meta]))
                token = token_data.get('Токен')
                # Подтягиваем доп. параметры из БД, если они там есть
                omsId = omsId or token_data.get('omsId')
                client_token = client_token or token_data.get('clientToken')

        # Создание API
        api = HonestSignAPI(token=token, omsId=omsId, clientToken=client_token)

        if args.balance:
            res = api.get_balance_all()
            logger.info(f"{'ORG ID':<15} | {'ТГ':<4} | {'БАЛАНС':>12}")
            for item in res:
                b = item.get('balance', 0) / 100
                logger.info(f"{item.get('organisationId', 'N/A'):<15} | {item.get('productGroupId'):<4} | {b:>12.2f}")

        if args.cises:
            for results in api.process_codes_iteratively():
                for r in results:
                    c = r.get('cisInfo', {})
                    logger.info(f"CIS: {c.get('cis')} | Статус: {c.get('status')}")

    except Exception as e:
        logger.error(f"Ошибка выполнения: {e}")

if __name__ == "__main__":
    main()