import os
import requests
import pyperclip
import logging
import urllib3
import argparse
from typing import List, Dict, Any, Generator

# Отключаем предупреждения SSL
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Настройка логгера
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("HonestSign")

class HonestSignAPI:
    def __init__(self, token: str = None):
        self.token = token or os.getenv('HONEST_SIGN_TOKEN')
        if not self.token:
            logger.error("Токен не найден. Установите переменную окружения HONEST_SIGN_TOKEN.")
            raise ValueError("Токен не найден")

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
            logger.info(f"Запрос баланса по всем ТГ...")
            response = requests.get(url, headers=self.headers, verify=False)
            logger.debug(f"RAW GET | Status: {response.status_code} | Body: {response.text}")
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Ошибка при получении баланса: {e}")
            return []

    def get_single_cis_info(self, code: str) -> Dict[str, Any]:
        """Получить информацию об одном коде маркировки"""
        url = f"{self.host}/api/v3/true-api/cises/info"
        try:
            response = requests.post(url, json=[code], headers=self.headers, verify=False)
            logger.debug(f"RAW POST | Status: {response.status_code} | Body: {response.text}")
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.warning(f"Ошибка кода {code}: {e}")
            return {"code": code, "error": str(e)}

    def get_codes_from_clipboard(self) -> List[str]:
        clipboard_text = pyperclip.paste()
        codes = [line.strip() for line in clipboard_text.split('\n') if line.strip()]
        logger.info(f"Извлечено кодов из буфера: {len(codes)}")
        return codes

    def process_codes_iteratively(self) -> Generator[Dict[str, Any], None, None]:
        codes = self.get_codes_from_clipboard()
        for code in codes:
            yield self.get_single_cis_info(code)

def main():
    parser = argparse.ArgumentParser(description="Утилита для работы с Честным Знаком (True API)")
    parser.add_argument('--balance', action='store_true', help='Вывести баланс по всем товарным группам')
    parser.add_argument('--cises', action='store_true', help='Обработать коды маркировки из буфера обмена')
    parser.add_argument('--debug', action='store_true', help='Включить вывод сырых ответов API (DEBUG)')

    args = parser.parse_args()

    if args.debug:
        logger.setLevel(logging.DEBUG)

    try:
        api = HonestSignAPI()

        if not (args.balance or args.cises):
            parser.print_help()
            return

        if args.balance:
            balances = api.get_balance_all()
            if not balances:
                logger.info("Данные о балансе отсутствуют.")
            
            logger.info(f"{'ORG ID':<15} | {'ТГ':<5} | {'Баланс (руб.)':>12}")
            logger.info("-" * 40)
            
            for item in balances:
                org_id = item.get('organisationId', 'N/A')
                pg_id = item.get('productGroupId', '??')
                raw_val = item.get('balance')
                
                if raw_val is not None:
                    balance_str = f"{raw_val / 100:>12.2f}"
                else:
                    balance_str = f"{'[Нет данных]':>12}"
                
                logger.info(f"{org_id:<15} | {pg_id:<5} | {balance_str}")

        if args.cises:
            for results in api.process_codes_iteratively():
                for res in results:
                    c_info = res.get('cisInfo', {})
                    logger.info(f"CIS: {c_info.get('cis')} | Статус: {c_info.get('status')} | Агрегат: {c_info.get('parent', 'Нет')}")

    except Exception as e:
        logger.error(f"Произошла ошибка: {e}")

if __name__ == "__main__":
    main()