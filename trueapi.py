import os
import requests
from requests import HTTPError
import pyperclip
import json
from typing import List, Dict, Any, Generator


class HonestSignAPI:
    def __init__(self, token: str = None):
        self.token = token or os.getenv('HONEST_SIGN_TOKEN')
        if not self.token:
            raise ValueError("Токен не найден")

        self.base_url = "https://markirovka.crpt.ru/api/v3/true-api/cises/info"
        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }

    def get_codes_from_clipboard(self) -> List[str]:
        """Получить коды маркировки из буфера обмена"""
        clipboard_text = pyperclip.paste()
        return [line.strip() for line in clipboard_text.split('\n') if line.strip()]

    def get_single_cis_info(self, code: str) -> Dict[str, Any]:
        """Получить информацию об одном коде маркировки"""
        try:
            response = requests.post(self.base_url, json=[code], headers=self.headers)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            return {"code": code, "error": f"Ошибка запроса: {e}"}

    def process_codes_iteratively(self) -> Generator[Dict[str, Any], None, None]:
        """Обработать коды по одному и вернуть генератор"""
        codes = self.get_codes_from_clipboard()

        for code in codes:
            yield self.get_single_cis_info(code)


# Использование
if __name__ == "__main__":
    try:
        api = HonestSignAPI()

        # Обработка через for-as
        res, i = [],0
        for result in api.process_codes_iteratively():
            for r in result:
                if r.get('cisInfo').get('parent'):
                    res.append(r)
                    print(r.get('cisInfo').get('parent'))
            print(f"{i}")  # Разделитель между результатами
            i+=1
        print('\n'.join([r.get('cisInfo').get('parent') for r in res]))

    except ValueError as e:
        print(f"Ошибка: {e}")
    except HTTPError as e:
        print(e.response.text)
        
    except Exception as e:
        print(f"Неожиданная ошибка: {e}")