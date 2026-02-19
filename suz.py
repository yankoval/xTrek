#
#       suz модуль для обмена с суз честный знак
#
import os
import logging
import sys
#from tqdm import tqdm
#from tqdm.contrib.logging import logging_redirect_tqdm
import argparse
from pathlib import Path
import textwrap
from typing import Any

import coloredlogs
import requests
from requests import HTTPError
#import pyperclip
import json
from typing import List, Dict, Any, Generator

# logging
logger = logging.getLogger(__name__)
coloredlogs.install(level=logging.DEBUG, logger=logger, isatty=True,
                    fmt="%(asctime)s %(levelname)-8s %(message)s",
                    stream=sys.stderr,
                    datefmt='%Y-%m-%d %H:%M:%S')


class SUZ:
    def __init__(self, token: str = None, omsId: str = None, clientToken: str = None):
        self.token = token or os.getenv('HONEST_SIGN_TOKEN')
        if not self.token:
            raise ValueError("Токен не найден")
        self.omsId = omsId or os.getenv('OMSID')
        if not self.omsId:
            raise ValueError("omsId не найден")
        self.clientToken = clientToken or os.getenv('CLIENT_TOKEN')
        if not self.clientToken:
            raise ValueError("omsId не найден")

        self.base_url = f"https://suzgrid.crpt.ru/"  # order/list?omsId={omsId}
        self.headers = {
            "clientToken": f"{self.clientToken}",
            "Accept": "application/json"  # Accept: application/json
        }

    def order_list(self):
        response = requests.get(self.base_url + f'api/v3/order/list?omsId={self.omsId}',
                                headers=self.headers, verify=False )  # json=["0104670404500312215'!,4L"],
        response.raise_for_status()
        return response.json()
    def order_status(self,orderId:str, gtin:str):

        response = requests.get(self.base_url + f'api/v3/order/status?omsId={self.omsId}'
                                                f'&orderId={orderId}'
                                                f'&gtin={gtin}',
                                headers=self.headers, verify=False)  # json=["0104670404500312215'!,4L"],
        response.raise_for_status()
        return response.json()

    def codes(self, orderId: str, quantity: int, gtin: str):

        response = requests.get(self.base_url + f'api/v3/codes?omsId={self.omsId}'
                                                f'&orderId={orderId}'
                                                f'&quantity={quantity}'
                                                f'&gtin={gtin}',
                                headers=self.headers, verify=False)  # json=["0104670404500312215'!,4L"],
        response.raise_for_status()
        return response.json()

    def order_codes_retry(self, blockId: str):
        """ Метод «Получить повторно коды маркировки из заказа КМ»
        Этот метод используется для повторного получения массива эмитированных КМ из 
        заказа кодов маркировки в случае, если коды маркировки не были получены в результате 
        коммуникационных ошибок или ошибок на стороне Системы, взаимодействующей с СУЗ. 
        Метод использует следующие параметры: идентификатор СУЗ, идентификатор 
        пакета кодов маркировки.
        . Ограничения (Restrictions)
        Повторно коды маркировки могут быть запрошены только в том случае, если:
        1) они были ранее запрошены через API;
        2) заказ кодов маркировки не был закрыт.
        """
        response = requests.get(self.base_url + f'api/v3/order/codes/retry?omsId={self.omsId}'
                                                f'&blockId={blockId}',
                                headers=self.headers, verify=False)  # json=["0104670404500312215'!,4L"],
        response.raise_for_status()
        return response.json()

    def order_codes_blocks(self,orderId:str, gtin:str, verify=False):

        response = requests.get(self.base_url + f'api/v3/order/codes/blocks?omsId={self.omsId}'
                                                f'&orderId={orderId}'
                                                f'&gtin={gtin}',
                                headers=self.headers)  # json=["0104670404500312215'!,4L"],
        response.raise_for_status()
        return response.json()

    def providers(self):
        response = requests.get(self.base_url + f'api/v3/providers?omsId={self.omsId}',
                                headers=self.headers, verify=False)  # json=["0104670404500312215'!,4L"],
        response.raise_for_status()
        return response.json()
    def validate_order_body(self, body_data: dict) -> bool:
        """
        Валидация тела запроса для создания заказа
        """
        required_fields = [
            "productGroup",
            "products"
        ]

        for field in required_fields:
            if field not in body_data:
                logger.error(f"Отсутствует обязательное поле: {field}")
                return False

        # Проверка продуктов
        if not isinstance(body_data.get("products"), list) or len(body_data["products"]) == 0:
            logger.error("Поле 'products' должно быть непустым списком")
            return False

        # Проверка каждого продукта
        for i, product in enumerate(body_data["products"]):
            product_required = ["gtin", "quantity", "serialNumberType"]
            for field in product_required:
                if field not in product:
                    logger.error(f"Продукт #{i}: отсутствует обязательное поле: {field}")
                    return False

        return True
    def order_create(self, body_file: str, signature_file: str, max_retries: int = 3):
        """
        Создание заказа на эмиссию кодов

        Args:
            body_file: путь к файлу с телом запроса (JSON)
            signature_file: путь к файлу с подписью
            max_retries: максимальное количество повторных попыток при ошибке 503
        """
        import time
        import re

        # Чтение тела запроса из файла
        if not os.path.exists(body_file):
            raise FileNotFoundError(f"Файл с телом запроса не найден: {body_file}")

        if not os.path.exists(signature_file):
            raise FileNotFoundError(f"Файл с подписью не найден: {signature_file}")

        # Чтение JSON тела
        with open(body_file, 'r', encoding='utf-8') as f:
            body_data = json.load(f)

        # Валидация тела запроса
        if not self.validate_order_body(body_data):
            raise ValueError("Тело запроса не прошло валидацию")

        # Чтение подписи и удаление символов новой строки
        with open(signature_file, 'r', encoding='utf-8') as f:
            signature = f.read().strip()

        # Удаляем все пробельные символы из подписи
        signature = re.sub(r'\s+', '', signature)

        # Проверяем, что подпись не пустая
        if not signature:
            raise ValueError("Подпись не может быть пустой")

        # Формирование заголовков
        headers = self.headers.copy()
        headers.update({
            "Content-Type": "application/json",
            "X-Signature": signature
        })

        # Формирование URL
        url = self.base_url + f'api/v3/order?omsId={self.omsId}'

        # Логирование для отладки
        logger.info(f"URL: {url}")
        logger.info(f"Заголовки (без подписи): { {k: v for k, v in headers.items() if k != 'X-Signature'} }")
        logger.info(f"Длина подписи: {len(signature)} символов")
        logger.info(f"Тело запроса: {json.dumps(body_data, indent=2, ensure_ascii=False)}")

        # Попытки с повторением при ошибке 503
        for attempt in range(max_retries):
            try:
                logger.info(f"Попытка {attempt + 1}/{max_retries}")
                response = requests.post(url,
                                        headers=headers,
                                        json=body_data,
                                        verify=False,
                                        timeout=30)

                logger.info(f"Ответ: {response.status_code}")

                response.raise_for_status()
                return response.json()

            except HTTPError as e:
                if e.response.status_code == 503 and attempt < max_retries - 1:
                    wait_time = (attempt + 1) * 5  # Увеличиваем время ожидания
                    logger.warning(f"Сервис недоступен (503). Повтор через {wait_time} секунд...")
                    time.sleep(wait_time)
                    continue

                # Для других ошибок или последней попытки при 503
                logger.error(f"HTTP Error {e.response.status_code}")
                logger.error(f"Ответ сервера: {e.response.text[:500]}...")

                # Попробуем получить больше информации об ошибке
                try:
                    error_detail = e.response.json()
                    logger.error(f"Детали ошибки: {json.dumps(error_detail, indent=2, ensure_ascii=False)}")
                except:
                    pass

                raise

            except requests.exceptions.Timeout:
                logger.error(f"Таймаут запроса")
                if attempt < max_retries - 1:
                    time.sleep(5)
                    continue
                raise

            except requests.exceptions.ConnectionError as e:
                logger.error(f"Ошибка соединения: {str(e)}")
                if attempt < max_retries - 1:
                    time.sleep(5)
                    continue
                raise

# Использование
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process some parameters.',
                                     epilog=textwrap.dedent('''   additional information:
 You have to specify parameters: omsId, token, client_token. 
 Or set up environment variables: OMSID, TOKEN, CLIENT_TOKEN.
 
         '''))

    # Add arguments for input file, output file, and model
    parser.add_argument('-input_filename', type=str, dest='input_filename',
                        help='Input file name')
    parser.add_argument('-o', '--output_filename', dest='output_filename', default='output_filename.txt', type=str,
                        help='Output file name')
    parser.add_argument('-t', '--token', dest='token', type=str,
                        help='Сгенерированный токен')
    parser.add_argument('-ct', '--client_token', dest='client_token', type=str,
                        help='Токен из ЛК УОТ/Упавление заказами/Устройства')
    parser.add_argument('-oid', '--omsid', dest='omsId', type=str,
                        help='OMS ID из ЛК УОТ/Упавление заказами/Устройства')
    parser.add_argument('--create-order', action='store_true',
                        help='Создать заказ на эмиссию кодов')
    parser.add_argument('--body-file', type=str,
                        help='Путь к файлу с телом запроса (JSON)')
    parser.add_argument('--signature-file', type=str,
                        help='Путь к файлу с подписью')
    parser.add_argument('--max-retries', type=int, default=3,
                    help='Максимальное количество повторных попыток при ошибке 503')
    parser.add_argument('-eo', '--eorder', dest='eorder', type=str, default='',
                        help='Идентификатор заказа на эмиссию для выгрузки')
    parser.add_argument('-qt', '--qty', dest='qty', type=int, default=1,
                        help='Количество кодов для выгрузки. 0 - все доступные')


    # Parse command line arguments
    args = parser.parse_args()
    if args.input_filename:
        logger.debug("Processing:" + args.input_filename)

    # Получение параметров
    token = args.token or os.getenv('HONEST_SIGN_TOKEN')
    if not token:
        raise ValueError("Токен не найден")
    omsId = args.omsId or os.getenv('OMSID')
    if not omsId:
        raise ValueError("omsId не найден")
    clientToken = args.client_token or os.getenv('CLIENT_TOKEN')
    if not clientToken:
        raise ValueError("CLIENT_TOKEN не найден")

    try:
        api = SUZ(token, omsId, clientToken)

        # Если указан флаг создания заказа
        if args.create_order:
            if not args.body_file:
                raise ValueError("Для создания заказа необходимо указать --body-file")
            if not args.signature_file:
                raise ValueError("Для создания заказа необходимо указать --signature-file")

            # Проверяем существование файлов
            if not os.path.exists(args.body_file):
                raise FileNotFoundError(f"Файл с телом запроса не найден: {args.body_file}")
            if not os.path.exists(args.signature_file):
                raise FileNotFoundError(f"Файл с подписью не найден: {args.signature_file}")

            logger.info(f"Создание заказа с использованием:")
            logger.info(f"  Тело запроса: {args.body_file}")
            logger.info(f"  Подпись: {args.signature_file}")

            result = api.order_create(args.body_file, args.signature_file, args.max_retries)
            print(json.dumps(result, indent=4, ensure_ascii=False))
            exit()
        if args.eorder=='test':
            res = api.order_list()
            for orderNum, r in enumerate(res.get('orderInfos', {})):
                if orderNum == args.eorder:
                    logger.info(f'Заказ {args.orderNum} найден')
                else:
                    continue
                if r.get('orderStatus') == 'READY':
                    logger.info(f'buffers: {len(r.get("buffers"))}')
                else:
                    logger.error(f'Заказ {args.orderNum} orderStatus {r.get("orderStatus")}')
            exit()

        else:
            # Оригинальная логика для получения списка заказов
            res = api.order_list()

            print(json.dumps(res, indent=4, ensure_ascii=False))
            for orderNum, r in enumerate(res.get('orderInfos', {})):
                logger.info(f'orderId: {r.get("orderId")}, productionOrderId: {r.get("productionOrderId")}, orderStatus: {r.get("orderStatus")}.')
                if args.eorder:
                    logger.info(f'eorder:{args.eorder} orderNum{orderNum} orderId{r.get("orderId")}')
                    if r.get("orderId") == args.eorder:
                        logger.info(f'Заказ {args.eorder} найден')
                    else:
                        continue

                if r.get('orderStatus') == 'READY':
                    logger.info(f'buffers: {len(r.get("buffers"))}')
                    for buffer in r.get('buffers', []):
                        order_statuses = api.order_status(r['orderId'], buffer['gtin'])
                        for i, order_status in enumerate(order_statuses):
                            logger.info(f'status:{order_status.get("productionOrderId","")+"_"+str(i)}, {order_status}')
                            bloks = api.order_codes_blocks(orderId=r['orderId'], gtin=buffer['gtin'])
                            logger.info(bloks.keys())
                            if not bloks['blocks'] or args.eorder:
                                if args.eorder:
                                    leftInBuffer = int(order_status.get('leftInBuffer'))
                                    if args.qty == 0:
                                        quantity = leftInBuffer
                                    else:
                                        quantity = args.qty if args.qty <= leftInBuffer else leftInBuffer
                                #codes = api.codes(orderId=r['orderId'], quantity=quantity, gtin=buffer['gtin'])
                                #json.dump(codes, open(codes['blockId'],'w',encoding='UTF8'), indent=4)
                            else:
                                for blok in bloks['blocks']:
                                    logger.debug(f'block:{blok["quantity"]}')
                                    codes = api.order_codes_retry(blok['blockId'])
                                    logger.debug(f'Successfully got:{len(codes.get("codes"))}')
                                    json.dump(codes, open(codes['blockId'],'w',encoding='UTF8'), indent=4)
                                    logger.debug(f'Write {codes["blockId"]}:{len(codes.get("codes"))}')
                else:
                    logger.info(f'orderId: {r.get("orderId")}, productionOrderId: {r.get("")}, status: {r.get("orderStatus")}')
                if orderNum > 10:
                    logger.debug(f'OrderNum:{orderNum}. Exiting')
                    break

    except HTTPError as e:
        print(f"HTTP Error: {e.response.status_code}")
        if e.response.text:
            try:
                error_json = e.response.json()
                print(json.dumps(error_json, indent=4, ensure_ascii=False))
            except:
                print(e.response.text)
    except FileNotFoundError as e:
        print(f"File Error: {str(e)}")
    except ValueError as e:
        print(f"Value Error: {str(e)}")
    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()