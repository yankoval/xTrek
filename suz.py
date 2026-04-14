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
import urllib3
from typing import List, Dict, Any, Generator, Union
from suz_api_models import EmissionOrderreceipts
from org_manager import OrganizationManager
from tokens import TokenProcessor

# logging
logger = logging.getLogger(__name__)
coloredlogs.install(level=logging.DEBUG, logger=logger, isatty=True,
                    fmt="%(asctime)s %(levelname)-8s %(message)s",
                    stream=sys.stderr,
                    datefmt='%Y-%m-%d %H:%M:%S')

# Отключаем предупреждения о небезопасном соединении
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


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
            raise ValueError("clientToken не найден")

        self.base_url = "https://suzgrid.crpt.ru"
        # В СУЗ API v3 для аутентификации используется заголовок clientToken.
        # В него передается либо динамический UUID-токен (полученный через auth),
        # либо статический Connection ID из ЛК.
        # Использование Authorization: Bearer приводит к ошибкам валидации или конфликтам.
        self.headers = {
            "clientToken": f"{self.token or self.clientToken}",
            "Accept": "application/json"
        }

    def _get(self, url, params=None):
        try:
            response = requests.get(url, params=params, headers=self.headers, verify=False)
            if response.status_code != 200:
                logger.debug(f"GET {url} failed with {response.status_code}: {response.text}")
            response.raise_for_status()
            return response.json()
        except Exception as e:
            if hasattr(e, 'response') and e.response is not None:
                logger.error(f"GET {url} failed. Status: {e.response.status_code}, Body: {e.response.text}")
            raise

    def order_list(self):
        url = f"{self.base_url}/api/v3/order/list?omsId={self.omsId}"
        return self._get(url)

    def order_status(self, orderId: str, gtin: str):
        url = f"{self.base_url}/api/v3/order/status?omsId={self.omsId}&orderId={orderId}&gtin={gtin}"
        return self._get(url)

    def codes(self, orderId: str, quantity: int, gtin: str):
        url = f"{self.base_url}/api/v3/codes?omsId={self.omsId}&orderId={orderId}&quantity={quantity}&gtin={gtin}"
        return self._get(url)

    def order_codes_retry(self, blockId: str):
        """ Метод «Получить повторно коды маркировки из заказа КМ» """
        url = self.base_url + f'api/v3/order/codes/retry?omsId={self.omsId}&blockId={blockId}'
        return self._get(url)

    def order_codes_blocks(self, orderId: str, gtin: str):
        url = f"{self.base_url}/api/v3/order/codes/blocks?omsId={self.omsId}&orderId={orderId}&gtin={gtin}"
        return self._get(url)

    def providers(self):
        url = f"{self.base_url}/api/v3/providers?omsId={self.omsId}"
        return self._get(url)

    def _send_signed_request(self, url: str, body_file: str, signature_file: str, max_retries: int = 3, extra_headers: dict = None) -> requests.Response:
        """
        Вспомогательный метод для отправки подписанного POST запроса в СУЗ
        """
        import time
        import re

        if not os.path.exists(body_file):
            raise FileNotFoundError(f"Файл с телом запроса не найден: {body_file}")
        if not os.path.exists(signature_file):
            raise FileNotFoundError(f"Файл с подписью не найден: {signature_file}")

        # Чтение JSON тела в бинарном виде для обеспечения идентичности при отправке
        with open(body_file, 'rb') as f:
            body_bytes = f.read()

        # Чтение подписи и удаление всех пробельных символов
        with open(signature_file, 'r', encoding='utf-8') as f:
            signature = f.read().strip()
        signature = re.sub(r'\s+', '', signature)

        if not signature:
            raise ValueError("Подпись не может быть пустой")

        # Формирование заголовков
        headers = self.headers.copy()
        headers.update({
            "Content-Type": "application/json; charset=utf-8",
            "X-Signature": signature
        })
        if extra_headers:
            headers.update(extra_headers)

        logger.info(f"URL: {url}")
        logger.info(f"Тело запроса (бинарное, длина): {len(body_bytes)} байт")

        # Попытки с повторением при ошибке 503 или сетевых ошибках
        for attempt in range(max_retries):
            try:
                logger.info(f"Попытка {attempt + 1}/{max_retries}")
                response = requests.post(url, headers=headers, data=body_bytes, verify=False, timeout=30)
                logger.info(f"Ответ: {response.status_code}")

                if response.status_code == 200:
                    return response

                response.raise_for_status()
            except HTTPError as e:
                if e.response.status_code == 503 and attempt < max_retries - 1:
                    wait_time = (attempt + 1) * 5
                    logger.warning(f"Сервис недоступен (503). Повтор через {wait_time} секунд...")
                    time.sleep(wait_time)
                    continue
                raise
            except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
                if attempt < max_retries - 1:
                    logger.warning(f"Ошибка сети: {str(e)}. Повтор через 5 секунд...")
                    time.sleep(5)
                    continue
                raise
        return None

    def utilisation_send(self, body_file: str, signature_file: str, max_retries: int = 3, orderId: str = None) -> str:
        """
        Отправить отчёт об использовании (нанесении) КМ (Метод 4.4.11)
        """
        url = f"{self.base_url}/api/v3/utilisation?omsId={self.omsId}"
        extra_headers = {}
        if orderId:
            extra_headers["orderId"] = orderId

        try:
            response = self._send_signed_request(url, body_file, signature_file, max_retries, extra_headers=extra_headers)
            if response and response.status_code == 200:
                data = response.json()
                return data.get('reportId', '')
            return ""
        except Exception as e:
            logger.error(f"Ошибка при отправке отчета о нанесении: {e}")
            if hasattr(e, 'response') and e.response is not None:
                return e.response.text
            return str(e)

    def report_info(self, reportId: str):
        """
        Получить статус обработки отчёта (Метод 4.4.13)
        """
        url = f"{self.base_url}/api/v3/report/info?omsId={self.omsId}&reportId={reportId}"
        return self._get(url)

    def utilisation_reports_list(self, orderId: str, limit: int = None, skip: int = None):
        """
        Получить список идентификаторов отчетов «Сведения о нанесении» (Метод 4.4.15)
        """
        url = f"{self.base_url}/api/v3/quality?omsId={self.omsId}&orderId={orderId}"
        params = {}
        if limit: params["limit"] = limit
        if skip: params["skip"] = skip

        return self._get(url, params=params)

    def utilisation_codes(self, reportId: str):
        """
        Получить список КИ из отчета «Сведения о нанесении» (Метод 4.4.16)
        """
        url = f"{self.base_url}/api/v3/utilisation/codes?omsId={self.omsId}&reportId={reportId}"
        return self._get(url)

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
    def order_create(self, body_file: str, signature_file: str, max_retries: int = 3) -> Union[EmissionOrderreceipts, str]:
        """
        Создание заказа на эмиссию кодов

        Args:
            body_file: путь к файлу с телом запроса (JSON)
            signature_file: путь к файлу с подписью
            max_retries: максимальное количество повторных попыток при ошибке 503
        """
        # Чтение тела для валидации
        with open(body_file, 'r', encoding='utf-8') as f:
            body_data = json.load(f)

        # Валидация тела запроса
        if not self.validate_order_body(body_data):
            raise ValueError("Тело запроса не прошло валидацию")

        # Формирование URL
        url = self.base_url + f'api/v3/order?omsId={self.omsId}'

        try:
            response = self._send_signed_request(url, body_file, signature_file, max_retries)

            if response and response.status_code == 200:
                data = response.json()
                return EmissionOrderreceipts(
                    orderId=data.get('orderId'),
                    expectedCompleteTimestamp=data.get('expectedCompleteTimestamp'),
                    omsId=data.get('omsId')
                )
            return ""

        except HTTPError as e:
            logger.error(f"HTTP Error {e.response.status_code}")
            logger.error(f"Ответ сервера: {e.response.text[:500]}...")
            try:
                error_detail = e.response.json()
                logger.error(f"Детали ошибки: {json.dumps(error_detail, indent=2, ensure_ascii=False)}")
            except:
                pass
            return str(e.response.text)

        except Exception as e:
            logger.error(f"Ошибка: {str(e)}")
            return ""

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
    parser.add_argument('--inn', type=str,
                        help='ИНН организации для автоматического поиска параметров')
    parser.add_argument('--create-order', action='store_true',
                        help='Создать заказ на эмиссию кодов')
    parser.add_argument('--body-file', type=str,
                        help='Путь к файлу с телом запроса (JSON)')
    parser.add_argument('--signature-file', type=str,
                        help='Путь к файлу с подписью')
    parser.add_argument('--max-retries', type=int, default=3,
                    help='Максимальное количество повторных попыток при ошибке 503')
    parser.add_argument('--order-id', type=str, help='Идентификатор заказа (orderId)')
    parser.add_argument('-eo', '--eorder', dest='eorder', type=str, default='',
                        help='Идентификатор заказа на эмиссию для выгрузки')
    parser.add_argument('-qt', '--qty', dest='qty', type=int, default=1,
                        help='Количество кодов для выгрузки. 0 - все доступные')
    parser.add_argument('--utilisation-reports-list', action='store_true',
                        help='Получить список отчетов о нанесении и коды из них')
    parser.add_argument('--group', type=str, default='chemistry',
                        help='Товарная группа (например, для utilisation_send)')


    parser.add_argument('--limit', type=int, help='Лимит количества отчетов')
    parser.add_argument('--skip', type=int, help='Пропустить N отчетов')

    # Parse command line arguments
    args = parser.parse_args()
    if args.input_filename:
        logger.debug("Processing:" + args.input_filename)

    # Получение параметров
    token = args.token or os.getenv('HONEST_SIGN_TOKEN')
    omsId = args.omsId or os.getenv('OMSID')
    clientToken = args.client_token or os.getenv('CLIENT_TOKEN')
    inn = args.inn

    # Автоматическое определение параметров по ИНН
    if inn and (not token or not omsId or not clientToken):
        logger.info(f"Поиск параметров для ИНН: {inn}")
        base_path = os.path.dirname(os.path.abspath(__file__))
        org_manager = OrganizationManager(os.path.join(base_path, 'my_orgs'))

        found_org = org_manager.find(inn=inn)
        if not found_org:
            # Пытаемся найти среди всех, вдруг там несколько записей
            for o in org_manager.list():
                if o.inn == inn and o.oms_id:
                    found_org = o
                    break

        if found_org:
            logger.info(f"[*] Используется профиль организации: {found_org.name}")
            omsId = omsId or found_org.oms_id
            clientToken = clientToken or found_org.connection_id

            if not token:
                token_processor = TokenProcessor(org_manager=org_manager)
                token = token_processor.get_token_value_by_inn(inn, token_type='UUID', conid=clientToken)
                if token:
                    logger.info("[*] Токен успешно получен из базы")

    if not token:
        raise ValueError("Токен не найден. Укажите --token, установите HONEST_SIGN_TOKEN или укажите --inn с настроенной базой.")
    if not omsId:
        raise ValueError("omsId не найден. Укажите --omsid, установите OMSID или укажите --inn с настроенной базой.")
    if not clientToken:
        raise ValueError("clientToken не найден. Укажите --client_token, установите CLIENT_TOKEN или укажите --inn с настроенной базой.")

    try:
        api = SUZ(token, omsId, clientToken)

        # Если указан флаг получения списка отчетов о нанесении
        if args.utilisation_reports_list:
            order_id = args.order_id or args.eorder
            if not order_id:
                raise ValueError("Для получения списка отчетов необходимо указать --order-id или -eo")

            logger.info(f"Запрос отчетов о нанесении для заказа {order_id}")

            reports_data = api.utilisation_reports_list(orderId=order_id, limit=args.limit, skip=args.skip)
            # В методе 4.4.15 возвращается "results": ["reportId1", "reportId2", ...]
            report_ids = reports_data.get('results', [])

            if not report_ids:
                logger.info("Отчеты не найдены.")
            else:
                for report_id in report_ids:
                    logger.info(f"Отчет ID: {report_id}")

                    try:
                        # Получаем информацию об отчете (статус)
                        info = api.report_info(report_id)
                        logger.info(f"  Статус: {info.get('reportStatus')}")

                        codes_data = api.utilisation_codes(report_id)
                        codes = codes_data.get('sntins', [])
                        logger.info(f"  Количество КИ в отчете: {len(codes)}")
                        if codes:
                            logger.info(f"    Первые 5 кодов: {codes[:5]}")
                    except Exception as e:
                        logger.error(f"  Не удалось получить данные для отчета {report_id}: {e}")
            exit()

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
            if isinstance(result, EmissionOrderreceipts):
                print(json.dumps(result.to_dict(), indent=4, ensure_ascii=False))
            else:
                print(result)
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