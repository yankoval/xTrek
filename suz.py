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
#import json
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
                                headers=self.headers)  # json=["0104670404500312215'!,4L"],
        response.raise_for_status()
        return response.json()

    def providers(self):
        response = requests.get(self.base_url + f'api/v3/providers?omsId={self.omsId}',
                                headers=self.headers)  # json=["0104670404500312215'!,4L"],
        response.raise_for_status()
        return response.json()


# Использование
def main(*args: Any, **kwargs: Any) -> None:
    try:
        api = SUZ()
        res = api.order_list()
        print(res)
        for r in res.get('orderInfos', {}):
            if r.get('orderStatus') == 'READY':
                print(r.get('buffers'))
    except HTTPError as e:
        print(e.response.text)

if __name__ == "__main__":
    # cProfile.run('main()'#, 'profile_output.txt'
    #              )
    parser = argparse.ArgumentParser(description='Process some parameters.',
                                     epilog=textwrap.dedent('''   additional information:
             If you vont to use .ini file put __CONSTANTS__=DEFAULT env variable 
             and create programm_name.ini file with content: 
             [DEFAULT] 
             something = a_default_value
             [a_section]
             something = a_section_value
         '''))

    # Add arguments for input file, output file, and model
    parser.add_argument('-input_filename', type=str, dest='input_filename', default='input_filename.txt',
                        help='Input file name')
    parser.add_argument('-o', '--output_filename', dest='output_filename', default='output_filename.txt', type=str,
                        help='Output file name')
    parser.add_argument('-t', '--token', dest='token', type=str,
                        help='Сгенерированный токен')
    parser.add_argument('-сt', '--client_token', dest='client_token', type=str,
                        help='Токен из ЛК УОТ/Упавление заказами/Устройства')
    parser.add_argument('-oid', '--omsid', dest='omsId', type=str,
                        help='OMS ID из ЛК УОТ/Упавление заказами/Устройства')

    # Parse command line arguments
    args = parser.parse_args()
    logger.debug("Procesing:" + args.input_filename)

    # Call the main function with the parsed arguments
    # main(args.input_filename, args.output_filename, logger)
    token = args.token or os.getenv('HONEST_SIGN_TOKEN')
    if not token:
        raise ValueError("Токен не найден")
    omsId = args.omsId or os.getenv('OMSID')
    if not omsId:
        raise ValueError("omsId не найден")
    clientToken = args.client_token or os.getenv('CLIENT_TOKEN')
    if not clientToken:
        raise ValueError("omsId не найден")

    try:
        api = SUZ(token, omsId, clientToken)
        res = api.order_list()
        print(res)
        for r in res.get('orderInfos', {}):
            if r.get('orderStatus') == 'READY':
                print(r.get('buffers'))
    except HTTPError as e:
        print(e.response.text)