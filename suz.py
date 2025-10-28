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
                                headers=self.headers)  # json=["0104670404500312215'!,4L"],
        response.raise_for_status()
        return response.json()
    def order_status(self,orderId:str, gtin:str):

        response = requests.get(self.base_url + f'api/v3/order/status?omsId={self.omsId}'
                                                f'&orderId={orderId}'
                                                f'&gtin={gtin}',
                                headers=self.headers)  # json=["0104670404500312215'!,4L"],
        response.raise_for_status()
        return response.json()

    def codes(self, orderId: str, quantity: int, gtin: str):

        response = requests.get(self.base_url + f'api/v3/codes?omsId={self.omsId}'
                                                f'&orderId={orderId}'
                                                f'&quantity={quantity}'
                                                f'&gtin={gtin}',
                                headers=self.headers)  # json=["0104670404500312215'!,4L"],
        response.raise_for_status()
        return response.json()

    def order_codes_retry(self, blockId: str):

        response = requests.get(self.base_url + f'api/v3//order/codes/retry?omsId={self.omsId}'
                                                f'&blockId={blockId}',
                                headers=self.headers)  # json=["0104670404500312215'!,4L"],
        response.raise_for_status()
        return response.json()

    def order_codes_blocks(self,orderId:str, gtin:str):

        response = requests.get(self.base_url + f'api/v3/order/codes/blocks?omsId={self.omsId}'
                                                f'&orderId={orderId}'
                                                f'&gtin={gtin}',
                                headers=self.headers)  # json=["0104670404500312215'!,4L"],
        response.raise_for_status()
        return response.json()

    def providers(self):
        response = requests.get(self.base_url + f'api/v3/providers?omsId={self.omsId}',
                                headers=self.headers)  # json=["0104670404500312215'!,4L"],
        response.raise_for_status()
        return response.json()


# Использование
if __name__ == "__main__":
    # cProfile.run('main()'#, 'profile_output.txt'
    #              )
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
    parser.add_argument('-сt', '--client_token', dest='client_token', type=str,
                        help='Токен из ЛК УОТ/Упавление заказами/Устройства')
    parser.add_argument('-oid', '--omsid', dest='omsId', type=str,
                        help='OMS ID из ЛК УОТ/Упавление заказами/Устройства')

    # Parse command line arguments
    args = parser.parse_args()
    if args.input_filename:
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
        raise ValueError("CLIENT_TOKEN не найден")

    try:
        api = SUZ(token, omsId, clientToken)
        res = api.order_list()
        print(res)
        for r in res.get('orderInfos', {}):
            logger.info(f'orderId: {r.get('orderId')}, productionOrderId: {r.get('productionOrderId')}, orderStatus: {r.get("orderStatus")}.')
            if r.get('orderStatus') == 'READY':
                logger.info(f'buffers: {len(r.get("buffers"))}')
                for buffer in r.get('buffers', []):
                    order_statuses = api.order_status(r['orderId'], buffer['gtin'])
                    for i, order_status in enumerate(order_statuses):
                        logger.info(f'status:{order_status['productionOrderId']+'_'+str(i)}, {order_status}')
                        bloks = api.order_codes_blocks(orderId=r['orderId'], gtin=buffer['gtin'])
                        if not bloks['blocks']:
                            codes = api.codes(orderId=r['orderId'], quantity=1, gtin=buffer['gtin'])
                            json.dump(codes, open(codes['blockId'],'w',encoding='UTF8'), indent=4)
                        else:
                            for blok in bloks['blocks']:
                                logger.debug(f'block:{blok['quantity']}')
                                codes = api.order_codes_retry(blok['blockId'])
                                logger.debug(f'Successfully got:{len(codes.ged('codes'))}')
                                json.dump(codes, open(codes['blockId'],'w',encoding='UTF8'), indent=4)
                                logger.debug(f'Write {codes['blockId']}:{len(codes.ged('codes'))}')

            else:
                logger.info(f'orderId: {r.get("orderId")}, productionOrderId: {r.get('')}, status: {r.get("orderStatus")}')
    except HTTPError as e:
        print(e.response.text)