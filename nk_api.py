import os
import sys
import argparse
import logging
import requests
from datetime import datetime
import csv
from openpyxl import load_workbook


# ---------------------------
# Настройка логгера
# ---------------------------
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


class NK:
    """
    Клиент API Национального Каталога маркированных товаров (Честный Знак).
    Версия API: v5.38
    """

    def __init__(self, token: str = None, apikey: str = None, sandbox: bool = False):
        """
        Инициализация API клиента.
        :param token: Bearer-токен True API
        :param apikey: API Key Национального каталога
        :param sandbox: использовать тестовую среду
        """
        self.token = token or os.getenv("TRUE_API_TOKEN")
        self.apikey = apikey or os.getenv("API_KEY")
        self.sandbox = sandbox

        if not self.token and not self.apikey:
            raise ValueError("Не найден ни token, ни apikey. "
                             "Установите переменные TRUE_API_TOKEN или API_KEY.")

        # Правильный хост API
        self.base_url = (
            "https://api.nk.sandbox.crptech.ru"
            if sandbox
            else "https://xn--80aqu.xn----7sbabas4ajkhfocclk9d3cvfsa.xn--p1ai"
        )

    # ---------------------------
    # Метод 1: Получить карточку по GTIN (v3/product)
    # ---------------------------
    def get_set_by_gtin(self, gtin: str):
        url = f"{self.base_url}/v3/product"
        headers = {"Accept": "application/json", "Content-Type": "application/json"}
        params = {"gtin": gtin, "format": "json"}

        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        elif self.apikey:
            params["apikey"] = self.apikey

        logger.info(f"GET {url} (GTIN: {gtin})")
        response = requests.get(url, headers=headers, params=params, timeout=30)
        logger.info(f"Status: {response.status_code}")

        if response.status_code != 200:
            logger.error(f"Ошибка API: {response.status_code}")
            return None

        try:
            return response.json()
        except Exception as e:
            logger.error(f"Ошибка декодирования JSON: {e}")
            return None
    # ---------------------------
    # Метод 1: Получить карточку с расширенной информацией по GTIN (v3/feed-product)
    # ---------------------------
    def feedProduct(self, gtin: str):
        url = f"{self.base_url}/v3/feed-product"
        headers = {"Accept": "application/json", "Content-Type": "application/json"}
        params = {"gtin": gtin, "format": "json"}

        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        elif self.apikey:
            params["apikey"] = self.apikey

        logger.info(f"GET {url} (GTIN: {gtin})")
        response = requests.get(url, headers=headers, params=params, timeout=30)
        logger.info(f"Status: {response.status_code}")

        if response.status_code != 200:
            logger.error(f"Ошибка API: {response.status_code}")
            return None

        try:
            return response.json()
        except Exception as e:
            logger.error(f"Ошибка декодирования JSON: {e}")
            return None

    # ---------------------------
    # Метод 2: Получить разрешительный документ (v4/rd-info-by-gtin)
    # ---------------------------
    def get_permit_document_by_gtin(self, gtin: str, inn: str):
        url = f"{self.base_url}/v4/rd-info-by-gtin"

        headers = {"Accept": "application/json", "Content-Type": "application/json"}
        params = {"format": "json"}
        payload = {"gtin": gtin, "inn": inn}

        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        elif self.apikey:
            params["apikey"] = self.apikey

        logger.info(f"POST {url} (GTIN: {gtin})")
        response = requests.post(url, headers=headers, params=params, json=payload, timeout=30)
        logger.info(f"Status: {response.status_code}")

        if response.status_code != 200:
            logger.error(f"Ошибка API: {response.status_code}")
            return []

        try:
            data = response.json()
        except Exception as e:
            logger.error(f"Ошибка декодирования JSON: {e}")
            return []

        documents = []
        try:
            result = data.get("result", {})
            docs = result.get("documents", [])
            for d in docs:
                number = d.get("number")
                from_date = d.get("from_date")
                to_date = d.get("to_date")
                applicant = d.get("applicant")
                manufacturer = d.get("manufacturer")

                days_left = None
                if to_date:
                    try:
                        dt_to = datetime.strptime(to_date, "%Y-%m-%d")
                        days_left = (dt_to - datetime.now()).days
                    except ValueError:
                        logger.warning(f"Неверный формат даты: {to_date}")

                documents.append({
                    "number": number,
                    "from_date": from_date,
                    "to_date": to_date,
                    "days_left": days_left,
                    "applicant": applicant,
                    "manufacturer": manufacturer,
                })
        except Exception as e:
            logger.warning(f"Ошибка разбора структуры документа: {e}")

        return documents


# ---------------------------
# Чтение GTIN из CSV или Excel
# ---------------------------
def load_gtin_list(filepath: str):
    """
    Загружает список GTIN из Excel (.xlsx) или CSV файла.
    Ожидается, что в файле есть колонка с названием 'gtin' (регистр не важен).
    """
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"Файл не найден: {filepath}")

    ext = os.path.splitext(filepath)[1].lower()
    gtins = []

    if ext == ".csv":
        with open(filepath, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            headers = [h.lower().strip() for h in reader.fieldnames]
            gtin_col = None
            for h in headers:
                if h in ("gtin", "код", "код_товара"):
                    gtin_col = h
                    break
            if not gtin_col:
                raise ValueError("Не найдена колонка 'gtin' или 'код_товара' в CSV.")

            for row in reader:
                val = row.get(gtin_col)
                if val:
                    gtins.append(val.strip())

    elif ext in (".xlsx", ".xls"):
        wb = load_workbook(filepath, read_only=True)
        ws = wb.active
        headers = [str(c.value).strip().lower() for c in next(ws.iter_rows(min_row=1, max_row=1))]
        gtin_col_idx = None
        for i, h in enumerate(headers):
            if h in ("gtin", "код", "код_товара"):
                gtin_col_idx = i
                break
        if gtin_col_idx is None:
            raise ValueError("Не найдена колонка 'gtin' или 'код_товара' в Excel.")

        for row in ws.iter_rows(min_row=2, values_only=True):
            val = row[gtin_col_idx]
            if val:
                gtins.append(str(val).strip())
    else:
        raise ValueError("Поддерживаются только файлы .csv и .xlsx")

    logger.info(f"Загружено {len(gtins)} GTIN из {filepath}")
    return gtins


# ---------------------------
# CLI: запуск проверки
# ---------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Получение данных из Национального Каталога Честный Знак через True API"
    )
    parser.add_argument("--gtin", help="GTIN товара или набора")
    parser.add_argument("--file", help="Excel или CSV файл со списком GTIN")
    parser.add_argument("--inn", required=False, help="ИНН компании для проверки документа")
    parser.add_argument("--sandbox", action="store_true", help="использовать тестовую среду")

    args = parser.parse_args()

    try:
        nk = NK(sandbox=args.sandbox)

        gtin_list = []
        if args.file:
            gtin_list = load_gtin_list(args.file)
        elif args.gtin:
            gtin_list = [args.gtin]
        else:
            logger.error("Не указан GTIN или файл (--gtin или --file).")
            sys.exit(1)

        for gtin in gtin_list:
            logger.info("=" * 80)
            logger.info(f"Проверка GTIN: {gtin}")
            try:
                product = nk.feedProduct(gtin=gtin)
            except Exception as e:
                logger.info(f'feed-product fo gtin{gtin}: {e}')
            product = nk.get_set_by_gtin(gtin)
            if product and product.get("result"):
                item = product["result"][0]
                logger.info(f"Наименование: {item.get('good_name')}")
                if item.get("is_set"):
                    logger.info("Это набор. Состав:")
                    for s in item.get("set_gtins"):
                        logger.info(f" - GTIN {s['gtin']} x{s['quantity']}")
            else:
                logger.warning("Карточка не найдена.")

            if args.inn:
                docs = nk.get_permit_document_by_gtin(gtin, args.inn)
                if not docs:
                    logger.warning("Разрешительные документы не найдены.")
                else:
                    for d in docs:
                        logger.info(f"Документ № {d['number']} от {d['from_date']} до {d['to_date']}")
                        if d["days_left"] is not None:
                            logger.info(f"  Осталось {d['days_left']} дней до окончания")
                        logger.info(f"  Заявитель: {d['applicant']}")
                        logger.info(f"  Изготовитель: {d['manufacturer']}")

    except Exception as e:
        logger.exception(f"Критическая ошибка: {e}")
        sys.exit(1)
