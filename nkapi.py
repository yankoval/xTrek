import os
import logging
import requests
from datetime import datetime

logger = logging.getLogger(__name__)


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
            errors = result.get("errors", [])
            logger.info(f'Gtin:{gtin} error:{errors}')
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
    # Метод 3: Получить список доступных GTIN для субаккаунта (v3/linked-gtins)
    # ---------------------------
    def get_linked_gtins(self, inn: str = None, gtin: str = None, limit: int = None, offset: int = None):
        """
        Получить список компаний и кодов товаров, по которым предоставлен доступ субаккаунту.
        
        :param inn: ИНН владельца товара (опционально)
        :param gtin: Код товара для проверки доступности (опционально)
        :param limit: Количество записей в ответе (макс. 10000)
        :param offset: Смещение относительно начала выдачи
        :return: Список доступных GTIN или None в случае ошибки
        """
        url = f"{self.base_url}/v3/linked-gtins"
        headers = {"Accept": "application/json", "Content-Type": "application/json"}
        params = {"format": "json"}

        # Добавляем опциональные параметры
        if inn:
            params["inn"] = inn
        if gtin:
            params["gtin"] = gtin
        if limit:
            params["limit"] = limit
        if offset:
            params["offset"] = offset

        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        elif self.apikey:
            params["apikey"] = self.apikey

        logger.info(f"GET {url} (INN: {inn}, GTIN: {gtin}, limit: {limit}, offset: {offset})")
        
        try:
            response = requests.get(url, headers=headers, params=params, timeout=30)
            logger.info(f"Status: {response.status_code}")

            if response.status_code != 200:
                logger.error(f"Ошибка API: {response.status_code}")
                return None

            data = response.json()
            
            # Обрабатываем результат
            result = data.get("result", {})
            linked_gtins = result.get("linked_gtins", [])
            errors = result.get("errors", [])
            
            # Логируем ошибки, если есть
            if errors:
                for error in errors:
                    logger.warning(f"Ошибка в ответе: {error.get('message')} (код: {error.get('code')})")
            
            return linked_gtins
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Ошибка сети: {e}")
            return None
        except Exception as e:
            logger.error(f"Ошибка обработки ответа: {e}")
            return None
    # ---------------------------
    # Метод 5: Получить список доступных GTIN для субаккаунта (/v4/product-list)
    # ---------------------------
    def _get_gtins(self, from_date:str=None, to_date:str=None, limit: int = None, offset: int = None):
        """
        3.1.4. Метод «Получить список собственных карточек с краткой информацией по
        ним»
        
        :param limit: Количество записей в ответе (макс. 10000)
        :param offset: Смещение относительно начала выдачи
        :param from_date: Дата и время в формате YYYY-MM-DD HH:ii:ss 
                            Будут выбраны все «gtin», обновленные в
                            течении месяца после указанной даты
        :return: Список доступных GTIN или None в случае ошибки

        Метод «/v4/product-list» возвращает список товаров, принадлежащих владельцу, с краткой
информацией по ним. Максимальное количество товарных позиций в выборке: 10000 По ним
можно перемещаться с помощью параметров «limit» («Количество записей в ответе») и «offset»
(«Смещение относительно начала выдачи»).
Примечание:
• если в запросе не передан ни один из параметров «from_date» или «to_date», то метод
выполняет поиск карточек, обновленных за месяц вперед от текущей даты;
• если у компании более 10000 карточек товаров, обновленных за заданный параметрами
«to_date» и/или «from_date» период, то будет возвращен ответ с кодом 413;
• если в запросе указываются параметры «limit» («Количество записей в ответе») и «offset»
(«Смещение относительно начала выдачи»), то их суммарное значение не должно превышать
10000, в противном случае будет возвращен ответ с кодом 413;
• если в запросе не указываются параметры «limit» («Количество записей в ответе») и «offset»
(«Смещение относительно начала выдачи»), то «limit» («Количество записей в ответе»)
считается равным 1000, а «offset» («Смещение относительно начала выдачи») равным 0;
• если в запросе одновременно передаются параметры «from_date» и «to_date», то заданный
период может быть больше месяца.
        """
        url = f"{self.base_url}/v4/product-list"
        headers = {"Accept": "application/json", "Content-Type": "application/json"}
        params = {"format": "json"}

        # Добавляем опциональные параметры
        if limit:
            params["limit"] = limit
        if offset:
            params["offset"] = offset
        if from_date:
            params["from_date"] = from_date
        if to_date:
            params["to_date"] = to_date

        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        elif self.apikey:
            params["apikey"] = self.apikey

        logger.info(f"GET {url} (limit: {limit}, offset: {offset})")
        
        try:
            response = requests.get(url, headers=headers, params=params, timeout=30)
            logger.info(f"Status: {response.status_code}")

            if response.status_code != 200:
                logger.error(f"Ошибка API: {response.status_code}")
                return None

            data = response.json()
            
            # Обрабатываем результат
            result = data.get("result", {})
            goods = result.get("goods", [])
            errors = result.get("errors", [])
            
            # Логируем ошибки, если есть
            if errors:
                for error in errors:
                    logger.warning(f"Ошибка в ответе: {error.get('message')} (код: {error.get('code')})")
            
            return goods
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Ошибка сети: {e}")
            return None
        except Exception as e:
            logger.error(f"Ошибка обработки ответа: {e}")
            return None

   # ---------------------------
    # Метод 6: Получить все доступные GTIN с постраничной выгрузкой
    # ---------------------------
    def get_gtins(self, page_size: int = 1000):
        """
        Получить все доступные GTIN принадлежащие клиенту с постраничной выгрузкой.
        
        :param page_size: Размер страницы (макс. 10000)
        :return: Список всех доступных GTIN или None в случае ошибки
        """
        all_gtins = []
        offset = 0
        
        logger.info(f"Начало постраничной выгрузки доступных GTIN (page_size: {page_size})")
        
        while True:
            logger.info(f"Запрос страницы с offset: {offset}")
            
            page_result = self._get_gtins(
                from_date='2000-01-01 00:00:00',
                to_date=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                limit=page_size,
                offset=offset
            )
            
            if page_result is None:
                logger.error("Ошибка при получении страницы, прерывание выгрузки")
                return None
                
            if not page_result:
                logger.info("Получена пустая страница, завершение выгрузки")
                break
                
            # Добавляем результаты текущей страницы
            all_gtins.extend(page_result)
            logger.info(f"Получено {len(page_result)} GTIN на текущей странице, всего: {len(all_gtins)}")
            
            # Проверяем, есть ли еще данные
            if len(page_result) < page_size:
                logger.info("Получено меньше запрошенного количества, завершение выгрузки")
                break
                
            # Увеличиваем offset для следующей страницы
            offset += page_size
            
            # Пауза между запросами для соблюдения лимитов API
            logger.info("Пауза 1 секунда перед следующим запросом...")
            import time
            time.sleep(1)
        
        logger.info(f"Постраничная выгрузка завершена. Всего получено GTIN: {len(all_gtins)}")
        return all_gtins

    # ---------------------------
    # Метод 4: Получить все доступные linked GTIN  с постраничной выгрузкой
    # ---------------------------
    def get_all_linked_gtins(self, inn: str = None, page_size: int = 1000):
        """
        Получить все доступные GTIN для субаккаунта с постраничной выгрузкой.
        
        :param inn: ИНН владельца товара (опционально)
        :param page_size: Размер страницы (макс. 10000)
        :return: Список всех доступных GTIN или None в случае ошибки
        """
        all_linked_gtins = []
        offset = 0
        
        logger.info(f"Начало постраничной выгрузки доступных GTIN (page_size: {page_size})")
        
        while True:
            logger.info(f"Запрос страницы с offset: {offset}")
            
            page_result = self.get_linked_gtins(
                inn=inn,
                limit=page_size,
                offset=offset
            )
            
            if page_result is None:
                logger.error("Ошибка при получении страницы, прерывание выгрузки")
                return None
                
            if not page_result:
                logger.info("Получена пустая страница, завершение выгрузки")
                break
                
            # Добавляем результаты текущей страницы
            all_linked_gtins.extend(page_result)
            logger.info(f"Получено {len(page_result)} GTIN на текущей странице, всего: {len(all_linked_gtins)}")
            
            # Проверяем, есть ли еще данные
            if len(page_result) < page_size:
                logger.info("Получено меньше запрошенного количества, завершение выгрузки")
                break
                
            # Увеличиваем offset для следующей страницы
            offset += page_size
            
            # Пауза между запросами для соблюдения лимитов API
            logger.info("Пауза 1 секунда перед следующим запросом...")
            import time
            time.sleep(1)
        
        logger.info(f"Постраничная выгрузка завершена. Всего получено GTIN: {len(all_linked_gtins)}")
        return all_linked_gtins
