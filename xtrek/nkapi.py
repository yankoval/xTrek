import os
import json
import logging
import requests
from datetime import date, datetime
from typing import Any, Dict, List, Optional
from .suz_api_models import GtinDocument

logger = logging.getLogger(__name__)


class NK:

    TRUE_API_PERMIT_TYPES = {
        "CONFORMITY_CERTIFICATE",
        "CONFORMITY_DECLARATION",
        "STATE_REGISTRATION_CERTIFICATE",
        "REGISTRATION_CERTIFICATE",
        "REGISTRATION_VET_CERTIFICATE",
    }

    RD_LIST_DATEFROM_REQUIRED = {
        "CONFORMITY_CERTIFICATE",
        "CONFORMITY_DECLARATION",
    }
    """
    Клиент API Национального Каталога маркированных товаров (Честный Знак).
    Версия API: v5.38
    """

    def __init__(self, token: str = None, apikey: str = None, sandbox: bool = False, host: str = None):
        """
        Инициализация API клиента.
        :param token: Bearer-токен True API
        :param apikey: API Key Национального каталога
        :param sandbox: использовать тестовую среду
        :param host: переопределение базового URL API
        """
        self.token = token or os.getenv("TRUE_API_TOKEN")
        self.apikey = apikey or os.getenv("API_KEY")
        self.sandbox = sandbox

        if not self.token and not self.apikey:
            raise ValueError("Не найден ни token, ни apikey. "
                             "Установите переменные TRUE_API_TOKEN или API_KEY.")

        # Правильный хост API
        if host:
            self.base_url = host
        elif sandbox:
            self.base_url = "https://api.nk.sandbox.crptech.ru"
        else:
            self.base_url = os.getenv("NK_API_HOST", "https://xn--80aqu.xn----7sbabas4ajkhfocclk9d3cvfsa.xn--p1ai")

    def _true_api_base_url(self) -> str:
        if self.sandbox:
            return "https://markirovka.sandbox.crptech.ru/api/v4/true-api"
        return "https://markirovka.crpt.ru/api/v4/true-api"

    def _true_api_headers(self) -> Dict[str, str]:
        if not self.token:
            raise ValueError("Для True API нужен Bearer token")
        return {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.token}",
        }

    def _parse_iso_date(self, value: Optional[str]) -> Optional[date]:
        if not value:
            return None
        try:
            return datetime.strptime(value, "%Y-%m-%d").date()
        except:
            return None

    def _doc_is_active_by_fields(self, doc: Dict[str, Any], on_date: date) -> bool:
        if isinstance(doc.get("active"), bool):
            return doc["active"]

        date_from = self._parse_iso_date(doc.get("date") or doc.get("dateFrom"))
        date_to = self._parse_iso_date(doc.get("dateTo"))

        if date_from and date_from > on_date:
            return False
        if date_to and date_to < on_date:
            return False

        return bool(date_from or date_to or doc.get("unlimited") is True)

    def _rd_list_key(self, doc: Dict[str, Any]) -> tuple:
        # For declaration/certificate dateFrom disambiguates documents.
        # For SGR-like docs API may ignore dateFrom.
        return (
            doc.get("type"),
            doc.get("number"),
            doc.get("date") or doc.get("dateFrom") or "",
        )

    def _get_rd_list_statuses(self, docs: List[Dict[str, Any]]) -> Dict[tuple, Dict[str, Any]]:
        if not docs:
            return {}

        result = {}
        url = f"{self._true_api_base_url()}/rd/list"

        payload_docs = []
        for doc in docs:
            doc_type = doc.get("type")
            number = doc.get("number")
            doc_date = doc.get("date") or doc.get("dateFrom")

            if doc_type not in self.TRUE_API_PERMIT_TYPES or not number:
                continue

            item = {"type": doc_type, "number": number}
            if doc_type in self.RD_LIST_DATEFROM_REQUIRED:
                if not doc_date:
                    continue
                item["dateFrom"] = doc_date
            elif doc_date:
                item["dateFrom"] = doc_date

            payload_docs.append(item)

        # /rd/list accepts max 25 documents.
        for i in range(0, len(payload_docs), 25):
            chunk = payload_docs[i:i + 25]
            logger.info(f"POST {url} (/rd/list docs: {len(chunk)})")
            try:
                response = requests.post(
                    url,
                    headers=self._true_api_headers(),
                    json={"documents": chunk},
                    timeout=30,
                )
                logger.info(f"Status: {response.status_code}")

                if response.status_code != 200:
                    logger.warning(f"/rd/list failed: {response.status_code} {response.text}")
                    continue

                data = response.json()
                for rd_doc in data.get("result", {}).get("documents", []):
                    key = (
                        rd_doc.get("type"),
                        rd_doc.get("number"),
                        rd_doc.get("dateFrom") or "",
                    )
                    result[key] = rd_doc

                for err in data.get("result", {}).get("errors", []):
                    logger.info(f"/rd/list error for {err.get('number')}: {err.get('message')}")
            except Exception as e:
                logger.error(f"Error in _get_rd_list_statuses: {e}")

        return result

    def product_info(self, gtin: str, rd_info: bool = False) -> Optional[Dict[str, Any]]:
        """
        Метод «/v4/true-api/product/info» возвращает расширенную информацию о товаре.
        """
        url = f"{self._true_api_base_url()}/product/info"
        payload = {"gtins": [gtin], "rdInfo": rd_info}

        logger.info(f"POST {url} (GTIN: {gtin}, rdInfo={rd_info})")
        try:
            response = requests.post(
                url,
                headers=self._true_api_headers(),
                json=payload,
                timeout=30,
            )
            logger.info(f"Status: {response.status_code}")

            if response.status_code != 200:
                logger.error(f"Ошибка product/info: {response.status_code} {response.text}")
                return None

            data = response.json()
            results = data.get("results") or []
            if not results:
                logger.info(f"GTIN {gtin}: product/info не вернул карточку")
                return None

            return results[0]
        except Exception as e:
            logger.error(f"Error calling product/info: {e}")
            return None

    def get_active_permit_documents_by_gtin(
        self,
        gtin: str,
        on_date: Optional[date] = None,
        verify_registry_status: bool = True,
        product: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Возвращает активные разрешительные документы по GTIN.

        Работает и для собственных GTIN, и для GTIN, предоставленных через субаккаунт,
        если текущий token имеет доступ к карточке.

        Источник списка документов:
          POST /api/v4/true-api/product/info с rdInfo=true

        Уточнение статуса:
          POST /api/v4/true-api/rd/list, если verify_registry_status=True.
          Если /rd/list не вернул документ, используется active/date/dateTo из product/info.
        """
        on_date = on_date or date.today()

        if not product:
            product = self.product_info(gtin, rd_info=True)
            if not product:
                return []
        cert_docs = product.get("certDocList") or []
        if not cert_docs:
            logger.info(f"GTIN {gtin}: certDocList пустой")
            return []

        registry_by_key = {}
        if verify_registry_status:
            registry_by_key = self._get_rd_list_statuses(cert_docs)

        active_docs = []
        for doc in cert_docs:
            key = self._rd_list_key(doc)
            registry_doc = registry_by_key.get(key)

            merged = dict(doc)
            if registry_doc:
                merged.update({
                    "registryStatus": registry_doc.get("status"),
                    "indx": registry_doc.get("indx"),
                    "date": registry_doc.get("dateFrom") or merged.get("date"),
                    "dateTo": registry_doc.get("dateTo") or merged.get("dateTo"),
                    "registryRaw": registry_doc,
                })

            if merged.get("registryStatus"):
                is_active = merged["registryStatus"] == "Действует"
            else:
                is_active = self._doc_is_active_by_fields(merged, on_date)

            if is_active:
                active_docs.append({
                    "gtin": product.get("gtin"),
                    "productName": product.get("name"),
                    "ownerInn": product.get("inn"),
                    "type": merged.get("type"),
                    "number": merged.get("number"),
                    "date": merged.get("date") or merged.get("dateFrom"),
                    "dateTo": merged.get("dateTo"),
                    "active": merged.get("active"),
                    "registryStatus": merged.get("registryStatus"),
                    "indx": merged.get("indx"),
                    "raw": merged,
                })

        return active_docs

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
            logger.info(f"Полный ответ API для GTIN {gtin}: {json.dumps(data, ensure_ascii=False)}")
        except Exception as e:
            logger.error(f"Ошибка декодирования JSON: {e}")
            return []

        documents = []
        try:
            result = data.get("result", {})
            if isinstance(result, list) and len(result) > 0:
                result = result[0]

            docs = result.get("documents", [])
            errors = result.get("errors", [])
            logger.info(f'Gtin:{gtin} error:{errors}')

            CERT_TYPE_MAP = {
                23557: "CONFORMITY_DECLARATION",
                23561: "CONFORMITY_CERTIFICATE",
                23765: "STATE_REGISTRATION_CERTIFICATE"
            }

            for d in docs:
                if d.get("status") == "Прекращен":
                    logger.info(f"Документ {d.get('number')} пропущен (статус: Прекращен)")
                    continue

                number = d.get("number")
                from_date = d.get("from_date")
                attr_id = d.get("attr_id")
                type_doc = CERT_TYPE_MAP.get(attr_id)

                if not type_doc:
                    # Резервный вариант, если attr_id не в мапе
                    type_doc = d.get("type") or d.get("product_type")

                documents.append(GtinDocument(
                    certificate_number=number,
                    certificate_date=from_date,
                    certificate_type=type_doc
                ))
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
