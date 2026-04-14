import os
import sys
import json
import time
import argparse
import uuid
import logging
import inspect
from pathlib import Path

# Добавляем текущую директорию в путь поиска модулей
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from suz_api_models import (
    EmissionOrder, OrderAttributes, OrderProduct, EmissionOrderreceipts,
    EmissionOrderStatus, ProductionOrder, PasportData,
    UtilisationReport, UtilisationReportReceipt
)
from suz import SUZ
from gs1_processor import get_inn_by_gtin
from tokens import TokenProcessor
from org_manager import OrganizationManager
from storage import get_storage, LocalStorage, S3Storage
from config_loader import load_config

# --- НАСТРОЙКИ ПО УМОЛЧАНИЮ ---
SIGNING_DIR = os.path.join(os.path.expanduser("~"), "tst")
SIGNING_TIMEOUT = 60
# ------------------------------

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_emission_task(production_order_id: str, group: str, contact: str):
    """
    Создает структуру заказа на эмиссию на основе производственного заказа из S3
    и сохраняет её обратно в S3.
    """
    try:
        config = load_config('suz_worker_config')
        s3_config = config.get('s3_config')
        production_orders_path = config.get('production_orders_path')
        emission_orders_path = config.get('emission_orders_path')

        if not all([production_orders_path, emission_orders_path]):
            logger.error("[!] В конфигурации отсутствуют необходимые пути (production_orders_path, emission_orders_path)")
            return None

        storage_prod = get_storage(production_orders_path, s3_config)
        prod_order_path = f"{production_orders_path.rstrip('/')}/{production_order_id}.json"

        if not storage_prod.exists(prod_order_path):
            logger.error(f"[!] Файл производственного заказа не найден: {prod_order_path}")
            return None

        prod_data = json.loads(storage_prod.read_text(prod_order_path))
        gtin = prod_data.get('Gtin')
        quantity = int(prod_data.get('Quantity', 0))

        if not gtin or not quantity:
            logger.error(f"[!] Некорректные данные в производственном заказе {production_order_id}: gtin={gtin}, quantity={quantity}")
            return None

        # Определяем ИНН по GTIN
        base_path = os.path.dirname(os.path.abspath(__file__))
        db_path = os.path.join(base_path, 'gs1prefix_inn_db.json')
        inn = get_inn_by_gtin(gtin, db_path=db_path)

        if not inn:
            logger.error(f"[!] Не удалось определить ИНН для GTIN {gtin}. Проверьте {db_path}")
            return None

        logger.info(f"[*] Определен ИНН: {inn} для GTIN: {gtin}")

        # Формируем структуру заказа
        attr = OrderAttributes(
            productionOrderId=production_order_id,
            createMethodType="SELF_MADE",
            releaseMethodType="PRODUCTION",
            paymentType=2,
            contactPerson=contact
        )

        product = OrderProduct(
            gtin=gtin,
            quantity=quantity,
            serialNumberType="OPERATOR",
            templateId=47,
            cisType="UNIT"
        )

        order = EmissionOrder(
            productGroup=group,
            attributes=attr,
            products=[product]
        )

        # Сохраняем в S3
        storage_emission = get_storage(emission_orders_path, s3_config)
        remote_path = f"{emission_orders_path.rstrip('/')}/{production_order_id}.json"

        # Временный локальный файл
        temp_local = Path(f"temp_order_{production_order_id}.json")
        with open(temp_local, 'w', encoding='utf-8') as f:
            f.write(order.to_json())

        logger.info(f"[*] Выгрузка заказа на эмиссию в S3: {remote_path}")
        storage_emission.upload(str(temp_local), remote_path)

        try: temp_local.unlink()
        except: pass

        return production_order_id

    except Exception as e:
        logger.error(f"[!] Ошибка в create_emission_task: {e}")
        return None

def sign_and_send_emission(production_order_id: str, signing_dir: str, timeout: int,
                  oms_id: str = None, client_token: str = None):
    """
    Загружает заказ из S3, подписывает его через файловый обмен и отправляет в СУЗ
    """
    try:
        config = load_config('suz_worker_config')
        s3_config = config.get('s3_config')
        emission_orders_path = config.get('emission_orders_path')
        emission_receipts_path = config.get('emission_receipts')

        if not all([emission_orders_path, emission_receipts_path]):
            logger.error(f"[!] В конфигурации отсутствуют необходимые пути (emission_orders_path, emission_receipts)")
            return None

        storage_orders = get_storage(emission_orders_path, s3_config)
        order_path = f"{emission_orders_path.rstrip('/')}/{production_order_id}.json"

        if not storage_orders.exists(order_path):
            logger.error(f"[!] Файл заказа не найден: {order_path}")
            return None

        # 1. Устанавливаем тег status:processing
        logger.info(f"[*] Пометка заказа {production_order_id} как processing")
        storage_orders.mark_processing(order_path)

        # Загружаем заказ
        order_content = storage_orders.read_text(order_path)
        order_data = json.loads(order_content)

        # Нам нужен ИНН для подписи. Мы можем его получить по GTIN из заказа.
        gtin = order_data['products'][0]['gtin']
        base_path = os.path.dirname(os.path.abspath(__file__))
        db_path = os.path.join(base_path, 'gs1prefix_inn_db.json')
        inn = get_inn_by_gtin(gtin, db_path=db_path)

        if not inn:
            logger.error(f"[!] Не удалось определить ИНН для GTIN {gtin}")
            storage_orders.mark_error(order_path)
            return None

        # Сначала проверяем учетные данные
        org_manager = OrganizationManager(os.path.join(base_path, 'my_orgs'))

        # Поиск подходящей организации
        final_oms_id = oms_id
        final_client_token = client_token

        if not final_oms_id or not final_client_token:
            found_org = None
            for o in org_manager.list():
                if o.inn == inn and o.oms_id:
                    found_org = o
                    break

            if not found_org:
                found_org = org_manager.find(inn=inn)

            if found_org:
                logger.info(f"[*] Используется профиль организации: {found_org.name}")
                final_oms_id = final_oms_id or found_org.oms_id
                final_client_token = final_client_token or found_org.connection_id

        if not final_oms_id or not final_client_token:
            logger.error(f"[!] Недостаточно данных для ИНН {inn} (OMS ID или Client Token)")
            storage_orders.mark_error(order_path)
            return None

        token_processor = TokenProcessor(org_manager=org_manager)
        token = token_processor.get_token_value_by_inn(inn, token_type='UUID', conid=final_client_token)

        if not token:
            logger.error(f"[!] Активный токен для ИНН {inn} и Connection ID {final_client_token} не найден.")
            storage_orders.mark_error(order_path)
            return None

        # 2. Подготовка к подписи
        work_dir = Path(signing_dir)
        work_dir.mkdir(parents=True, exist_ok=True)

        unique_id = uuid.uuid4()
        body_filename = f"{inn}_{unique_id}_order.json"
        body_path = work_dir / body_filename
        signature_path = work_dir / f"{body_filename}.sig"

        try:
            with open(body_path, "w", encoding="utf-8") as f:
                # СУЗ требует компактный JSON без пробелов между ключами
                json.dump(order_data, f, ensure_ascii=False, separators=(',', ':'))

            logger.info(f"[*] Файл заказа сохранен в: {body_path}. Ожидание подписи...")

            start_time = time.time()
            while not signature_path.exists():
                if time.time() - start_time > timeout:
                    logger.error(f"[!] Таймаут ({timeout}с): Файл подписи {signature_path.name} не найден.")
                    storage_orders.mark_error(order_path)
                    return None
                time.sleep(1)

            time.sleep(0.5)
            logger.info("[+] Подпись обнаружена!")

            # 3. Отправка в СУЗ
            suz_api = SUZ(token=token, omsId=final_oms_id, clientToken=final_client_token)
            logger.info(f"[*] Отправка заказа в СУЗ (omsId: {final_oms_id})...")
            result = suz_api.order_create(str(body_path), str(signature_path))

            # 4. Сохранение результата и обновление статуса
            if isinstance(result, EmissionOrderreceipts):
                logger.info("[+++] Заказ успешно создан!")
                storage_orders.mark_finished(order_path)

                if emission_receipts_path:
                    storage_receipts = get_storage(emission_receipts_path, s3_config)
                    remote_receipt_path = f"{emission_receipts_path.rstrip('/')}/{production_order_id}.json"

                    temp_receipt = work_dir / f"receipt_{unique_id}.json"
                    with open(temp_receipt, 'w', encoding='utf-8') as f:
                        json.dump(result.to_dict(), f, ensure_ascii=False, indent=4)

                    logger.info(f"[*] Выгрузка чека в S3: {remote_receipt_path}")
                    storage_receipts.upload(str(temp_receipt), remote_receipt_path)
                    try: temp_receipt.unlink()
                    except: pass

                return result
            else:
                logger.error(f"[!] Ошибка СУЗ: {result}")
                storage_orders.mark_error(order_path)
                return result

        finally:
            if body_path.exists(): body_path.unlink()
            if signature_path.exists(): signature_path.unlink()

    except Exception as e:
        logger.error(f"[!] Ошибка в sign_and_send_emission: {e}")
        return None

def get_emission_kodes(order_id: str):
    """
    Получает коды маркировки для заказа, если он в статусе ACTIVE и еще не обрабатывался.
    """
    MAX_CODES_QTY = 10000
    try:
        config = load_config('suz_worker_config')
        s3_config = config.get('s3_config')
        emissions_path = config.get('emissions_path')
        kodes_path = config.get('kodes')

        if not all([emissions_path, kodes_path]):
            logger.error("[!] В конфигурации отсутствуют необходимые пути (emissions_path, kodes)")
            return None

        storage_emissions = get_storage(emissions_path, s3_config)

        # Поиск файла статуса
        target_path = None
        if isinstance(storage_emissions, LocalStorage):
            p = Path(emissions_path)
            if not p.exists():
                logger.error(f"[!] Директория {emissions_path} не существует.")
                return None
            matches = list(p.glob(f"{order_id}.*"))
            if matches:
                target_path = str(matches[0])
        else:
            target_path = f"{emissions_path.rstrip('/')}/{order_id}.json"
            if not storage_emissions.exists(target_path):
                target_path = None

        if not target_path:
            logger.error(f"[!] Файл статуса для orderId {order_id} не найден в {emissions_path}")
            return None

        # Проверка на status:processing / finished
        is_processing = False
        if isinstance(storage_emissions, S3Storage):
            bucket, key = storage_emissions._parse_s3_url(target_path)
            try:
                resp = storage_emissions.s3.get_object_tagging(Bucket=bucket, Key=key)
                tags = {t['Key']: t['Value'] for t in resp.get('TagSet', [])}
                if tags.get('status') == 'processing':
                    is_processing = True
                if tags.get('status') == 'finished':
                    logger.info(f"[*] Заказ {order_id} уже обработан (status:finished).")
                    return None
            except Exception as e:
                logger.error(f"Ошибка при проверке тегов S3: {e}")
        elif isinstance(storage_emissions, LocalStorage):
            if target_path.endswith('.processing'):
                is_processing = True
            if target_path.endswith('.finished'):
                logger.info(f"[*] Заказ {order_id} уже обработан (status:finished).")
                return None

        if is_processing:
            logger.info(f"[*] Заказ {order_id} уже в обработке.")
            return None

        # Читаем данные из файла статуса
        status_content = storage_emissions.read_text(target_path)
        status_data = json.loads(status_content)
        gtin = status_data.get('gtin')
        oms_id = status_data.get('omsId')

        if not gtin or not oms_id:
            logger.error(f"[!] Не удалось получить gtin или omsId из файла {target_path}")
            return None

        # Инициализация API
        base_path = os.path.dirname(os.path.abspath(__file__))
        org_manager = OrganizationManager(os.path.join(base_path, 'my_orgs'))

        found_org = None
        for o in org_manager.list():
            if o.oms_id == oms_id:
                found_org = o
                break

        if not found_org:
            logger.error(f"[!] Организация с omsId {oms_id} не найдена в базе my_orgs.")
            return None

        token_processor = TokenProcessor(org_manager=org_manager)
        token = token_processor.get_token_value_by_inn(found_org.inn, token_type='UUID', conid=found_org.connection_id)
        if not token:
            logger.error(f"[!] Активный UUID токен для ИНН {found_org.inn} не найден.")
            return None

        suz_api = SUZ(token=token, omsId=oms_id, clientToken=found_org.connection_id)

        # Проверяем статус в СУЗ
        logger.info(f"[*] Запрос актуального статуса из СУЗ для orderId: {order_id}, gtin: {gtin}")
        try:
            api_status_res = suz_api.order_status(order_id, gtin)
        except Exception as api_err:
            logger.error(f"[!] Ошибка API при запросе статуса: {api_err}")
            return None

        if not api_status_res or not isinstance(api_status_res, list):
            logger.error(f"[!] Некорректный ответ от API: {api_status_res}")
            return None

        api_status = api_status_res[0]
        if api_status.get('bufferStatus') != 'ACTIVE':
            logger.info(f"[*] Заказ {order_id} имеет статус {api_status.get('bufferStatus')}, а не ACTIVE. Пропуск.")
            return None

        # Устанавливаем статус processing
        logger.info(f"[*] Пометка заказа {order_id} как processing")
        new_path = storage_emissions.mark_processing(target_path)
        if isinstance(storage_emissions, LocalStorage):
            target_path = new_path

        # Получаем коды
        available_codes = api_status.get('availableCodes', 0)

        if available_codes > MAX_CODES_QTY:
            error_msg = f"Количество доступных кодов ({available_codes}) превышает максимально допустимое ({MAX_CODES_QTY})"
            logger.error(f"[!] {error_msg}")
            storage_emissions.mark_error(target_path)
            raise ValueError(error_msg)

        if available_codes == 0:
            logger.info("[*] Доступных кодов нет (availableCodes=0). Завершение.")
            storage_emissions.mark_finished(target_path)
            return None

        logger.info(f"[*] Получение {available_codes} кодов из СУЗ...")
        try:
            codes_res = suz_api.codes(order_id, available_codes, gtin)
        except Exception as codes_err:
            logger.error(f"[!] Ошибка при получении кодов: {codes_err}")
            storage_emissions.mark_error(target_path)
            return None

        # Сохранение кодов
        storage_kodes = get_storage(kodes_path, s3_config)
        output_path = f"{kodes_path.rstrip('/')}/{order_id}.json"

        temp_file = Path(f"temp_codes_{order_id}.json")
        with open(temp_file, 'w', encoding='utf-8') as f:
            json.dump(codes_res, f, ensure_ascii=False, indent=4)

        logger.info(f"[*] Выгрузка кодов в: {output_path}")
        storage_kodes.upload(str(temp_file), output_path)

        # Пометка как finished
        logger.info(f"[*] Пометка заказа {order_id} как finished")
        storage_emissions.mark_finished(target_path)

        try: temp_file.unlink()
        except: pass

        return codes_res

    except Exception as e:
        logger.error(f"[!] Ошибка в get_emission_kodes: {e}")
        return None

def format_date_suz(date_str: str) -> str:
    """Преобразует дату из dd.mm.yyyy в yyyy-MM-dd"""
    if not date_str:
        return None
    try:
        # Пробуем dd.mm.yyyy
        if '.' in date_str:
            parts = date_str.split('.')
            if len(parts) == 3:
                return f"{parts[2]}-{parts[1]}-{parts[0]}"
        return date_str
    except:
        return date_str

def create_utilisation_task(order_id: str, group: str, production_date: str = None, expiration_date: str = None):
    """
    Создает задачу на отчет о нанесении на основе полученных кодов.
    Пытается автоматически найти даты в исходном производственном заказе.
    """
    try:
        config = load_config('suz_worker_config')
        s3_config = config.get('s3_config')
        kodes_path = config.get('kodes')
        utilisation_tasks_path = config.get('utilisation_tasks_path')
        emission_receipts_path = config.get('emission_receipts')
        production_orders_path = config.get('production_orders_path')

        if not all([kodes_path, utilisation_tasks_path]):
            logger.error("[!] В конфигурации отсутствуют пути (kodes, utilisation_tasks_path)")
            return None

        storage_kodes = get_storage(kodes_path, s3_config)
        kodes_file_path = f"{kodes_path.rstrip('/')}/{order_id}.json"

        if not storage_kodes.exists(kodes_file_path):
            logger.error(f"[!] Файл кодов не найден: {kodes_file_path}")
            return None

        codes_data = json.loads(storage_kodes.read_text(kodes_file_path))
        codes = codes_data.get('codes', [])

        if not codes:
            logger.error(f"[!] В файле {kodes_file_path} нет кодов")
            return None

        # 1. Попытка найти даты автоматически
        auto_prod_date = production_date
        auto_exp_date = expiration_date
        manufacturer_inn = None

        if not auto_prod_date or not auto_exp_date:
            logger.info(f"[*] Поиск дат в исходном заказе для orderId: {order_id}")
            # Пытаемся найти соответствие orderId -> productionOrderId
            storage_receipts = get_storage(emission_receipts_path, s3_config)
            # В S3Receipts файлы именуются по productionOrderId, поэтому придется искать перебором
            # (или если у нас есть база соответствий, но тут мы ищем в S3)
            # Для простоты примера предположим, что мы можем найти production_order_id

            # Если не переданы даты, попробуем найти productionOrderId по содержимому чеков
            production_order_id = None
            if isinstance(storage_receipts, LocalStorage):
                 for f in Path(emission_receipts_path).glob("*.json"):
                     try:
                         data = json.loads(f.read_text())
                         if data.get('orderId') == order_id:
                             production_order_id = f.stem
                             break
                     except: continue

            if production_order_id:
                storage_prod = get_storage(production_orders_path, s3_config)
                prod_path = f"{production_orders_path.rstrip('/')}/{production_order_id}.json"
                if storage_prod.exists(prod_path):
                    prod_data = json.loads(storage_prod.read_text(prod_path))
                    pasport = prod_data.get('PasportData', {})
                    if not auto_prod_date:
                        auto_prod_date = format_date_suz(pasport.get('Batch_date_production'))
                    if not auto_exp_date:
                        auto_exp_date = format_date_suz(pasport.get('Batch_date_expired'))
                    manufacturer_inn = pasport.get('Manufacturer_inn')
                    logger.info(f"[+] Найдены даты: prod={auto_prod_date}, exp={auto_exp_date}")

        # Определяем GTIN по первому коду (01 + 14 цифр)
        first_code = codes[0]
        if first_code.startswith('01'):
            gtin = first_code[2:16]
        else:
            logger.error(f"[!] Не удалось определить GTIN из кода: {first_code}")
            return None

        # Формируем атрибуты
        attributes = {}
        # participantId ИСКЛЮЧЕН, так как это вызывает ошибку 400 в СУЗ,
        # но если мы нашли manufacturer_inn, можно попробовать (или оставить исключенным)
        # В примере пользователя participantId отсутствовал в успешно принятом JSON.

        if auto_prod_date:
            attributes["productionDate"] = auto_prod_date
        if auto_exp_date:
            attributes["expirationDate"] = auto_exp_date

        report = UtilisationReport(
            productGroup=group,
            sntins=codes,
            attributes=attributes
        )

        # Сохраняем в S3
        storage_util = get_storage(utilisation_tasks_path, s3_config)
        remote_path = f"{utilisation_tasks_path.rstrip('/')}/{order_id}.json"

        temp_local = Path(f"temp_util_{order_id}.json")
        with open(temp_local, 'w', encoding='utf-8') as f:
            f.write(report.to_json())

        logger.info(f"[*] Выгрузка задачи на отчет о нанесении в S3: {remote_path}")
        storage_util.upload(str(temp_local), remote_path)

        try: temp_local.unlink()
        except: pass

        return order_id

    except Exception as e:
        logger.error(f"[!] Ошибка в create_utilisation_task: {e}")
        return None

def sign_and_send_utilisation(order_id: str, signing_dir: str, timeout: int,
                            oms_id: str = None, client_token: str = None):
    """
    Загружает задачу отчета о нанесении, подписывает и отправляет в СУЗ
    """
    try:
        config = load_config('suz_worker_config')
        s3_config = config.get('s3_config')
        utilisation_tasks_path = config.get('utilisation_tasks_path')
        utilisation_receipts_path = config.get('utilisation_receipts')

        if not all([utilisation_tasks_path, utilisation_receipts_path]):
            logger.error(f"[!] В конфигурации отсутствуют пути (utilisation_tasks_path, utilisation_receipts)")
            return None

        storage_tasks = get_storage(utilisation_tasks_path, s3_config)
        task_path = f"{utilisation_tasks_path.rstrip('/')}/{order_id}.json"

        if not storage_tasks.exists(task_path):
            logger.error(f"[!] Файл задачи не найден: {task_path}")
            return None

        # Помечаем как processing
        storage_tasks.mark_processing(task_path)

        task_content = storage_tasks.read_text(task_path)
        task_data = json.loads(task_content)

        # Получаем ИНН из атрибутов или по GTIN
        inn = task_data.get('attributes', {}).get('participantId')
        if not inn:
             first_code = task_data['sntins'][0]
             gtin = first_code[2:16] if first_code.startswith('01') else None
             base_path = os.path.dirname(os.path.abspath(__file__))
             inn = get_inn_by_gtin(gtin, db_path=os.path.join(base_path, 'gs1prefix_inn_db.json'))

        if not inn:
            logger.error(f"[!] Не удалось определить ИНН для задачи {order_id}")
            storage_tasks.mark_error(task_path)
            return None

        # Разрешение учетных данных
        base_path = os.path.dirname(os.path.abspath(__file__))
        org_manager = OrganizationManager(os.path.join(base_path, 'my_orgs'))

        final_oms_id = oms_id
        final_client_token = client_token

        if not final_oms_id or not final_client_token:
            found_org = None
            for o in org_manager.list():
                if o.inn == inn and o.oms_id:
                    found_org = o
                    break
            if not found_org:
                found_org = org_manager.find(inn=inn)

            if found_org:
                final_oms_id = final_oms_id or found_org.oms_id
                final_client_token = final_client_token or found_org.connection_id

        if not final_oms_id or not final_client_token:
            logger.error(f"[!] Недостаточно данных для ИНН {inn}")
            storage_tasks.mark_error(task_path)
            return None

        token_processor = TokenProcessor(org_manager=org_manager)
        token = token_processor.get_token_value_by_inn(inn, token_type='UUID', conid=final_client_token)

        if not token:
            logger.error(f"[!] Токен не найден.")
            storage_tasks.mark_error(task_path)
            return None

        # Подпись
        work_dir = Path(signing_dir)
        work_dir.mkdir(parents=True, exist_ok=True)
        unique_id = uuid.uuid4()
        body_filename = f"{inn}_{unique_id}_utilisation.json"
        body_path = work_dir / body_filename
        signature_path = work_dir / f"{body_filename}.sig"

        try:
            with open(body_path, "w", encoding="utf-8") as f:
                json.dump(task_data, f, ensure_ascii=False, separators=(',', ':'))

            logger.info(f"[*] Ожидание подписи для {body_path}...")
            start_time = time.time()
            while not signature_path.exists():
                if time.time() - start_time > timeout:
                    logger.error("[!] Таймаут ожидания подписи.")
                    storage_tasks.mark_error(task_path)
                    return None
                time.sleep(1)

            # Отправка
            suz_api = SUZ(token=token, omsId=final_oms_id, clientToken=final_client_token)
            logger.info(f"[*] Отправка отчета в СУЗ (orderId: {order_id})...")
            report_id = suz_api.utilisation_send(str(body_path), str(signature_path), orderId=order_id)

            if report_id and not report_id.startswith('{') and 'Error' not in report_id:
                logger.info(f"[+++] Отчет принят! ID: {report_id}")
                storage_tasks.mark_finished(task_path)

                storage_receipts = get_storage(utilisation_receipts_path, s3_config)
                remote_receipt_path = f"{utilisation_receipts_path.rstrip('/')}/{order_id}.json"

                temp_receipt = work_dir / f"receipt_util_{unique_id}.json"
                with open(temp_receipt, 'w', encoding='utf-8') as f:
                    json.dump({"reportId": report_id, "orderId": order_id, "omsId": final_oms_id}, f, indent=4)

                storage_receipts.upload(str(temp_receipt), remote_receipt_path)
                try: temp_receipt.unlink()
                except: pass

                return report_id
            else:
                logger.error(f"[!] Ошибка СУЗ: {report_id}")
                storage_tasks.mark_error(task_path)
                return report_id

        finally:
            if body_path.exists(): body_path.unlink()
            if signature_path.exists(): signature_path.unlink()

    except Exception as e:
        logger.error(f"[!] Ошибка в sign_and_send_utilisation: {e}")
        return None

def update_emission_order_status(production_order_id: str):
    """
    Получает статус заказа из СУЗ и сохраняет его в S3
    """
    try:
        config = load_config('suz_worker_config')
        s3_config = config.get('s3_config')
        emission_receipts_path = config.get('emission_receipts')
        production_orders_path = config.get('production_orders_path')
        emissions_path = config.get('emissions_path')

        if not all([emission_receipts_path, production_orders_path, emissions_path]):
            logger.error("[!] В конфигурации отсутствуют необходимые пути (emission_receipts, production_orders_path, emissions_path)")
            return None

        # 1. Загружаем чек заказа на эмиссию
        storage_receipts = get_storage(emission_receipts_path, s3_config)
        receipt_path = f"{emission_receipts_path.rstrip('/')}/{production_order_id}.json"

        if not storage_receipts.exists(receipt_path):
            logger.error(f"[!] Файл чека не найден: {receipt_path}")
            return None

        receipt_data = json.loads(storage_receipts.read_text(receipt_path))
        order_id = receipt_data.get('orderId')
        oms_id = receipt_data.get('omsId')

        # 2. Загружаем производственный заказ для получения GTIN
        storage_production = get_storage(production_orders_path, s3_config)
        prod_order_path = f"{production_orders_path.rstrip('/')}/{production_order_id}.json"

        if not storage_production.exists(prod_order_path):
            logger.error(f"[!] Файл производственного заказа не найден: {prod_order_path}")
            return None

        prod_data = json.loads(storage_production.read_text(prod_order_path))
        gtin = prod_data.get('Gtin')

        if not all([order_id, oms_id, gtin]):
            logger.error(f"[!] Не удалось получить все данные (orderId, omsId, gtin) для {production_order_id}")
            return None

        # 3. Инициализация API и получение статуса
        base_path = os.path.dirname(os.path.abspath(__file__))
        org_manager = OrganizationManager(os.path.join(base_path, 'my_orgs'))

        # Ищем организацию по oms_id
        found_org = None
        for o in org_manager.list():
            if o.oms_id == oms_id:
                found_org = o
                break

        if not found_org:
            logger.error(f"[!] Организация с omsId {oms_id} не найдена в базе.")
            return None

        token_processor = TokenProcessor(org_manager=org_manager)
        token = token_processor.get_token_value_by_inn(found_org.inn, token_type='UUID', conid=found_org.connection_id)

        if not token:
            logger.error(f"[!] Активный токен для ИНН {found_org.inn} не найден.")
            return None

        suz_api = SUZ(token=token, omsId=oms_id, clientToken=found_org.connection_id)
        logger.info(f"[*] Запрос статуса для orderId: {order_id}, gtin: {gtin}")

        try:
            status_response = suz_api.order_status(order_id, gtin)
        except Exception as api_err:
            logger.error(f"[!] Ошибка при запросе статуса: {api_err}")
            return str(api_err)

        if not status_response or not isinstance(status_response, list):
            logger.error(f"[!] Получен некорректный ответ от СУЗ: {status_response}")
            return str(status_response)

        # Берем первый элемент статуса
        status_data = status_response[0]

        # Фильтруем поля для инициализации dataclass, на случай появления новых полей в API
        sig = inspect.signature(EmissionOrderStatus.__init__)
        valid_fields = {k for k, v in sig.parameters.items() if k != 'self'}
        filtered_data = {k: v for k, v in status_data.items() if k in valid_fields}

        status_obj = EmissionOrderStatus(**filtered_data)

        # 4. Сохранение результата
        storage_emissions = get_storage(emissions_path, s3_config)
        output_filename = f"{order_id}.json"
        output_path = f"{emissions_path.rstrip('/')}/{output_filename}"

        # Временный локальный файл
        temp_local = Path(f"temp_status_{order_id}.json")
        with open(temp_local, 'w', encoding='utf-8') as f:
            f.write(status_obj.to_json())

        logger.info(f"[*] Сохранение статуса в {output_path}")
        storage_emissions.upload(str(temp_local), output_path)

        # Установка тегов/расширения
        storage_emissions.set_tags(output_path, {"bufferStatus": status_obj.bufferStatus})

        try: temp_local.unlink()
        except: pass

        return status_obj

    except Exception as e:
        logger.error(f"[!] Ошибка в update_emission_order_status: {e}")
        return str(e)

def main():
    parser = argparse.ArgumentParser(description="Создание, подпись и отправка заказа на эмиссию КМ в СУЗ")
    parser.add_argument("--create-task", help="Создать задачу на эмиссию по productionOrderId")
    parser.add_argument("--send-task", help="Подписать и отправить задачу на эмиссию по productionOrderId")
    parser.add_argument("--create-utilisation", help="Создать задачу на отчет о нанесении по orderId (UUID)")
    parser.add_argument("--send-utilisation", help="Подписать и отправить отчет о нанесении по orderId (UUID)")

    parser.add_argument("--group", default="chemistry", help="Товарная группа (например: chemistry, perfumes, clothes...)")
    parser.add_argument("--contact", default="хТрек 2.5.11.6", help="Контактное лицо в заказе")
    parser.add_argument("--oms_id", help="OMS ID (если не задан, будет найден в my_orgs по ИНН)")
    parser.add_argument("--client_token", help="Client Token / Connection ID")
    parser.add_argument("--signing_dir", default=SIGNING_DIR, help="Директория для обмена с демоном подписи")
    parser.add_argument("--timeout", type=int, default=SIGNING_TIMEOUT, help="Тайм-аут ожидания подписи (сек)")
    parser.add_argument("--status", help="Получить статус заказа по productionOrderId")
    parser.add_argument("--production-date", help="Дата производства для отчета (yyyy-MM-dd)")
    parser.add_argument("--expiration-date", help="Дата истечения срока годности для отчета (yyyy-MM-dd)")
    parser.add_argument("--get-codes", help="Получить коды для заказа по orderId (UUID)")

    args = parser.parse_args()

    if args.status:
        result = update_emission_order_status(args.status)
        if isinstance(result, EmissionOrderStatus):
            print(json.dumps(result.to_dict(), indent=2, ensure_ascii=False))
        else:
            print(f"Результат: {result}")
        return

    if args.get_codes:
        result = get_emission_kodes(args.get_codes)
        if result:
            print(f"Успешно получено кодов: {len(result.get('codes', []))}")
        else:
            print("Не удалось получить коды.")
        return

    if args.create_task:
        result = create_emission_task(args.create_task, args.group, args.contact)
        if result:
            logger.info(f"[+++] Задача на эмиссию успешно создана для {result}")
        else:
            logger.error(f"[!] Не удалось создать задачу на эмиссию для {args.create_task}")
        return

    if args.send_task:
        result = sign_and_send_emission(args.send_task, args.signing_dir, args.timeout,
                                     oms_id=args.oms_id, client_token=args.client_token)
        if isinstance(result, EmissionOrderreceipts):
            logger.info("[+++] Заказ успешно отправлен в СУЗ!")
            print(json.dumps(result.to_dict(), indent=2, ensure_ascii=False))
        elif result:
            logger.error(f"[!] Не удалось отправить заказ. Ответ СУЗ: {result}")
        else:
            logger.error("[!] Не удалось отправить заказ (см. логи выше).")
        return

    if args.create_utilisation:
        result = create_utilisation_task(args.create_utilisation, args.group,
                                       production_date=args.production_date,
                                       expiration_date=args.expiration_date)
        if result:
            logger.info(f"[+++] Задача на отчет о нанесении успешно создана для {result}")
        else:
            logger.error(f"[!] Не удалось создать задачу для {args.create_utilisation}")
        return

    if args.send_utilisation:
        result = sign_and_send_utilisation(args.send_utilisation, args.signing_dir, args.timeout,
                                         oms_id=args.oms_id, client_token=args.client_token)
        if result and not result.startswith('{') and 'Error' not in result:
             logger.info(f"[+++] Отчет о нанесении успешно отправлен! ID: {result}")
        elif result:
            logger.error(f"[!] Ошибка отправки: {result}")
        return

    parser.print_help()

if __name__ == "__main__":
    main()
