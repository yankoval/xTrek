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

import base64
from suz_api_models import (
    EmissionOrder, OrderAttributes, OrderProduct, EmissionOrderreceipts,
    EmissionOrderStatus, ProductionOrder, PasportData,
    UtilisationReport, UtilisationReportReceipt, UtilisationReportStatus,
    AggregationReport, AggregationUnit, EquipmentAggTask, EquipmentAggTaskReport,
    EquipmentAggBox, DocumentWrapper
)
from suz import SUZ
from trueapi import HonestSignAPI
from gs1_processor import get_inn_by_gtin
from tokens import TokenProcessor
from crpt_auth import get_new_token
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

        # Количество в заказе указано в коробках.
        # Чтобы получить количество КМ, умножаем на количество в одной коробке (Product_PackQty)
        boxes_qty = int(prod_data.get('Quantity', 0))
        pack_qty_str = prod_data.get('PasportData', {}).get('Product_PackQty', '1')
        try:
            pack_qty = int(pack_qty_str)
        except (ValueError, TypeError):
            logger.warning(f"[!] Некорректный Product_PackQty: '{pack_qty_str}', используем 1")
            pack_qty = 1

        quantity = boxes_qty * pack_qty

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
                # СУЗ требует компактный JSON без пробелов между ключами.
                # Используем ensure_ascii=True (по умолчанию) для экранирования ASCII 29 как \u001d.
                json.dump(order_data, f, separators=(',', ':'))

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
                        json.dump(result.to_dict(), f, indent=4)

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
            json.dump(codes_res, f, indent=4)

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

        if not auto_prod_date or not auto_exp_date:
            logger.info(f"[*] Поиск дат в исходном заказе для orderId: {order_id}")
            storage_receipts = get_storage(emission_receipts_path, s3_config)
            production_order_id = None

            # Ищем во всех файлах чеков
            if isinstance(storage_receipts, LocalStorage):
                for f in Path(emission_receipts_path).glob("*.json"):
                    try:
                        content = f.read_text(encoding='utf-8')
                        data = json.loads(content)
                        if data.get('orderId') == order_id:
                            production_order_id = f.stem
                            if production_order_id.startswith('receipt_'):
                                production_order_id = production_order_id[len('receipt_'):]
                            logger.info(f"  -> Совпадение найдено в локальном файле: {f.name}, productionOrderId={production_order_id}")
                            break
                    except: continue
            else:
                # В S3 перебираем объекты в папке чеков
                try:
                    bucket, prefix = storage_receipts._parse_s3_url(emission_receipts_path)
                    res = storage_receipts.s3.list_objects_v2(Bucket=bucket, Prefix=prefix.strip('/') + '/')
                    for obj in res.get('Contents', []):
                        if obj['Key'].endswith('.json'):
                            content = storage_receipts.read_text(f"s3://{bucket}/{obj['Key']}")
                            data = json.loads(content)
                            if data.get('orderId') == order_id:
                                production_order_id = Path(obj['Key']).stem
                                if production_order_id.startswith('receipt_'):
                                    production_order_id = production_order_id[len('receipt_'):]
                                logger.info(f"  -> Совпадение найдено в S3: {obj['Key']}, productionOrderId={production_order_id}")
                                break
                except Exception as e:
                    logger.error(f"Ошибка при поиске в S3: {e}")

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
                    logger.info(f"[+] Извлечены даты: prod={auto_prod_date}, exp={auto_exp_date}")
                else:
                    logger.warning(f"[!] Файл производственного заказа {production_order_id} не найден по пути {prod_path}")

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
                # Важно: ensure_ascii=True для корректной передачи GS1-разделителей
                json.dump(task_data, f, separators=(',', ':'))

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

def create_aggregation_report(task_uuid: str, inn_override: str = None):
    """
    Создает отчет об агрегации для ЛК на основе отчета оборудования.
    """
    try:
        config = load_config('suz_worker_config')
        s3_config = config.get('s3_config')
        equipment_tasks_path = config.get('equipment-tasks')
        equipment_reports_path = config.get('equipment-reports')
        agg_tasks_path = config.get('agg-tasks')

        if not all([equipment_tasks_path, equipment_reports_path, agg_tasks_path]):
            logger.error("[!] В конфигурации отсутствуют пути (equipment-tasks, equipment-reports, agg-tasks)")
            return None

        storage_tasks = get_storage(equipment_tasks_path, s3_config)

        # 1. Загружаем задание оборудования
        task_path = f"{equipment_tasks_path.rstrip('/')}/{task_uuid}.json"
        if not storage_tasks.exists(task_path):
            logger.error(f"[!] Задание оборудования не найдено: {task_path}")
            return None

        task_data = json.loads(storage_tasks.read_text(task_path))
        # Заменяем дефисы на подчеркивания для совместимости с dataclass
        task_data_clean = {k.replace('-', '_'): v for k, v in task_data.items()}
        # Фильтрация полей для dataclass
        sig_task = inspect.signature(EquipmentAggTask.__init__)
        valid_fields_task = {k for k, v in sig_task.parameters.items() if k != 'self'}
        filtered_task_data = {k: v for k, v in task_data_clean.items() if k in valid_fields_task}
        task_obj = EquipmentAggTask(**filtered_task_data)

        # 2. Получаем ID отчета из задания и ищем сам отчет в equipment-reports
        report_uuid = task_obj.id
        storage_reports = get_storage(equipment_reports_path, s3_config)
        report_path = f"{equipment_reports_path.rstrip('/')}/{report_uuid}.json"

        if not storage_reports.exists(report_path):
            logger.info(f"[*] Отчет оборудования {report_uuid} не найден в {equipment_reports_path}. Завершение без ошибки.")
            return None

        report_data = json.loads(storage_reports.read_text(report_path))
        if not report_data.get('readyBox'):
            logger.info(f"[*] Отчет {report_uuid} содержит пустой readyBox. Завершение без ошибки.")
            return None

        # Фильтрация полей для отчета
        sig_report = inspect.signature(EquipmentAggTaskReport.__init__)
        valid_fields_report = {k for k, v in sig_report.parameters.items() if k != 'self'}

        boxes_raw = report_data.get('readyBox', [])
        boxes_objs = []
        sig_box = inspect.signature(EquipmentAggBox.__init__)
        valid_fields_box = {k for k, v in sig_box.parameters.items() if k != 'self'}
        for b_dict in boxes_raw:
            filtered_box_data = {k: v for k, v in b_dict.items() if k in valid_fields_box}
            boxes_objs.append(EquipmentAggBox(**filtered_box_data))

        filtered_report_data = {k: v for k, v in report_data.items() if k in valid_fields_report}
        filtered_report_data['readyBox'] = boxes_objs
        report_obj = EquipmentAggTaskReport(**filtered_report_data)

        # 3. Определяем ИНН по GTIN задания или используем переопределение
        inn = inn_override
        if not inn:
            base_path = os.path.dirname(os.path.abspath(__file__))
            inn = get_inn_by_gtin(task_obj.gtin, db_path=os.path.join(base_path, 'gs1prefix_inn_db.json'))

        if not inn:
            logger.error(f"[!] Не удалось определить ИНН для GTIN {task_obj.gtin} и не задан --inn")
            return None

        logger.info(f"[*] Используется ИНН участника: {inn}")

        # 4. Формируем целевой отчет AggregationReport
        aggregation_units = []
        for box in report_obj.readyBox:
            box_number = str(box.boxNumber)
            # Нормализация SSCC: должен быть 20 цифр, начинаться на 00
            if len(box_number) == 18:
                box_number = "00" + box_number
            elif len(box_number) == 20 and not box_number.startswith("00"):
                logger.warning(f"[*] Странный формат boxNumber (20 знаков, не 00...): {box_number}")

            # Очистка кодов от криптохвоста (до первого \u001d)
            clean_sntins = []
            for code in box.productNumbersFull:
                clean_code = code.split('\u001d')[0]
                clean_sntins.append(clean_code)

            unit = AggregationUnit(
                unitSerialNumber=box_number,
                aggregationType="AGGREGATION",
                sntins=clean_sntins,
                unitSerialNumberList=None
            )
            aggregation_units.append(unit)

        final_report = AggregationReport(
            participantId=inn,
            aggregationUnits=aggregation_units
        )

        # 5. Сохраняем итоговый отчет
        storage_agg = get_storage(agg_tasks_path, s3_config)
        output_path = f"{agg_tasks_path.rstrip('/')}/{task_uuid}.json"

        temp_local = Path(f"temp_agg_{task_uuid}.json")
        with open(temp_local, 'w', encoding='utf-8') as f:
            f.write(final_report.to_json())

        logger.info(f"[*] Выгрузка отчета об агрегации в S3: {output_path}")
        storage_agg.upload(str(temp_local), output_path)

        try: temp_local.unlink()
        except: pass

        return task_uuid

    except Exception as e:
        logger.error(f"[!] Ошибка в create_aggregation_report: {e}")
        return None

def sign_and_send_aggregation(task_uuid: str, group: str, signing_dir: str, timeout: int, refresh_token: bool = False):
    """
    Загружает отчет об агрегации, подписывает его и отправляет в ЛК ЧЗ в обертке.
    """
    try:
        config = load_config('suz_worker_config')
        s3_config = config.get('s3_config')
        agg_tasks_path = config.get('agg-tasks')

        if not agg_tasks_path:
            logger.error("[!] В конфигурации отсутствует путь agg-tasks")
            return None

        # 1. Сначала загружаем отчет и проверяем ИНН
        storage_agg = get_storage(agg_tasks_path, s3_config)
        report_path = f"{agg_tasks_path.rstrip('/')}/{task_uuid}.json"

        if not storage_agg.exists(report_path):
            logger.error(f"[!] Файл отчета об агрегации не найден: {report_path}")
            return None

        logger.info(f"[*] Загрузка отчета {task_uuid} из {report_path}...")
        report_content = storage_agg.read_text(report_path)
        report_data = json.loads(report_content)
        inn = report_data.get('participantId')

        if not inn:
            logger.error(f"[!] Не найден participantId в отчете {task_uuid}")
            return None

        # 2. Проверяем токен ДО цикла подписи, чтобы не ждать зря
        base_path = os.path.dirname(os.path.abspath(__file__))
        org_manager = OrganizationManager(os.path.join(base_path, 'my_orgs'))
        token_processor = TokenProcessor(org_manager=org_manager)

        token = None
        if not refresh_token:
            token = token_processor.get_token_value_by_inn(inn, token_type='JWT')

        if not token:
            logger.info(f"[*] Получение нового JWT токена для ИНН {inn}...")
            token = get_new_token(inn=inn, mode='jwt', timeout=timeout)
            if token:
                token_processor.save_token(token)
                logger.info(f"[+] Новый токен успешно получен и сохранен.")
            else:
                logger.error(f"[!] Не удалось получить JWT токен для ИНН {inn}.")
                return None

        # Декодируем JWT для логов, чтобы проверить PID/INN
        try:
            payload = token_processor._decode_jwt_payload(token)
            logger.info(f"[*] Токен участника: INN={payload.get('inn')}, PID={payload.get('pid')}, Name={payload.get('full_name')}")
        except: pass

        # 3. Подготовка к подписи
        work_dir = Path(signing_dir)
        work_dir.mkdir(parents=True, exist_ok=True)
        unique_id = uuid.uuid4()
        body_filename = f"{inn}_{unique_id}_agg.json"
        body_path = work_dir / body_filename
        signature_path = work_dir / f"{body_filename}.sig"

        try:
            with open(body_path, "wb") as f:
                # ЧЗ крайне чувствителен к изменению тела документа после подписи.
                # Поэтому мы используем исходные байты, загруженные из S3.
                if isinstance(report_content, str):
                    f.write(report_content.encode('utf-8'))
                else:
                    f.write(report_content)

            logger.info(f"[*] Ожидание подписи для {body_path}...")
            start_time = time.time()
            while not signature_path.exists():
                if time.time() - start_time > timeout:
                    logger.error("[!] Таймаут ожидания подписи.")
                    return None
                time.sleep(1)

            # Читаем тело и подпись как байты, кодируем в Base64
            # Мы повторно читаем файл body_path, чтобы гарантировать
            # побайтовую идентичность с тем, что видел демон подписи.
            with open(body_path, "rb") as f:
                doc_bytes = f.read()
                doc_base64 = base64.b64encode(doc_bytes).decode('utf-8')

            with open(signature_path, "r", encoding="utf-8") as f:
                # В данном проекте демон подписи сохраняет подпись в формате Base64.
                # Повторное кодирование приведет к ошибке проверки подписи в ЛК.
                sig_base64 = f.read().strip()

            # Создаем обертку
            wrapper = DocumentWrapper(
                document_format="MANUAL",
                product_document=doc_base64,
                type="AGGREGATION_DOCUMENT",
                signature=sig_base64
            )

            # Отправка через TrueAPI
            api = HonestSignAPI(token=token)
            wrapped_json = wrapper.to_json()
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f"[*] Текст запроса агрегации: {wrapped_json}")

            result = api.documents_create(wrapped_json, pg=group)

            if result and "error" not in str(result).lower():
                logger.info(f"[+++] Отчет об агрегации успешно отправлен в ЛК! Результат: {result}")

                # Сохраняем чек отправки
                agg_receipts_path = config.get('agg-receipts')
                if agg_receipts_path:
                    storage_receipts = get_storage(agg_receipts_path, s3_config)
                    remote_receipt_path = f"{agg_receipts_path.rstrip('/')}/{task_uuid}.json"

                    temp_receipt = work_dir / f"receipt_agg_{unique_id}.json"
                    with open(temp_receipt, 'w', encoding='utf-8') as f:
                        json.dump(result, f, indent=4, ensure_ascii=False)

                    logger.info(f"[*] Выгрузка чека агрегации в S3: {remote_receipt_path}")
                    storage_receipts.upload(str(temp_receipt), remote_receipt_path)
                    try:
                        temp_receipt.unlink()
                    except:
                        pass

                return result
            else:
                logger.error(f"[!] Ошибка отправки отчета в ЛК: {result}")
                return result

        finally:
            if body_path.exists(): body_path.unlink()
            if signature_path.exists(): signature_path.unlink()

    except Exception as e:
        logger.error(f"[!] Ошибка в sign_and_send_aggregation: {e}")
        return None

def update_utilisation_report_status(order_id: str):
    """
    Получает актуальный статус отчета о нанесении из СУЗ и сохраняет в S3/Локально.
    """
    try:
        config = load_config('suz_worker_config')
        s3_config = config.get('s3_config')
        utilisation_receipts_path = config.get('utilisation_receipts')
        utilisation_reports_path = config.get('utilisation_reports')

        if not all([utilisation_receipts_path, utilisation_reports_path]):
            logger.error("[!] В конфигурации отсутствуют пути (utilisation_receipts, utilisation_reports)")
            return None

        # 1. Загружаем чек отчета (там reportId и omsId)
        storage_receipts = get_storage(utilisation_receipts_path, s3_config)
        receipt_path = f"{utilisation_receipts_path.rstrip('/')}/{order_id}.json"

        if not storage_receipts.exists(receipt_path):
            logger.error(f"[!] Чек отчета о нанесении не найден: {receipt_path}")
            return None

        receipt_data = json.loads(storage_receipts.read_text(receipt_path))
        report_id = receipt_data.get('reportId')
        oms_id = receipt_data.get('omsId')

        if not report_id or not oms_id:
            logger.error(f"[!] Недостаточно данных в чеке для {order_id}: reportId={report_id}, omsId={oms_id}")
            return None

        # 2. Инициализация API
        base_path = os.path.dirname(os.path.abspath(__file__))
        org_manager = OrganizationManager(os.path.join(base_path, 'my_orgs'))

        found_org = None
        for o in org_manager.list():
            if o.oms_id == oms_id:
                found_org = o
                break

        if not found_org:
            logger.error(f"[!] Организация с omsId {oms_id} не найдена.")
            return None

        token_processor = TokenProcessor(org_manager=org_manager)
        token = token_processor.get_token_value_by_inn(found_org.inn, token_type='UUID', conid=found_org.connection_id)

        if not token:
            logger.error(f"[!] Токен не найден.")
            return None

        suz_api = SUZ(token=token, omsId=oms_id, clientToken=found_org.connection_id)

        # 3. Запрос статуса
        logger.info(f"[*] Запрос статуса отчета {report_id} для заказа {order_id}...")
        try:
            status_res = suz_api.report_info(report_id)
        except Exception as e:
            logger.error(f"[!] Ошибка API: {e}")
            return str(e)

        if not status_res:
            logger.error("[!] Пустой ответ от СУЗ.")
            return None

        # Фильтрация полей для dataclass
        sig = inspect.signature(UtilisationReportStatus.__init__)
        valid_fields = {k for k, v in sig.parameters.items() if k != 'self'}
        filtered_data = {k: v for k, v in status_res.items() if k in valid_fields}

        status_obj = UtilisationReportStatus(**filtered_data)

        # 4. Сохранение
        storage_reports = get_storage(utilisation_reports_path, s3_config)
        output_path = f"{utilisation_reports_path.rstrip('/')}/{order_id}.json"

        temp_local = Path(f"temp_util_status_{order_id}.json")
        with open(temp_local, 'w', encoding='utf-8') as f:
            f.write(status_obj.to_json())

        logger.info(f"[*] Сохранение статуса отчета в {output_path}")
        storage_reports.upload(str(temp_local), output_path)

        # Теги для расширения (если локально) или метаданных S3
        storage_reports.set_tags(output_path, {"reportStatus": status_obj.reportStatus})

        try: temp_local.unlink()
        except: pass

        return status_obj

    except Exception as e:
        logger.error(f"[!] Ошибка в update_utilisation_report_status: {e}")
        return str(e)


def update_aggregation_status(task_uuid: str, group: str):
    """
    Получает актуальный статус документа агрегации из ЛК ЧЗ и сохраняет чек.
    """
    try:
        config = load_config('suz_worker_config')
        s3_config = config.get('s3_config')
        agg_receipts_path = config.get('agg-receipts')
        agg_tasks_path = config.get('agg-tasks')

        if not all([agg_receipts_path, agg_tasks_path]):
            logger.error("[!] В конфигурации отсутствуют пути (agg-receipts, agg-tasks)")
            return None

        # 1. Загружаем чек отправки (там docId и ИНН)
        storage_receipts = get_storage(agg_receipts_path, s3_config)
        receipt_path = f"{agg_receipts_path.rstrip('/')}/{task_uuid}.json"

        if not storage_receipts.exists(receipt_path):
            logger.error(f"[!] Чек отправки агрегации не найден: {receipt_path}")
            return None

        receipt_data = json.loads(storage_receipts.read_text(receipt_path))
        doc_id = receipt_data.get('document_id')

        # Если в чеке нет doc_id, возможно это старая версия или ошибка
        if not doc_id:
            logger.error(f"[!] В чеке {task_uuid} отсутствует document_id")
            return None

        # 2. Получаем ИНН из исходного отчета для авторизации
        storage_agg = get_storage(agg_tasks_path, s3_config)
        report_path = f"{agg_tasks_path.rstrip('/')}/{task_uuid}.json"
        if not storage_agg.exists(report_path):
            logger.error(f"[!] Файл отчета об агрегации не найден: {report_path}")
            return None

        report_data = json.loads(storage_agg.read_text(report_path))
        inn = report_data.get('participantId')

        if not inn:
            logger.error(f"[!] Не найден participantId в отчете {task_uuid}")
            return None

        # 3. Инициализация API
        base_path = os.path.dirname(os.path.abspath(__file__))
        org_manager = OrganizationManager(os.path.join(base_path, 'my_orgs'))
        token_processor = TokenProcessor(org_manager=org_manager)
        token = token_processor.get_token_value_by_inn(inn, token_type='JWT')

        if not token:
            logger.error(f"[!] JWT токен для ИНН {inn} не найден.")
            return None

        api = HonestSignAPI(token=token)

        # 4. Запрос статуса
        logger.info(f"[*] Запрос статуса документа {doc_id} для задачи {task_uuid}...")
        status_res = api.doc(doc_id, pg=group)

        if not status_res or "error" in status_res:
            logger.error(f"[!] Ошибка API при запросе статуса: {status_res}")
            return status_res

        # 5. Сохранение обновленного чека (перезаписываем или дополняем)
        # В данном случае просто сохраняем полный ответ API как актуальный статус
        temp_local = Path(f"temp_agg_status_{task_uuid}.json")
        with open(temp_local, 'w', encoding='utf-8') as f:
            json.dump(status_res, f, indent=4, ensure_ascii=False)

        logger.info(f"[*] Обновление чека агрегации в {receipt_path}")
        storage_receipts.upload(str(temp_local), receipt_path)

        # Установка тегов статуса
        doc_status = status_res.get('status')
        if doc_status:
            storage_receipts.set_tags(receipt_path, {"status": doc_status})

        try:
            temp_local.unlink()
        except:
            pass

        return status_res

    except Exception as e:
        logger.error(f"[!] Ошибка в update_aggregation_status: {e}")
        return str(e)


def main():
    parser = argparse.ArgumentParser(description="Создание, подпись и отправка заказа на эмиссию КМ в СУЗ")
    parser.add_argument("--create-task", help="Создать задачу на эмиссию по productionOrderId")
    parser.add_argument("--send-task", help="Подписать и отправить задачу на эмиссию по productionOrderId")
    parser.add_argument("--create-utilisation", help="Создать задачу на отчет о нанесении по orderId (UUID)")
    parser.add_argument("--send-utilisation", help="Подписать и отправить отчет о нанесении по orderId (UUID)")
    parser.add_argument("--utilisation-status", help="Получить статус отчета о нанесении по orderId (UUID)")
    parser.add_argument("--create-aggregation", help="Создать отчет об агрегации для ЛК по UUID задания оборудования")
    parser.add_argument("--send-aggregation", help="Отправить отчет об агрегации в ЛК по UUID задания оборудования")
    parser.add_argument("--aggregation-status", help="Получить статус отчета об агрегации по UUID задания оборудования")

    parser.add_argument("--group", default="chemistry", help="Товарная группа (например: chemistry, perfumes, clothes...)")
    parser.add_argument("--contact", default="хТрек 2.5.11.6", help="Контактное лицо в заказе")
    parser.add_argument("--oms_id", help="OMS ID (если не задан, будет найден в my_orgs по ИНН)")
    parser.add_argument("--inn", help="ИНН участника (переопределяет автоматическое определение)")
    parser.add_argument("--refresh-token", action="store_true", help="Принудительно получить новый токен")
    parser.add_argument("--suz_worker_config", help="Путь к конфигурационному файлу")
    parser.add_argument("--debug", action="store_true", help="Выводить отладочную информацию (текст запроса)")
    parser.add_argument("--client_token", help="Client Token / Connection ID")
    parser.add_argument("--signing_dir", default=SIGNING_DIR, help="Директория для обмена с демоном подписи")
    parser.add_argument("--timeout", type=int, default=SIGNING_TIMEOUT, help="Тайм-аут ожидания подписи (сек)")
    parser.add_argument("--status", help="Получить статус заказа по productionOrderId")
    parser.add_argument("--production-date", help="Дата производства для отчета (yyyy-MM-dd)")
    parser.add_argument("--expiration-date", help="Дата истечения срока годности для отчета (yyyy-MM-dd)")
    parser.add_argument("--get-codes", help="Получить коды для заказа по orderId (UUID)")

    args = parser.parse_args()

    if args.suz_worker_config:
        os.environ['suz_worker_config'] = args.suz_worker_config
        logger.info(f"[*] Переопределен путь к конфигу: {args.suz_worker_config}")

    if args.debug:
        logger.setLevel(logging.DEBUG)
        logging.getLogger('trueapi').setLevel(logging.DEBUG)
        logging.getLogger('suz').setLevel(logging.DEBUG)

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

    if args.utilisation_status:
        result = update_utilisation_report_status(args.utilisation_status)
        if isinstance(result, UtilisationReportStatus):
            print(json.dumps(result.to_dict(), indent=2, ensure_ascii=False))
        else:
            print(f"Результат: {result}")
        return

    if args.create_aggregation:
        result = create_aggregation_report(args.create_aggregation, inn_override=args.inn)
        if result:
            logger.info(f"[+++] Отчет об агрегации успешно создан для {result}")
        else:
            logger.error(f"[!] Не удалось создать отчет об агрегации для {args.create_aggregation}")
        return

    if args.send_aggregation:
        result = sign_and_send_aggregation(args.send_aggregation, args.group, args.signing_dir, args.timeout,
                                          refresh_token=args.refresh_token)
        if result and "error" not in str(result).lower():
            logger.info(f"[+++] Отчет об агрегации успешно отправлен!")
            print(json.dumps(result, indent=2, ensure_ascii=False))
        else:
            logger.error(f"[!] Не удалось отправить отчет об агрегации.")
        return

    if args.aggregation_status:
        result = update_aggregation_status(args.aggregation_status, args.group)
        if result and "error" not in str(result).lower():
            print(json.dumps(result, indent=2, ensure_ascii=False))
        else:
            print(f"Результат: {result}")
        return

    parser.print_help()

if __name__ == "__main__":
    main()
