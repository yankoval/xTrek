import os
import sys
import json
import time
import argparse
import uuid
import logging
from pathlib import Path

# Добавляем текущую директорию в путь поиска модулей
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from suz_api_models import EmissionOrder, OrderAttributes, OrderProduct, EmissionOrderreceipts
from suz import SUZ
from gs1_processor import get_inn_by_gtin
from tokens import TokenProcessor
from org_manager import OrganizationManager
from storage import get_storage
from config_loader import load_config

# --- НАСТРОЙКИ ПО УМОЛЧАНИЮ ---
SIGNING_DIR = os.path.join(os.path.expanduser("~"), "tst")
SIGNING_TIMEOUT = 60
# ------------------------------

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def sign_and_send_emission(order: EmissionOrder, inn: str, signing_dir: str, timeout: int,
                  oms_id: str = None, client_token: str = None):
    """
    Подписывает заказ через файловый обмен и отправляет в СУЗ
    """
    # 1. Сначала проверяем учетные данные, чтобы не ждать подписи зря
    base_path = os.path.dirname(os.path.abspath(__file__))
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
            # Если с oms_id не нашли, берем любую по ИНН
            found_org = org_manager.find(inn=inn)

        if found_org:
            logger.info(f"[*] Используется профиль организации: {found_org.name}")
            final_oms_id = final_oms_id or found_org.oms_id
            final_client_token = final_client_token or found_org.connection_id

    if not final_oms_id:
        logger.error(f"[!] OMS ID для ИНН {inn} не найден в базе и не передан параметром.")
        return None

    if not final_client_token:
        logger.error(f"[!] Connection ID (clientToken) для ИНН {inn} не найден в базе.")
        return None

    token_processor = TokenProcessor(org_manager=org_manager)
    # Для СУЗ обычно требуется UUID токен (auth)
    token = token_processor.get_token_value_by_inn(inn, token_type='UUID', conid=final_client_token)

    if not token:
        logger.error(f"[!] Активный токен для ИНН {inn} и Connection ID {final_client_token} не найден.")
        logger.error("Получите токен с помощью crpt_auth.py перед запуском.")
        return None

    # 2. Подготовка к подписи
    work_dir = Path(signing_dir)
    work_dir.mkdir(parents=True, exist_ok=True)

    unique_id = uuid.uuid4()
    # Имя файла должно содержать ИНН для работы демона подписи
    body_filename = f"{inn}_{unique_id}_order.json"
    body_path = work_dir / body_filename
    signature_path = work_dir / f"{body_filename}.sig"

    try:
        # Сохраняем тело заказа для подписи
        order_json = order.to_json()
        with open(body_path, "w", encoding="utf-8") as f:
            f.write(order_json)

        logger.info(f"[*] Файл заказа сохранен в: {body_path}. Ожидание подписи...")

        # Ожидание появления файла подписи
        start_time = time.time()
        while not signature_path.exists():
            if time.time() - start_time > timeout:
                logger.error(f"[!] Таймаут ({timeout}с): Файл подписи {signature_path.name} не найден.")
                return None
            time.sleep(1)

        time.sleep(0.5) # Небольшая пауза для завершения записи файла
        logger.info("[+] Подпись обнаружена!")

        # 3. Инициализация SUZ и отправка
        suz_api = SUZ(token=token, omsId=final_oms_id, clientToken=final_client_token)

        logger.info(f"[*] Отправка заказа в СУЗ (omsId: {final_oms_id})...")
        result = suz_api.order_create(str(body_path), str(signature_path))

        # 4. Сохранение в S3
        if isinstance(result, EmissionOrderreceipts):
            try:
                config = load_config('suz_worker_config')
                s3_config = config.get('s3_config')
                emission_orders_path = config.get('emission_orders_path')

                if emission_orders_path:
                    storage = get_storage(emission_orders_path, s3_config)
                    production_order_id = order.attributes.productionOrderId
                    remote_filename = f"{production_order_id}.json"
                    # Убеждаемся, что путь заканчивается на /
                    base_remote_path = emission_orders_path.rstrip('/') + '/'
                    remote_path = base_remote_path + remote_filename

                    if storage.exists(remote_path):
                        existing_content = storage.read_text(remote_path)
                        logger.info(f"[!] Файл {remote_path} уже существует в S3. Содержимое:")
                        logger.info(existing_content)
                    else:
                        # Временный локальный файл для загрузки
                        temp_local = work_dir / f"response_{unique_id}.json"
                        with open(temp_local, 'w', encoding='utf-8') as f:
                            json.dump(result.to_dict(), f, ensure_ascii=False, indent=4)

                        logger.info(f"[*] Выгрузка ответа в S3: {remote_path}")
                        storage.upload(str(temp_local), remote_path)

                        try: temp_local.unlink()
                        except: pass
            except Exception as s3_err:
                logger.error(f"Ошибка при сохранении ответа в S3: {s3_err}")

        return result

    finally:
        # Удаляем временные файлы
        if body_path.exists():
            try: body_path.unlink()
            except: pass
        if signature_path.exists():
            try: signature_path.unlink()
            except: pass

def main():
    parser = argparse.ArgumentParser(description="Создание, подпись и отправка заказа на эмиссию КМ в СУЗ")
    parser.add_argument("--gtin", default="04630234044646", help="GTIN товара (для определения ИНН)")
    parser.add_argument("--quantity", type=int, default=15, help="Количество запрашиваемых кодов")
    parser.add_argument("--group", default="chemistry", help="Товарная группа (например: chemistry, perfumes, clothes...)")
    parser.add_argument("--contact", default="хТрек 2.5.11.6", help="Контактное лицо в заказе")
    parser.add_argument("--oms_id", help="OMS ID (если не задан, будет найден в my_orgs по ИНН)")
    parser.add_argument("--client_token", help="Client Token / Connection ID")
    parser.add_argument("--signing_dir", default=SIGNING_DIR, help="Директория для обмена с демоном подписи")
    parser.add_argument("--timeout", type=int, default=SIGNING_TIMEOUT, help="Тайм-аут ожидания подписи (сек)")

    args = parser.parse_args()

    # 1. Определяем ИНН по GTIN
    # Используем базу gs1prefix_inn_db.json
    base_path = os.path.dirname(os.path.abspath(__file__))
    db_path = os.path.join(base_path, 'gs1prefix_inn_db.json')
    inn = get_inn_by_gtin(args.gtin, db_path=db_path)

    if not inn:
        logger.error(f"Не удалось определить ИНН для GTIN {args.gtin}. Проверьте {db_path}")
        sys.exit(1)

    logger.info(f"Определен ИНН: {inn} для GTIN: {args.gtin}")

    # 2. Формируем структуру заказа
    attr = OrderAttributes(
        productionOrderId=str(uuid.uuid4()),
        createMethodType="SELF_MADE",
        releaseMethodType="PRODUCTION",
        paymentType=2,
        contactPerson=args.contact
    )

    product = OrderProduct(
        gtin=args.gtin,
        quantity=args.quantity,
        serialNumberType="OPERATOR",
        templateId=47,
        cisType="UNIT"
    )

    order = EmissionOrder(
        productGroup=args.group,
        attributes=attr,
        products=[product]
    )

    # 3. Подписываем и отправляем
    try:
        result = sign_and_send_emission(order, inn, args.signing_dir, args.timeout,
                             oms_id=args.oms_id, client_token=args.client_token)

        if isinstance(result, EmissionOrderreceipts):
            logger.info("[+++] Заказ успешно создан!")
            print(json.dumps(result.to_dict(), indent=2, ensure_ascii=False))
        elif result:
            logger.error(f"Не удалось создать заказ. Ответ СУЗ: {result}")
        else:
            logger.error("Не удалось создать заказ (см. логи выше).")
    except Exception as e:
        logger.error(f"Ошибка при выполнении: {e}")
        # import traceback
        # traceback.print_exc()

if __name__ == "__main__":
    main()
