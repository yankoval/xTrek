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

from suz_api_models import EmissionOrder, OrderAttributes, OrderProduct
from suz import SUZ
from gs1_processor import get_inn_by_gtin
from tokens import TokenProcessor
from org_manager import OrganizationManager

# --- НАСТРОЙКИ ПО УМОЛЧАНИЮ ---
SIGNING_DIR = os.path.join(os.path.expanduser("~"), "tst")
SIGNING_TIMEOUT = 60
# ------------------------------

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def sign_and_send(order: EmissionOrder, inn: str, signing_dir: str, timeout: int):
    """
    Подписывает заказ через файловый обмен и отправляет в СУЗ
    """
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

        # Инициализация SUZ и отправка
        # Используем TokenProcessor и OrganizationManager для получения учетных данных
        base_path = os.path.dirname(os.path.abspath(__file__))
        org_manager = OrganizationManager(os.path.join(base_path, 'my_orgs'))
        org = org_manager.find(inn=inn)
        if not org:
            logger.error(f"[!] Организация с ИНН {inn} не найдена в базе (папка my_orgs).")
            return None

        if not org.oms_id:
            logger.error(f"[!] OMS ID для организации с ИНН {inn} не задан в профиле.")
            return None

        if not org.connection_id:
            logger.error(f"[!] Connection ID (clientToken) для организации с ИНН {inn} не задан.")
            return None

        token_processor = TokenProcessor(org_manager=org_manager)
        # Для СУЗ обычно требуется UUID токен (auth)
        token = token_processor.get_token_value_by_inn(inn, token_type='UUID', conid=org.connection_id)

        if not token:
            logger.error(f"[!] Активный токен для ИНН {inn} и Connection ID {org.connection_id} не найден.")
            logger.error("Получите токен с помощью crpt_auth.py перед запуском.")
            return None

        suz_api = SUZ(token=token, omsId=org.oms_id, clientToken=org.connection_id)

        logger.info(f"[*] Отправка заказа в СУЗ (omsId: {org.oms_id})...")
        result = suz_api.order_create(str(body_path), str(signature_path))
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
    parser.add_argument("--gtin", default="4630234044646", help="GTIN товара (для определения ИНН)")
    parser.add_argument("--quantity", type=int, default=515, help="Количество запрашиваемых кодов")
    parser.add_argument("--group", default="chemistry", help="Товарная группа (например: chemistry, perfumes, clothes...)")
    parser.add_argument("--contact", default="хТрек 2.5.11.6", help="Контактное лицо в заказе")
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
        result = sign_and_send(order, inn, args.signing_dir, args.timeout)

        if result:
            logger.info("[+++] Заказ успешно создан!")
            print(json.dumps(result, indent=2, ensure_ascii=False))
        else:
            logger.error("Не удалось создать заказ (см. логи выше).")
    except Exception as e:
        logger.error(f"Ошибка при выполнении: {e}")
        # import traceback
        # traceback.print_exc()

if __name__ == "__main__":
    main()
