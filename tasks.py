import os
from celery import Celery
from urllib.parse import quote

# 1. Импорт вашей бизнес-логики
from xtrek.config_loader import load_config
from xtrek.create_emission_task_sample import (
    process_incoming_task, 
    create_equipment_aggregation_task, 
    create_emission_task, 
    sign_and_send_emission,
    update_emission_order_status,
    get_emission_kodes,
    create_virtual_utilisation_task,
    sign_and_send_utilisation,
    update_utilisation_report_status,
    create_virtual_introduce_task,
    sign_and_send_introduce,
    update_introduce_status,
    create_virtual_tasks_from_equipment_report,
    create_utilisation_task_from_report,
    update_aggregation_status,
)
from xtrek.prn_util import generate_prn_files
from xtrek.utils import (
        check_aggregation_reports,
        )
from xtrek.suz_api_models import (
    EmissionOrder, OrderAttributes, OrderProduct, EmissionOrderreceipts,
    EmissionOrderStatus, ProductionOrder, PasportData,
    UtilisationReport, UtilisationReportReceipt, UtilisationReportStatus,
    AggregationReport, AggregationUnit, EquipmentAggTask, EquipmentAggTaskReport,
    EquipmentAggBox, DocumentWrapper, IntroduceMessage, IntroduceProduct, GtinDocument
)
# 2. Настройки доступа
ACCESS_KEY = os.environ.get('YMQ_ACCESS_KEY')
SECRET_KEY = os.environ.get('YMQ_SECRET_KEY')
QUEUE_URL = os.environ.get('YMQ_QUEUE_URL')
REAL_QUEUE_NAME = 'queue_task_create_1C'

safe_secret = quote(SECRET_KEY, safe='')
BROKER_URL = f'sqs://{ACCESS_KEY}:{safe_secret}@'

app = Celery('tasks', broker=BROKER_URL)

# Загрузка конфигурации
config = load_config('suz_worker_config')

# Настройки бакетов
INPUT_BUCKET = config.get('input_bucket', "1bf11148-3595-4a07-a089-d460153b7c7a")
INTERNAL_BUCKET = config.get('internal_bucket', "20ab2a0c-2726-4ba1-9c7c-7deae82941ff")

# Глобальные настройки для процессов
PRODUCT_GROUP = config.get('product_group', "chemistry")
CONTACT_PERSON = config.get('contact_person', "scan")

# Директория для подписи
signing_dir = config.get('sign', r"Y:\BatchPassToPrint\tst")

app.conf.update(
    broker_transport_options={
        'region': 'ru-central1',
        'predefined_queues': {
            REAL_QUEUE_NAME: {'url': QUEUE_URL}
        }
    },
    task_default_queue=REAL_QUEUE_NAME,
    accept_content=['json'],
    task_serializer='json',
    result_serializer='json',
    broker_connection_retry_on_startup=True,
    worker_enable_remote_control=False,
    task_acks_late=True, # Подтверждаем удаление только после успеха
)

# --- ЛОГИКА ДЛЯ БАКЕТА: 1bf11148... / ПАПКА: Задания ---
def logic_create_order(full_key):
    group, contact = PRODUCT_GROUP, CONTACT_PERSON
    print(f"[LOGIC-0] Запуск создания заказа для: {full_key}")
    
    production_order_id = process_incoming_task(s3_full_key=full_key)
    if not production_order_id:
        return "No production_order_id created"

    create_equipment_aggregation_task(production_order_id)
    resultCEmT = create_emission_task(production_order_id, group, contact)
    
    if not resultCEmT:
        raise RuntimeError(f"create_emission_task failed for {production_order_id}")
    
    return f"Order {production_order_id} created and emission task started"

# --- ЛОГИКА ДЛЯ БАКЕТА: 20ab2a0c... / ПАПКА: emissionOrders ---
def logic_sign_emission(full_key):
    # Здесь предполагается, что full_key содержит ID заказа или путь к нему
    # ВАЖНО: Если sign_and_send_emission нужен ID, извлеките его из ключа
    production_order_id = full_key.split('/')[-1].replace('.json', '') 
    ###signing_dir = r"/Users/ivankiselev/tst"
    
    print(f"[LOGIC-2] Запуск подписания эмиссии для ID: {production_order_id}")
    
    resultSEmT = sign_and_send_emission(production_order_id, signing_dir, 120)
    
    if not resultSEmT:
        raise RuntimeError(f"sign_and_send_emission failed for {production_order_id}")
    
    return f"Emission for {production_order_id} signed and sent"

# --- ЛОГИКА ДЛЯ БАКЕТА: 20ab2a0c... / ПАПКА: emissionRecepts ---
def logic_update_emission(full_key):
    # Здесь предполагается, что full_key содержит ID заказа или путь к нему
    # ВАЖНО: Если update_emission_order_status нужен ID, извлеките его из ключа
    production_order_id = full_key.split('/')[-1].replace('.json', '') 
    ##signing_dir = r"/Users/ivankiselev/tst"
    
    print(f"[LOGIC-3] Запуск обновления статуса эмиссии для ID: {production_order_id}")
    
    result = update_emission_order_status(production_order_id)
    if isinstance(result, EmissionOrderStatus):
        print(f"[LOGIC-3] Статус эмиссии для {production_order_id}: {result.bufferStatus}")
        if result.bufferStatus == "ACTIVE":
            return f"Emission for {production_order_id} ready for download (ACTIVE)"
        elif result.bufferStatus == "EXHAUSTED":
            return f"Emission for {production_order_id} has been downloaded (EXHAUSTED)"
        else:
            raise RuntimeError(f"Unexpected bufferStatus '{result.bufferStatus}' for {production_order_id}")
    else:
        raise RuntimeError(f"update_emission_order_status failed for {production_order_id}, got {type(result)} : {result}")

# --- ЛОГИКА ДЛЯ БАКЕТА: 20ab2a0c... / ПАПКА: emissions/ ---
def logic_get_emission_kodes(full_key):
    # Здесь предполагается, что full_key содержит ID эмиссии или путь к нему
    # ВАЖНО: Если get_emission_kodes нужен ID, извлеките его из ключа
    emission_order_id = full_key.split('/')[-1].replace('.json', '') 
    ##signing_dir = r"/Users/ivankiselev/tst"
    
    print(f"[LOGIC-4] Запуск получение кодов эмиссии для ID: {emission_order_id}")
    
    result = get_emission_kodes(emission_order_id)
    if not result:
        raise RuntimeError(f"get_emission_kodes failed for {emission_order_id}")
    if 'codes' in result.keys():
        total_codes = len(result['codes'])
    else: 
        total_codes = result.get('totalCodes', 'unknown count')
    return f"Emission codes for {emission_order_id} retrieved: {total_codes} codes"

# --- ЛОГИКА ДЛЯ БАКЕТА: 20ab2a0c... / ПАПКА: kodes/ ---
def logic_kodes(full_key):
    # Здесь предполагается, что full_key содержит ID эмиссии или путь к нему
    kodes_order_id = full_key.split('/')[-1].replace('.json', '') 
    
    print(f"[LOGIC-5] печать а так же проверка и запуск создания отчёта о нанесении: {kodes_order_id}")
    
    # Создание задания на печать
    try:
        result = generate_prn_files(kodes_order_id)
        if not result:
            raise RuntimeError(f"generate_prn_files failed for {kodes_order_id}")
    except Exception as e:
        print(e)
    # Создаем отчет о нанесении
    result = create_virtual_utilisation_task(kodes_order_id, PRODUCT_GROUP)
    if not result:
        raise RuntimeError(f"create_virtual_utilisation_task failed for {kodes_order_id}")
    if type(result) is ProductionOrder:
        if result.virtual:
                result = sign_and_send_utilisation(kodes_order_id, signing_dir, 120)
                if not result:
                    raise RuntimeError(f"sign_and_send_utilisation failed for {kodes_order_id}")

                return f"Virtual utilisation task for {kodes_order_id} created successfully"
        else:
            #TODO: Create print task if not virtual
            return f"For virtual production order {kodes_order_id}, no need to create utilisation report"

# --- ЛОГИКА ДЛЯ БАКЕТА: 20ab2a0c... / ПАПКА: utilisationReceipts/ ---
def logic_utilisationReceipt(full_key):
    # Здесь предполагается, что full_key содержит ID эмиссии или путь к нему
    utilisationReceipt_id = full_key.split('/')[-1].replace('.json', '') 
    
    print(f"[LOGIC-6] проверка статуса отчёта о нанесении: {utilisationReceipt_id}")
    
    result = update_utilisation_report_status(utilisationReceipt_id)
    if not result:
        raise RuntimeError(f"logic_utilisationReceipt failed for {utilisationReceipt_id}")
    print(f"Virtual utilisation task for {utilisationReceipt_id} status is {result}")
    if type(result) is not UtilisationReportStatus:
        raise RuntimeError(f"logic_utilisationReceipt failed for {utilisationReceipt_id} result:{result}")
    if result.reportStatus !="SUCCESS":
        raise RuntimeError(f"logic_utilisationReceipt failed for {utilisationReceipt_id} result staus:{result.reportStatus}")
    # Если документ относиться к виртуальному заданию на производство то запускаем создание сообщения о вводе в оборот
    if utilisationReceipt_id[0:2] == 'T-':
        return f"пропускаем создание сообщения о вводе в оборот для не виртуального{utilisationReceipt_id}"
    result = create_virtual_introduce_task(utilisationReceipt_id, PRODUCT_GROUP)
    if not result:
        raise RuntimeError(f"create_virtual_introduce_task failed for {utilisationReceipt_id}")
    if type(result) is ProductionOrder:
        if result.virtual:
            result = sign_and_send_introduce(utilisationReceipt_id, PRODUCT_GROUP, signing_dir, 120)
            if not result:
                raise RuntimeError(f"sign_and_send_introduce failed for {utilisationReceipt_id}")

            return f"Introduce task for virtual {utilisationReceipt_id} has been send successfully:{result}"
        else:
            #TODO: Create print task if not virtual
            return f"For real production order {utilisationReceipt_id}, skip send introduce task in virtual task flow."

    return f"Virtual utilisation task for {utilisationReceipt_id} status is {result}"

# --- ЛОГИКА ДЛЯ БАКЕТА: 20ab2a0c... / ПАПКА: emissionRecepts ---
def logic_update_introduce(full_key):
    # Здесь предполагается, что full_key содержит ID заказа или путь к нему
    # ВАЖНО: Если update_introduce_status нужен ID, извлеките его из ключа
    introduceReceipt_id = full_key.split('/')[-1].replace('.json', '') 
    ##signing_dir = r"/Users/ivankiselev/tst"
    
    print(f"[LOGIC-7] Запуск обновления статуса сообщения о вводе в оборот для ID: {introduceReceipt_id}")
    
    result = update_introduce_status(introduceReceipt_id, PRODUCT_GROUP)
    if not result:
        raise RuntimeError(f"update_introduce_status failed for {introduceReceipt_id} result:{result}")
    if isinstance(result, dict) and "error" in result:
        print("Ошибка API при запросе статуса")
        raise RuntimeError(f"update_introduce_status failed for {introduceReceipt_id}  result:{result}")
    result = result[0] if isinstance(result, list) and len(result) > 0 else result
    if isinstance(result, dict) and result.get('status')== 'CHECKED_OK':
        return f"introduce status for {introduceReceipt_id} is {result.get('status')}"
    else:
        raise RuntimeError(f"update_introduce_status failed for {introduceReceipt_id} result:{result}")

# --- ЛОГИКА ДЛЯ БАКЕТА: 20ab2a0c... / ПАПКА: equipment-reports/ ---
def logic_start_equipment_reports(full_key):
    group = PRODUCT_GROUP
    print(f"[LOGIC-20] Запуск обработки equipment-reports: {full_key}")
    report_id = full_key.split('/')[-1].replace('.json', '') 
    if not report_id:
        return f"No production_order_id found for {full_key}"
    # Номер заказа на производство равен номеру отчета оборудования
    production_order_id = report_id
    # Проверяем отчет перед обработкой
    result = check_aggregation_reports([production_order_id])
    
    # Если есть ошибки не обрабатывам
    if result:
        error = list(result.values())[0] 
        if error:
            return f"Отчет оборудования об агрегации {production_order_id} пропускаем. Нацдены ошибки: {error}."
    else:
        return f"Отчет оборудования об агрегации {production_order_id} пропускаем. Нацдены ошибки: {error}."
    
    # Запускаем процедуру создания/подписания/отправки отчета об утилизации по емиссии связаной с заказом на производство
    print(f"Запускаем процедуру создания/подписания/отправки отчета об утилизации по емиссии связаной с заказом на производство")
    utResult = create_utilisation_task_from_report(report_id, group)
    if not utResult:
        print(f"Ошибка при попытке создания отчёта о нанесении по отчету оборудования f{report_id}. Продолжаем обработку. {utResult}")
    else:
        print("Подписываем/отправляем")
        sutResult = sign_and_send_utilisation(report_id, signing_dir, 120)
        if not sutResult:
            print(f"sign_and_send_utilisation failed for {report_id}")
    
    
    # Запускаем поцедуру создания виртуальных наборов для вложений по отчету оборудования
    vtResult = create_virtual_tasks_from_equipment_report(report_id)
    
    
    # Если при создании ошибки то сообщаем и выходим без ошибки. Дальнейшая обработка в автомате невозможна
    if not vtResult:
        print(f"Ошибка при попытке создания виртуальных заданий на производство по отчету оборудования f{report_id}.")
        print(f"Дальнейшая обработка в автомате невозможна. Отчет оборудования f{report_id}.")
        return(f"Ошибка при попытке создания виртуальных заданий на производство по отчету оборудования f{report_id}.")
    print(f" Успешно обработан отчет оборудования {report_id}")

# --- ЛОГИКА ДЛЯ БАКЕТА: 20ab2a0c... / ПАПКА: productionOrders/ ---
def logic_start_virtualProdTask_emission(full_key):
    group, contact = PRODUCT_GROUP, CONTACT_PERSON
    print(f"[LOGIC-21] Запуск обработки productionOrders: {full_key}")
    production_order_id = full_key.split('/')[-1].replace('.json', '') 
    if not production_order_id:
        return f"No production_order_id found for {full_key}"
    # Проверяем задание оно должно быть виртуальным
    if production_order_id[0:2] != 'V-':
        print(f"Задание на производство не виртуальное, завершаем обработку. {production_order_id}")
        return f"Задание на производство не виртуальное, завершаем обработку. {production_order_id}"
    # Запускаем создание эмиссии
    resultCEmT = create_emission_task(production_order_id, group, contact)
    
    if not resultCEmT:
        raise RuntimeError(f"create_emission_task failed for {production_order_id}")

# --- ЛОГИКА ДЛЯ БАКЕТА: 20ab2a0c... / ПАПКА: aggReceipts/ ---
def logic_update_agg(full_key):
    group, contact = PRODUCT_GROUP, CONTACT_PERSON
    print(f"[LOGIC-92] Запуск обработки aggReceipts: {full_key}")
    production_order_id = full_key.split('/')[-1].replace('.json', '') 
    if not production_order_id:
        return f"No production_order_id found for {full_key}"

    result = update_aggregation_status(production_order_id, group)
    if not result:
        raise RuntimeError(f"update_aggregation_status failed for {production_order_id} result:{result}")
    if isinstance(result, dict) and "error" in result:
        print("Ошибка API при запросе статуса")
        raise RuntimeError(f"update_aggregation_status failed for {production_order_id}  result:{result}")
    result = result[0] if isinstance(result, list) and len(result) > 0 else result
    if isinstance(result, dict) and result.get('status')== 'CHECKED_OK':
        # Проверяем отчет перед обработкой
        result = check_aggregation_reports([production_order_id])
        return f"aggregation status for {production_order_id} is {result.get('status')}"
    else:
        raise RuntimeError(f"update_aggregation_status failed for {production_order_id} result:{result}")

# --- ГЛАВНЫЙ ВОРКЕР (РОУТЕР) ---
#@app.task(name='tasks.process_s3_event', bind=True)
@app.task(
    name='tasks.process_s3_event',
    bind=True,
    acks_late=True,
    autoretry_for=(Exception,),
    retry_kwargs={'max_retries': 200}, # Увеличено до 200 для ожидания до 12+ часов
    retry_backoff=60,                 # 60, 120, 240, 300...
    retry_backoff_max=300,
    retry_jitter=True
)
def process_s3_event(self, data):
    bucket = data.get('bucket')
    key = data.get('key') or ""

    if not bucket or not key:
        return "Error: Missing bucket or key"

    full_key = f"{bucket}/{key}"
    print(f"\n[ROUTER] Новое событие S3: {full_key}")

    try:
        # Условие №1: Создание заказа
        if bucket == INPUT_BUCKET and key.startswith("Задания/"):
            result = logic_create_order(full_key)
            print(f"[OK] {result}")

        # Условие №2: Подписание эмиссии
        elif bucket == INTERNAL_BUCKET and key.startswith("emissionOrders/"):
            result = logic_sign_emission(full_key)
            print(f"[OK] {result}")
        # Условие №3: Обновление статуса эмиссии
        elif bucket == INTERNAL_BUCKET and key.startswith("emissionReceipts/"):
            result = logic_update_emission(full_key)
            print(f"[OK] {result}")
        # Условие №4: Получение кодов эмиссии
        elif bucket == INTERNAL_BUCKET and key.startswith("emissions/"):
            result = logic_get_emission_kodes(full_key)
            print(f"[OK] {result}")
        # Условие №5: Создание отчёта о нанесении
        elif bucket == INTERNAL_BUCKET and key.startswith("kodes/"):
            result = logic_kodes(full_key)
            print(f"[OK] {result}")
        # Условие №6: Создание отчёта о нанесении
        elif bucket == INTERNAL_BUCKET and key.startswith("utilisationReceipts/"):
            result = logic_utilisationReceipt(full_key)
            print(f"[OK] {result}")
        # Условие №7: Обновление статуса отчёта о нанесении
        elif bucket == INTERNAL_BUCKET and key.startswith("introduceReceipts/"):
            result = logic_update_introduce(full_key)
            print(f"[OK] {result}")
        # Условие №20: Обработка отчета оборудования 
        elif bucket == INTERNAL_BUCKET and key.startswith("equipment-reports/"):
            result = logic_start_equipment_reports(full_key)
            print(f"[OK] {result}")
        # Условие №21: Обработка виртуального заказа на производство
        elif bucket == INTERNAL_BUCKET and key.startswith("productionOrders/"):
            result = logic_start_virtualProdTask_emission(full_key)
            print(f"[OK] {result}")
        # Условие №92: Обработка чека об агрегации
        elif bucket == INTERNAL_BUCKET and key.startswith("aggReceipts/"):
            result = logic_update_agg(full_key)
            print(f"[OK] {result}")

        else:
            print(f"[SKIP] Бакет или папка не соответствуют фильтрам. Игнорирую.")
            return "Skipped: No match"

    except Exception as e:
        print(f"[ERROR] Критическая ошибка: {e}")
        # Celery перехватит это и сделает retry (если настроено) или залогирует
        raise e
