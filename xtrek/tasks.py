import os
import json
import re
from functools import partial
from celery import Celery
from celery.exceptions import Reject, Retry
from celery.signals import task_postrun, task_prerun, worker_ready
from kombu import Queue
from urllib.parse import quote, urlparse

# 1. Импорт вашей бизнес-логики
from xtrek.config_loader import load_config
from xtrek.sign import DocumentSigner
from xtrek.tokens import TokenProcessor
from xtrek.storage import get_storage
from xtrek.create_emission_task_sample import (
    process_incoming_task, 
    create_equipment_aggregation_task, 
    is_unlimited_equipment_quantity,
    create_emission_task, 
    sign_and_send_emission,
    update_emission_order_status,
    get_emission_kodes,
    create_virtual_utilisation_task,
    create_utilisation_task,
    create_introduce_task,
    sign_and_send_utilisation,
    update_utilisation_report_status,
    create_virtual_introduce_task,
    sign_and_send_introduce,
    update_introduce_status,
    create_virtual_tasks_from_equipment_report,
    create_virtual_production_tasks,
    create_equipment_set_report,
    create_utilisation_task_from_report,
    create_introduce_task_from_report,
    update_aggregation_status,
    _find_production_order_id_by_suz_order_id,
    create_equipment_set_report_from_report,
    create_aggregation_set_report,
    sign_and_send_aggregation_set,
    update_aggregation_set_status,
    create_aggregation_report,
    sign_and_send_aggregation,
    create_disaggregation_task_from_report,
    sign_and_send_disaggregation,
    update_disaggregation_status,
    create_reaggregation_removing_task_from_report,
    sign_and_send_reaggregation,
    update_reaggregation_status,
    sign_and_send_cis_information_change,
    update_cis_information_change_status,
)
from xtrek.prn_util import generate_prn_files
from xtrek.direct_mode import classify_input, direct_context, publish_direct_codes
from xtrek.utils import (
    check_aggregation_reports,
    set_ready_check,
    check_aggregation_report,
    check_disaggregation_reports,
    check_disaggregation_report,
    check_reaggregation_removing_reports,
    check_reaggregation_removing_report,
)
from xtrek.suz_api_models import (
    EmissionOrder, OrderAttributes, OrderProduct, EmissionOrderreceipts,
    EmissionOrderStatus, ProductionOrder, PasportData,
    UtilisationReport, UtilisationReportReceipt, UtilisationReportStatus,
    AggregationReport, AggregationUnit, EquipmentAggTask, EquipmentAggTaskReport,
    EquipmentAggBox, DocumentWrapper, IntroduceMessage, IntroduceProduct, GtinDocument
)
from .operation_state import guarded, OperationConflict, ReconciliationRequired
from .terminal_feedback import (TerminalBusinessError, TerminalRecorded, WorkflowPending, classify,
                                event_identity, source_stage)
from .terminal_feedback_runtime import worker_ledger
from .equipment_progress import applied_checkpoint

# 2. Настройки доступа
ACCESS_KEY = os.environ.get('YMQ_ACCESS_KEY')
SECRET_KEY = os.environ.get('YMQ_SECRET_KEY')
QUEUE_URL = os.environ.get('YMQ_QUEUE_URL')
LOCAL_POLLING = os.environ.get('XTREK_LOCAL_S3_POLLING') == '1'
REAL_QUEUE_NAME = (os.environ.get('XTREK_LOCAL_QUEUE_NAME', 'xtrek_s3_events') if LOCAL_POLLING
                   else os.environ.get('YMQ_QUEUE_NAME', 'queue_task_create_1C'))

safe_secret = quote(SECRET_KEY or '', safe='')
if LOCAL_POLLING:
    # Temporary single-host mode: persistent messages go to a local RabbitMQ
    # durable queue. Filesystem and SQLAlchemy transports do not reclaim a
    # message reliably after a worker dies while handling it.
    BROKER_URL = os.environ.get('XTREK_LOCAL_BROKER_URL')
    if not BROKER_URL:
        raise RuntimeError('XTREK_LOCAL_BROKER_URL must configure a dedicated local RabbitMQ vhost and user')
    _broker_parsed = urlparse(BROKER_URL)
    if (_broker_parsed.scheme not in {'amqp', 'amqps'}
            or _broker_parsed.hostname not in {'localhost', '127.0.0.1', '::1'}
            or not _broker_parsed.username or _broker_parsed.username == 'guest'
            or not _broker_parsed.password or not _broker_parsed.path.strip('/')):
        raise RuntimeError('Local RabbitMQ must use a dedicated credential and non-default vhost on loopback')
else:
    BROKER_URL = f'sqs://{ACCESS_KEY}:{safe_secret}@'

app = Celery('tasks', broker=BROKER_URL)


@task_prerun.connect
def _start_token_snapshot(**kwargs):
    """Каждая Celery-задача должна получить актуальный снимок tokens.json из S3."""
    TokenProcessor.clear_command_snapshots()
    _record_local_event_state(kwargs.get('task_id'), 'started')


@task_postrun.connect
def _finish_token_snapshot(**kwargs):
    """Не переносим снимок токенов в следующую задачу долгоживущего worker."""
    TokenProcessor.clear_command_snapshots()
    state = kwargs.get('state')
    if state == 'SUCCESS':
        _record_local_event_state(kwargs.get('task_id'), 'succeeded')
    elif state == 'RETRY':
        _record_local_event_state(kwargs.get('task_id'), 'retry_scheduled')
    elif state == 'FAILURE':
        _record_local_event_state(kwargs.get('task_id'), 'failed')


def _record_local_event_state(task_id, state, detail=None):
    if not LOCAL_POLLING or not task_id:
        return
    try:
        from xtrek.s3_poller import EventLedger
        ledger = EventLedger(os.environ.get('XTREK_S3_POLL_LEDGER', '/var/lib/xtrek/s3-poller/outbox.sqlite3'))
        ledger.task_state(task_id, state, detail)
        ledger.close()
    except Exception as exc:
        # RabbitMQ redelivery is authoritative; this ledger is for lifecycle evidence.
        print(f'Could not record local event state: {type(exc).__name__}')


# Загрузка конфигурации
config = load_config('suz_worker_config')
if os.environ.get('XTREK_PARALLEL_REQUIRED') == '1':
    state_path = config.get('operation_state_path', '')
    if not state_path.startswith('s3://'):
        raise RuntimeError('Parallel workers require a shared S3 operation_state_path')
    state_bucket = state_path[5:].split('/', 1)[0]
    business_buckets = {config.get('input_bucket', '1bf11148-3595-4a07-a089-d460153b7c7a'),
                        config.get('internal_bucket', '20ab2a0c-2726-4ba1-9c7c-7deae82941ff')}
    if state_bucket in business_buckets:
        raise RuntimeError('Parallel worker state must use a separate bucket without business triggers')

# Один снимок настроек подписи для этого экземпляра. Токены остаются динамическими.
document_signer = DocumentSigner(config)
sign_and_send_emission = partial(sign_and_send_emission, document_signer=document_signer)
sign_and_send_utilisation = partial(sign_and_send_utilisation, document_signer=document_signer)
sign_and_send_introduce = partial(sign_and_send_introduce, document_signer=document_signer)
sign_and_send_aggregation = partial(sign_and_send_aggregation, document_signer=document_signer)
sign_and_send_aggregation_set = partial(sign_and_send_aggregation_set, document_signer=document_signer)
sign_and_send_disaggregation = partial(sign_and_send_disaggregation, document_signer=document_signer)
sign_and_send_reaggregation = partial(sign_and_send_reaggregation, document_signer=document_signer)


sign_and_send_cis_information_change = partial(sign_and_send_cis_information_change, document_signer=document_signer)


@worker_ready.connect
def _check_local_signing(**kwargs):
    # После создания prefork-пула: не инициализируем CSP при импорте до fork.
    # Ошибки теста только логируются; реальные документы проверяются заново.
    return document_signer.self_test()

# Настройки бакетов
INPUT_BUCKET = config.get('input_bucket', "1bf11148-3595-4a07-a089-d460153b7c7a")
INTERNAL_BUCKET = config.get('internal_bucket', "20ab2a0c-2726-4ba1-9c7c-7deae82941ff")

# Глобальные настройки для процессов
PRODUCT_GROUP = config.get('product_group', "chemistry")
CONTACT_PERSON = config.get('contact_person', "scan")

# Директория для подписи
signing_dir = config.get('sign', r"Y:\BatchPassToPrint\tst")

eta_task_limit = int(config.get('worker_eta_task_limit', 100))
if eta_task_limit < 1:
    raise ValueError('worker_eta_task_limit must be positive')

_broker_options = ({'confirm_publish': True} if LOCAL_POLLING else {
    'region': 'ru-central1',
    'predefined_queues': {REAL_QUEUE_NAME: {'url': QUEUE_URL}},
})

app.conf.update(
    broker_transport_options=_broker_options,
    task_default_queue=REAL_QUEUE_NAME,
    task_queues=(Queue(REAL_QUEUE_NAME, durable=True),),
    task_default_delivery_mode='persistent',
    accept_content=['json'],
    task_serializer='json',
    result_serializer='json',
    broker_connection_retry_on_startup=True,
    worker_enable_remote_control=False,
    task_acks_late=True, # Подтверждаем удаление только после успеха
    task_reject_on_worker_lost=True,
    worker_prefetch_multiplier=1,
    # Status polls retry with countdown. Ten waiting retries blocked intake of
    # fresh equipment reports even while the execution process was idle.
    worker_eta_task_limit=eta_task_limit,
)

# --- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ---
_EQUIPMENT_REPORT_RETRYABLE_TRUE_API_HTTP_ERRORS = {
    401: "Unauthorized",
    408: "Request Timeout",
    429: "Too Many Requests",
    500: "Internal Server Error",
    502: "Bad Gateway",
    503: "Service Unavailable",
    504: "Gateway Timeout",
}
_EQUIPMENT_REPORT_STOP_TRUE_API_HTTP_ERRORS = {
    403: "Forbidden",
}


def _equipment_report_api_error_details(error):
    if isinstance(error, str):
        error_text = error
    else:
        error_text = json.dumps(error, ensure_ascii=False, default=str)

    status_codes = {
        int(match)
        for match in re.findall(r"\b([45]\d{2})\b", error_text)
    }
    return error_text, status_codes


def _describe_equipment_report_api_error(error):
    error_text, status_codes = _equipment_report_api_error_details(error)
    known_errors = {
        **_EQUIPMENT_REPORT_RETRYABLE_TRUE_API_HTTP_ERRORS,
        **_EQUIPMENT_REPORT_STOP_TRUE_API_HTTP_ERRORS,
    }
    detected = [
        "HTTP "
        f"{status_code}"
        f"{f' {known_errors[status_code]}' if status_code in known_errors else ''}"
        for status_code in sorted(status_codes)
    ]
    if detected:
        failure = ", ".join(detected)
    else:
        failure = "network/transport error or unclassified True API response"

    retryable_codes = ", ".join(
        str(status_code)
        for status_code in _EQUIPMENT_REPORT_RETRYABLE_TRUE_API_HTTP_ERRORS
    )
    return (
        f"{failure}; retryable HTTP codes: {retryable_codes}; "
        f"details: {error_text}"
    )


def _set_equipment_report_check_tag(report_path, tag_value):
    try:
        from xtrek.storage import get_storage

        storage = get_storage(report_path, config.get("s3_config"))
        storage.set_tags(report_path, {"check": tag_value})
        return True
    except Exception as exc:
        print(
            f"[WARN] Не удалось установить check={tag_value} "
            f"для {report_path}: {exc}"
        )
        return False


def _is_failed_send_result(result):
    if not result:
        return True
    if isinstance(result, dict):
        if result.get("error"):
            return True
        status_code = result.get("status_code") or result.get("code")
        if isinstance(status_code, int) and status_code >= 400:
            return True
        if isinstance(status_code, str) and status_code.isdigit() and int(status_code) >= 400:
            return True
    if isinstance(result, str) and "error" in result.lower():
        return True
    return False


def get_production_order_data(production_order_id):
    """
    Загружает данные производственного задания из S3/Локально.
    """
    from xtrek.storage import get_storage
    s3_config = config.get('s3_config')
    production_orders_path = config.get('production_orders_path')

    if not production_orders_path:
        # Пытаемся сконструировать путь из INTERNAL_BUCKET
        bucket = INTERNAL_BUCKET
        if bucket:
            production_orders_path = f"s3://{bucket}/productionOrders/"
            print(f"[*] production_orders_path не задан, используем: {production_orders_path}")
        else:
            return None

    storage = get_storage(production_orders_path, s3_config)
    path = f"{production_orders_path.rstrip('/')}/{production_order_id}.json"

    if not storage.exists(path):
        # Попробуем без .json если id уже содержит его или просто на всякий случай
        if not production_order_id.endswith('.json'):
            path = f"{production_orders_path.rstrip('/')}/{production_order_id}.json"

        if not storage.exists(path):
            print(f"[!] Файл задания не найден: {path}")
            return None

    try:
        content = storage.read_text(path)
        data = json.loads(content)
        return data
    except FileNotFoundError:
        return None
    except Exception as e:
        print(f"[!] Ошибка при чтении задания {production_order_id}: {e}")
        return None

def trigger_set_aggregation_if_ready(parent_id):
    """
    Проверяет готовность набора и запускает процесс агрегации наборов.
    """
    print(f"[*] Проверка готовности набора для {parent_id}...")
    # set_ready_check принимает путь к отчету оборудования.
    # В xtrek/utils.py resolve_file_path достроит его если передать просто ID.
    parent = (get_production_order_data(parent_id) or {}) if parent_id.startswith('T-direct-') else {}
    direct_set = parent.get('directMode') is True and parent.get('GtinType') == 'SET'
    ready_path = (config['production_orders_path'].rstrip('/') + '/' + parent_id + '.json'
                  if direct_set else parent_id)
    ready_status = set_ready_check(ready_path)

    if ready_status == "finished":
        return f"Set {parent_id} already finished"
    if ready_status == "setReady":
        print(f"[+++] Набор {parent_id} готов к агрегации. Запуск процедур...")

        # 1. Создаем отчет об агрегации наборов
        res1 = (create_equipment_set_report(parent_id) if direct_set
                else create_equipment_set_report_from_report(parent_id))
        if not res1:
            raise RuntimeError(f"create_equipment_set_report_from_report failed for {parent_id}")

        # 2. Создаем задачу на агрегацию наборов (в формате для ЛК)
        res2 = create_aggregation_set_report(parent_id, PRODUCT_GROUP)
        if not res2:
            raise RuntimeError(f"create_aggregation_set_report failed for {parent_id}")

        # 3. Подписываем и отправляем
        res3 = sign_and_send_aggregation_set(parent_id, PRODUCT_GROUP, signing_dir, 120)
        if not res3:
            raise RuntimeError(f"sign_and_send_aggregation_set failed for {parent_id}")

        return f"Set aggregation for {parent_id} started successfully"
    else:
        print(f"[*] Набор {parent_id} еще не готов к агрегации.")
        raise WorkflowPending("Waiting for set aggregation prerequisites")

# --- ЛОГИКА ДЛЯ БАКЕТА: 1bf11148... / ПАПКА: Задания ---
class CisInformationChangeRejected(TerminalBusinessError):
    """An existing correction document was definitively rejected by True API."""
    category = "cis_rejected"


def logic_send_cis_information_change(full_key):
    task_id = full_key.rsplit("/", 1)[-1].removesuffix(".json")
    result = sign_and_send_cis_information_change(task_id, PRODUCT_GROUP, signing_dir, 120)
    if _is_failed_send_result(result):
        raise RuntimeError(f"CIS_INFORMATION_CHANGE send failed for {task_id}")
    return result


def logic_update_cis_information_change(full_key):
    task_id = full_key.rsplit("/", 1)[-1].removesuffix(".json")
    result = update_cis_information_change_status(task_id, PRODUCT_GROUP)
    # This receipt identifies one existing document. Ambiguous or malformed
    # responses must never be accepted as either successful or terminal.
    target = result[0] if isinstance(result, list) and len(result) == 1 else result
    status = target.get("status") if isinstance(target, dict) else None
    if isinstance(target, dict) and "error" in target:
        raise RuntimeError("CIS_INFORMATION_CHANGE status not confirmed")
    if status == "CHECKED_NOT_OK":
        # The status updater has already persisted the API response and tag.
        # Leave receipts and send guards intact; fail the task without resending
        # the document or scheduling another status retry.
        raise CisInformationChangeRejected(
            f"CIS_INFORMATION_CHANGE rejected for {task_id}: CHECKED_NOT_OK"
        )
    if status != "CHECKED_OK":
        # Do not put the API payload (including marking codes) into the journal.
        raise RuntimeError(f"CIS_INFORMATION_CHANGE status not confirmed for {task_id}")
    return result


def logic_create_order(full_key, source_data=None):
    group, contact = PRODUCT_GROUP, CONTACT_PERSON
    print(f"[LOGIC-0] Запуск создания заказа для: {full_key}")
    
    production_order_id = process_incoming_task(s3_full_key=full_key)
    if not production_order_id:
        return "No production_order_id created"

    task = get_production_order_data(production_order_id) if production_order_id.startswith('T-direct-') else None
    if task and task.get('directMode') is True:
        codes_path = 's3://' + full_key.rsplit('/', 1)[0].removeprefix('s3://') + '/' + task['emissionDocumentId'] + '.json'
        codes_storage = get_storage(codes_path, config.get('s3_config'))
        if not codes_storage.exists(codes_path):
            raise RuntimeError('Waiting for emission code block: ' + task['emissionDocumentId'])
        block = json.loads(codes_storage.read_text(codes_path))
        order_id = publish_direct_codes(config, production_order_id, task, block)
        if task.get('GtinType') == 'SET':
            create_virtual_production_tasks(production_order_id, qty=len(block['codes']))
        return f'Direct task {production_order_id}: codes {order_id} published'

    equipment_task = create_equipment_aggregation_task(production_order_id)
    if not equipment_task:
        raise RuntimeError(f"create_equipment_aggregation_task failed for {production_order_id}")

    # Blank/missing Quantity requests an equipment aggregation task only.
    # Do not start emission: there is no finite code quantity to issue.
    if source_data is None:
        source_bucket, _, source_key = full_key.partition('/')
        try:
            source_config = config.get('s3_config')
            source_storage = get_storage(f"s3://{source_bucket}/{source_key}", source_config)
            source_data = json.loads(source_storage.read_text(f"s3://{source_bucket}/{source_key}"))
        except Exception as exc:
            # If we cannot establish the source quantity, fail closed before emission.
            raise RuntimeError(f"Could not verify source Quantity before emission: {full_key}") from exc
    if is_unlimited_equipment_quantity(source_data.get('Quantity')):
        return f"Equipment task {production_order_id} created; emission skipped (unlimited quantity)"

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
        elif result.bufferStatus == "REJECTED":
            print(f"[TERMINAL] Emission rejected for {production_order_id}; preserved for investigation")
            return f"Emission for {production_order_id} rejected"
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
    if result.get('bufferStatus') == 'REJECTED':
        return f"Emission {emission_order_id} rejected; no codes will be requested"
    if 'codes' in result.keys():
        total_codes = len(result['codes'])
    else: 
        total_codes = result.get('totalCodes', 'unknown count')
    return f"Emission codes for {emission_order_id} retrieved: {total_codes} codes"

# --- ЛОГИКА ДЛЯ БАКЕТА: 20ab2a0c... / ПАПКА: kodes/ ---
def logic_kodes(full_key):
    # Здесь предполагается, что full_key содержит ID эмиссии или путь к нему
    kodes_order_id = full_key.split('/')[-1].replace('.json', '') 
    
    print(f"[LOGIC-5] Обработка файла кодов: {kodes_order_id}")
    
    context = direct_context(config, kodes_order_id)
    if context:
        if not create_utilisation_task(kodes_order_id, PRODUCT_GROUP, production_order_id=context[0]):
            raise RuntimeError('Direct utilisation preparation failed')
        if not sign_and_send_utilisation(kodes_order_id, signing_dir, 120):
            raise RuntimeError('Direct utilisation submission failed')
        return f'Direct utilisation for {kodes_order_id} submitted'

    # Создание задания на печать
    try:
        result = generate_prn_files(kodes_order_id)
        if not result:
            raise RuntimeError(f"generate_prn_files failed for {kodes_order_id}")
    except Exception:
        raise
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
    print(f"Utilisation task for {utilisationReceipt_id} status is {result}")
    if type(result) is not UtilisationReportStatus:
        raise RuntimeError(f"logic_utilisationReceipt failed for {utilisationReceipt_id} result:{result}")
    if result.reportStatus in {'FAILED', 'REJECTED'}:
        failure = TerminalBusinessError('Utilisation document rejected')
        failure.category = 'utilisation_rejected'
        raise failure
    if result.reportStatus !="SUCCESS":
        raise WorkflowPending(f"logic_utilisationReceipt failed for {utilisationReceipt_id} result status:{result.reportStatus}")

    context = direct_context(config, utilisationReceipt_id)
    if context:
        if context[1].get('GtinType') == 'SET':
            return trigger_set_aggregation_if_ready(context[0])
        if not create_introduce_task(utilisationReceipt_id, PRODUCT_GROUP, production_order_id=context[0]):
            raise RuntimeError('Direct introduction preparation failed')
        if not sign_and_send_introduce(utilisationReceipt_id, PRODUCT_GROUP, signing_dir, 120):
            raise RuntimeError('Direct introduction submission failed')
        return f'Direct introduction for {utilisationReceipt_id} submitted'

    # Если это реальный заказ
    if utilisationReceipt_id.startswith('T-'):
        prod_data = get_production_order_data(utilisationReceipt_id)
        gtin_type = prod_data.get('GtinType') if prod_data else None

        # Если тип не указан в задании, пробуем определить его (для старых заданий)
        if not gtin_type:
            gtin = prod_data.get('Gtin') if prod_data else None

            # Если в задании нет GTIN, попробуем найти его через файл статуса эмиссии
            if not gtin:
                print(f"[*] GTIN не найден в задании {utilisationReceipt_id}, ищем в эмиссии...")
                try:
                    from xtrek.storage import get_storage
                    s3_config = config.get('s3_config')
                    # UTIL_ID совпадает с orderId в этом контексте
                    emissions_path = config.get('emissions_path', f"s3://{INTERNAL_BUCKET}/emissions/")
                    storage_em = get_storage(emissions_path, s3_config)
                    # Ищем файл {utilisationReceipt_id}.json
                    em_file = f"{emissions_path.rstrip('/')}/{utilisationReceipt_id}.json"
                    if storage_em.exists(em_file):
                        em_data = json.loads(storage_em.read_text(em_file))
                        gtin = em_data.get('gtin')
                        print(f"[*] GTIN из эмиссии: {gtin}")
                except Exception as e:
                    print(f"[!] Ошибка при поиске GTIN в эмиссии: {e}")

            if gtin:
                print(f"[*] Определение типа для GTIN {gtin}...")
                try:
                    from xtrek.utils import _ensure_resources, AggregationAnalyzer
                    # Используем ID квитанции для обеспечения ресурсов
                    _, api, nk, _ = _ensure_resources(utilisationReceipt_id)
                    analyzer = AggregationAnalyzer(api, nk)
                    is_set = analyzer.is_set(gtin)
                    gtin_type = 'SET' if is_set else 'UNIT'
                    print(f"[*] Определен тип GTIN: {gtin_type}")
                except Exception as e:
                    print(f"[!] Не удалось определить тип GTIN для {gtin}: {e}")

        if gtin_type == 'UNIT':
            print(f"[*] Задание {utilisationReceipt_id} является UNIT. Запуск ввода в оборот...")
            res1 = create_introduce_task_from_report(utilisationReceipt_id, PRODUCT_GROUP)
            if not res1:
                raise RuntimeError(f"create_introduce_task_from_report failed for {utilisationReceipt_id}")
            res2 = sign_and_send_introduce(utilisationReceipt_id, PRODUCT_GROUP, signing_dir, 120)
            if _is_failed_send_result(res2):
                raise RuntimeError(f"sign_and_send_introduce failed for {utilisationReceipt_id}")
            return f"Introduction task for UNIT {utilisationReceipt_id} started successfully"
        else:
            # По умолчанию считаем SET для совместимости
            trigger_set_aggregation_if_ready(utilisationReceipt_id)
            return f"Utilization report for set {utilisationReceipt_id} is SUCCESS, checked set readiness"

    # Если документ относиться к виртуальному заданию на производство то запускаем создание сообщения о вводе в оборот
    result = create_virtual_introduce_task(utilisationReceipt_id, PRODUCT_GROUP)
    if not result:
        raise RuntimeError(f"create_virtual_introduce_task failed for {utilisationReceipt_id}")
    if type(result) is ProductionOrder:
        if result.virtual:
            result = sign_and_send_introduce(utilisationReceipt_id, PRODUCT_GROUP, signing_dir, 120)
            if _is_failed_send_result(result):
                raise RuntimeError(f"sign_and_send_introduce failed for {utilisationReceipt_id}")

            return f"Introduce task for virtual {utilisationReceipt_id} has been send successfully:{result}"
        else:
            #TODO: Create print task if not virtual
            return f"For real production order {utilisationReceipt_id}, skip send introduce task in virtual task flow."

    return f"Utilisation task for {utilisationReceipt_id} status is {result}"

# --- ЛОГИКА ДЛЯ БАКЕТА: 20ab2a0c... / ПАПКА: emissionRecepts ---
def _finish_direct_production_order(production_order_id):
    """Persist completion before acknowledging the route; tag failures must retry."""
    path = config['production_orders_path'].rstrip('/') + '/' + production_order_id + '.json'
    get_storage(path, config.get('s3_config')).set_tags(path, {'check': 'finished'})


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
    result = result[0] if isinstance(result, list) and len(result) == 1 else result
    if isinstance(result, dict) and "error" not in result and result.get('status') == 'CHECKED_NOT_OK':
        failure = TerminalBusinessError('Introduction document rejected')
        failure.category = 'introduction_rejected'
        raise failure
    if isinstance(result, dict) and "error" not in result and result.get('status') == 'CHECKED_OK':
        prod_id = result.get('productionOrderId') or _find_production_order_id_by_suz_order_id(introduceReceipt_id)
        if not prod_id:
            return f"introduce status for {introduceReceipt_id} is CHECKED_OK, but prod_id not found"

        context = direct_context(config, introduceReceipt_id)
        if context:
            if context[1].get('GtinType') != 'UNIT' or context[0] != prod_id:
                raise TerminalBusinessError('Direct introduction does not match its UNIT task')
            _finish_direct_production_order(prod_id)
            return f'Direct introduction for {introduceReceipt_id} completed (CHECKED_OK)'

        # Если это виртуальный компонент, пробуем запустить агрегацию набора
        if prod_id.startswith('V-'):
            # Извлекаем parent_id из V-{parent_id}-{gtin}
            parts = prod_id[2:].split('-')
            if len(parts) > 1:
                parent_id = "-".join(parts[:-1])
                trigger_set_aggregation_if_ready(parent_id)

        # Если это реальный UNIT, запускаем транспортную агрегацию
        elif prod_id.startswith('T-'):
            prod_data = get_production_order_data(prod_id)
            if prod_data and prod_data.get('GtinType') == 'UNIT':
                print(f"[*] Ввод в оборот для UNIT {prod_id} успешен. Запуск агрегации...")
                res1 = create_aggregation_report(prod_id)
                if not res1:
                    raise RuntimeError(f"create_aggregation_report failed for {prod_id}")
                res2 = sign_and_send_aggregation(prod_id, PRODUCT_GROUP, signing_dir, 120)
                if _is_failed_send_result(res2):
                    raise RuntimeError(f"sign_and_send_aggregation failed for {prod_id}")
                return f"Standard aggregation for UNIT {prod_id} started successfully"

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
    
    # Ошибки авторизации (401), таймауты и rate limit (408, 429), ошибки
    # True API/upstream (500, 502, 503, 504), а также сетевые ошибки без
    # HTTP-кода означают, что отчет фактически не проверен.
    # Поднимаем исключение, чтобы Celery повторил проверку после восстановления,
    # не переходя к созданию документов.
    if not result:
        raise RuntimeError(
            f"check_aggregation_reports returned no result for {production_order_id}"
        )

    error = next(iter(result.values()))
    if isinstance(error, dict) and set(error) == {'finished'}:
        return 'Equipment report already finished'
    if isinstance(error, dict) and 'waitingstatus' in error and set(error) <= {'waitingstatus', 'alreadyregistered'}:
        # An APPLIED code is progress, not a rejection. Resume only from the
        # exact durable accepted request; this path never calls utilisation POST.
        kind = applied_checkpoint(report_id, config)
        if kind == 'SET':
            if not create_virtual_tasks_from_equipment_report(report_id):
                raise WorkflowPending('Waiting for set component tasks')
            return trigger_set_aggregation_if_ready(report_id)
        if not create_introduce_task_from_report(report_id, group):
            raise WorkflowPending('Waiting for UNIT introduction prerequisites')
        sent = sign_and_send_introduce(report_id, group, signing_dir, 120)
        if _is_failed_send_result(sent):
            raise WorkflowPending('Waiting for UNIT introduction receipt')
        return 'Accepted UNIT utilisation continued to introduction'
    if error:
        if isinstance(error, dict) and error.get("api_error"):
            _, status_codes = _equipment_report_api_error_details(
                error["api_error"]
            )
            api_error = _describe_equipment_report_api_error(
                error["api_error"]
            )
            if status_codes & set(
                _EQUIPMENT_REPORT_STOP_TRUE_API_HTTP_ERRORS
            ):
                report_path = next(iter(result))
                _set_equipment_report_check_tag(
                    report_path,
                    "true-api-403-forbidden",
                )
                failure = TerminalBusinessError('Report access denied')
                failure.category = 'report_access_denied'
                raise failure
            raise RuntimeError(
                f"equipment report precheck True API failure for "
                f"{production_order_id}: {api_error}"
            )
        if isinstance(error, dict) and set(error) <= {'gtinnotfound', 'waitingstatus'}:
            raise WorkflowPending('Waiting for verified product information')
        failure = TerminalBusinessError('Report business validation requires reconciliation')
        if isinstance(error, dict):
            if set(error) & {'duplicateaggregation', 'duplicateattachment', 'invalidreport', 'minssccinaggrep'}:
                failure.category = 'invalid_report'
            elif 'alreadyregistered' in error:
                failure.category = 'report_aggregate_conflict'
            elif set(error) & {'wrongsetstatus', 'wrongunitstatus'}:
                failure.category = 'report_status_rejected'
            else:
                raise WorkflowPending('Waiting for confirmed validation outcome')
        raise failure
    
    # Запускаем процедуру создания/подписания/отправки отчета об утилизации по емиссии связаной с заказом на производство
    print(f"Запускаем процедуру создания/подписания/отправки отчета об утилизации по емиссии связаной с заказом на производство")
    utResult = create_utilisation_task_from_report(report_id, group)
    if not utResult:
        raise RuntimeError(f"create_utilisation_task_from_report failed for {report_id}")
    else:
        print("Подписываем/отправляем")
        sutResult = sign_and_send_utilisation(report_id, signing_dir, 120)
        if _is_failed_send_result(sutResult):
            raise RuntimeError(f"sign_and_send_utilisation failed for {report_id}")
    
    
    # Проверяем тип задания
    prod_data = get_production_order_data(production_order_id)
    gtin_type = prod_data.get('GtinType') if prod_data else None

    if gtin_type == 'UNIT':
        print(f"[*] Пропускаем создание виртуальных заданий для UNIT {production_order_id}")
    else:
        # Запускаем поцедуру создания виртуальных наборов для вложений по отчету оборудования
        vtResult = create_virtual_tasks_from_equipment_report(report_id)

        # Если при создании ошибки то сообщаем и выходим без ошибки. Дальнейшая обработка в автомате невозможна
        if not vtResult:
            print(f"Ошибка при попытке создания виртуальных заданий на производство по отчету оборудования f{report_id}.")
            print(f"Дальнейшая обработка в автомате невозможна. Отчет оборудования f{report_id}.")
            raise RuntimeError("Virtual task prerequisites are not confirmed")
    
    print(f" Успешно обработан отчет оборудования {report_id}")


def _aggregate_operation_report_path(full_key):
    return full_key if full_key.startswith("s3://") else f"s3://{full_key}"


def _aggregate_operation_precheck(result, report_id, operation):
    if not result:
        raise RuntimeError(
            f"{operation} report precheck returned no result for {report_id}"
        )

    report_path, error = next(iter(result.items()))
    if not error:
        return None

    if isinstance(error, dict):
        api_error_value = error.get("api_error") or error.get("error")
        if api_error_value:
            _, status_codes = _equipment_report_api_error_details(api_error_value)
            api_error = _describe_equipment_report_api_error(api_error_value)
            if status_codes & set(_EQUIPMENT_REPORT_STOP_TRUE_API_HTTP_ERRORS):
                _set_equipment_report_check_tag(
                    report_path,
                    "true-api-403-forbidden",
                )
                failure = TerminalBusinessError('Report access denied')
                failure.category = 'report_access_denied'
                raise failure
            raise RuntimeError(
                f"{operation} report precheck True API failure for "
                f"{report_id}: {api_error}"
            )

    raise TerminalBusinessError("Report business validation requires reconciliation")


def _aggregate_operation_status_target(result, operation, task_id):
    if not result:
        raise RuntimeError(
            f"update_{operation}_status returned no result for {task_id}"
        )
    if isinstance(result, dict) and result.get("error"):
        raise RuntimeError(
            f"update_{operation}_status failed for {task_id}: {result}"
        )
    target = result[0] if isinstance(result, list) and len(result) == 1 else result
    if not isinstance(target, dict) or "error" in target:
        raise RuntimeError(
            f"update_{operation}_status returned unexpected result for "
            f"{task_id}: {result}"
        )
    if target.get("status") == "CHECKED_NOT_OK":
        raise AggregationDocumentRejected("Aggregate operation rejected")
    return target


def _finish_aggregate_operation_report(operation, task_id, check_result):
    if check_result is None:
        raise RuntimeError(
            f"{operation} {task_id} is CHECKED_OK, but the operation is not "
            "reflected in the current aggregate state yet"
        )
    if isinstance(check_result, dict):
        api_error_value = check_result.get("api_error") or check_result.get("error")
        if api_error_value:
            api_error = _describe_equipment_report_api_error(api_error_value)
            raise RuntimeError(
                f"{operation} final report check True API failure for "
                f"{task_id}: {api_error}"
            )
        if check_result.get("finished"):
            return f"{operation} {task_id} finished and report tagged"
    raise TerminalBusinessError("Final report validation requires reconciliation")


# --- Расформирование агрегата: equipment report -> task -> receipt/status ---
def logic_create_disaggregation(full_key):
    task_id = full_key.split("/")[-1].removesuffix(".json")
    print(f"[LOGIC-30] Проверка отчета расформирования: {full_key}")
    precheck = _aggregate_operation_precheck(
        check_disaggregation_reports([task_id]),
        task_id,
        "disaggregation",
    )
    if precheck:
        return precheck

    result = create_disaggregation_task_from_report(
        _aggregate_operation_report_path(full_key),
        task_id=task_id,
    )
    if not result:
        raise RuntimeError(f"create_disaggregation_task failed for {task_id}")
    return f"Disaggregation task {task_id} created"


def logic_send_disaggregation(full_key):
    task_id = full_key.split("/")[-1].removesuffix(".json")
    print(f"[LOGIC-31] Подписание и отправка расформирования: {task_id}")
    result = sign_and_send_disaggregation(
        task_id,
        PRODUCT_GROUP,
        signing_dir,
        120,
    )
    if _is_failed_send_result(result):
        raise RuntimeError(f"sign_and_send_disaggregation failed for {task_id}")
    return f"Disaggregation task {task_id} sent"


def logic_update_disaggregation(full_key):
    task_id = full_key.split("/")[-1].removesuffix(".json")
    print(f"[LOGIC-32] Проверка статуса расформирования: {task_id}")
    target = _aggregate_operation_status_target(
        update_disaggregation_status(task_id, PRODUCT_GROUP),
        "disaggregation",
        task_id,
    )
    if target.get("status") != "CHECKED_OK":
        raise RuntimeError(
            f"disaggregation {task_id} status is {target.get('status') or 'unknown'}"
        )
    return _finish_aggregate_operation_report(
        "disaggregation",
        task_id,
        check_disaggregation_report(task_id, final=True),
    )


# --- Изъятие из агрегата: equipment report -> task -> receipt/status ---
def logic_create_reaggregation_removing(full_key):
    task_id = full_key.split("/")[-1].removesuffix(".json")
    print(f"[LOGIC-33] Проверка отчета изъятия: {full_key}")
    precheck = _aggregate_operation_precheck(
        check_reaggregation_removing_reports([task_id]),
        task_id,
        "reaggregation-removing",
    )
    if precheck:
        return precheck

    result = create_reaggregation_removing_task_from_report(
        _aggregate_operation_report_path(full_key),
        task_id=task_id,
    )
    if not result:
        raise RuntimeError(
            f"create_reaggregation_removing_task failed for {task_id}"
        )
    return f"Reaggregation REMOVING task {task_id} created"


def logic_send_reaggregation(full_key):
    task_id = full_key.split("/")[-1].removesuffix(".json")
    print(f"[LOGIC-34] Подписание и отправка изъятия: {task_id}")
    result = sign_and_send_reaggregation(
        task_id,
        PRODUCT_GROUP,
        signing_dir,
        120,
    )
    if _is_failed_send_result(result):
        raise RuntimeError(f"sign_and_send_reaggregation failed for {task_id}")
    return f"Reaggregation REMOVING task {task_id} sent"


def logic_update_reaggregation(full_key):
    task_id = full_key.split("/")[-1].removesuffix(".json")
    print(f"[LOGIC-35] Проверка статуса изъятия: {task_id}")
    target = _aggregate_operation_status_target(
        update_reaggregation_status(task_id, PRODUCT_GROUP),
        "reaggregation",
        task_id,
    )
    if target.get("status") != "CHECKED_OK":
        raise RuntimeError(
            f"reaggregation {task_id} status is {target.get('status') or 'unknown'}"
        )
    return _finish_aggregate_operation_report(
        "reaggregation-removing",
        task_id,
        check_reaggregation_removing_report(task_id),
    )

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

# --- ЛОГИКА ДЛЯ БАКЕТА: 20ab2a0c... / ПАПКА: aggSetReceipts/ ---
class AggregationDocumentRejected(TerminalBusinessError):
    """A rejected aggregation requires operator reconciliation, not a retry."""
    category = "aggregation_rejected"


def _require_checked_aggregation_status(result, production_order_id, operation):
    # A receipt refers to one existing document. Multiple results and API
    # errors cannot establish success or a definitive business rejection.
    target = result[0] if isinstance(result, list) and len(result) == 1 else result
    if not isinstance(target, dict) or "error" in target:
        raise RuntimeError(f"{operation} status not confirmed for {production_order_id}")
    status = target.get("status")
    if status == "CHECKED_NOT_OK":
        # ReconciliationRequired is already excluded from Celery autoretry.
        # Status evidence is persisted by the updater; keep the receipt and
        # send guard. Do not finalize the report or create another document.
        raise AggregationDocumentRejected(
            f"{operation} rejected for {production_order_id}: CHECKED_NOT_OK"
        )
    if status != "CHECKED_OK":
        raise RuntimeError(f"{operation} status not confirmed for {production_order_id}")
    return target


def logic_update_agg_set(full_key):
    group = PRODUCT_GROUP
    print(f"[LOGIC-93] Запуск обработки aggSetReceipts: {full_key}")
    production_order_id = full_key.split('/')[-1].replace('.json', '')
    if not production_order_id:
        return f"No production_order_id found for {full_key}"

    result = update_aggregation_set_status(production_order_id, group)
    result = _require_checked_aggregation_status(result, production_order_id, "aggregation-set")
    production = ((get_production_order_data(production_order_id) or {})
                  if production_order_id.startswith('T-direct-') else {})
    if production.get('directMode') is True and production.get('GtinType') == 'SET':
        _finish_direct_production_order(production_order_id)
        return f'Direct set {production_order_id} completed (CHECKED_OK)'

    print(f"[+++] Агрегация наборов для {production_order_id} успешна. Запуск стандартной агрегации...")

    # 1. Создаем отчет об агрегации (стандартный для ЛК)
    res1 = create_aggregation_report(production_order_id)
    if not res1:
        raise RuntimeError(f"create_aggregation_report failed for {production_order_id}")

    # 2. Подписываем и отправляем
    res2 = sign_and_send_aggregation(production_order_id, group, signing_dir, 120)
    if _is_failed_send_result(res2):
        raise RuntimeError(f"sign_and_send_aggregation failed for {production_order_id}")

    return f"Standard aggregation for {production_order_id} started successfully"

# --- ЛОГИКА ДЛЯ БАКЕТА: 20ab2a0c... / ПАПКА: aggReceipts/ ---
def logic_update_agg(full_key):
    group, contact = PRODUCT_GROUP, CONTACT_PERSON
    print(f"[LOGIC-92] Запуск обработки aggReceipts: {full_key}")
    production_order_id = full_key.split('/')[-1].replace('.json', '') 
    if not production_order_id:
        return f"No production_order_id found for {full_key}"

    result = update_aggregation_status(production_order_id, group)
    result = _require_checked_aggregation_status(result, production_order_id, "aggregation")
    # Выполняем проверку отчета и установку тега check
    print(f"[+++] Агрегация для {production_order_id} успешна. Проверка отчета...")
    check_aggregation_report(production_order_id)
    return f"aggregation status for {production_order_id} is CHECKED_OK, report checked and tagged"

# --- ГЛАВНЫЙ ВОРКЕР (РОУТЕР) ---
#@app.task(name='tasks.process_s3_event', bind=True)
# A route completes only after all its downstream stages succeeded. External
# mutations have separate durable guards, so a route may safely resume halfway.
for _route_name in tuple(name for name in globals() if name.startswith('logic_')):
    globals()[_route_name] = guarded('route-' + _route_name, cache_none=True,
                                     config_loader=lambda name: config)(globals()[_route_name])

def _retry_terminal_persistence(task, data, category):
    try:
        raise task.retry(args=(), kwargs={'data': data, 'terminal_failure': category},
                         exc=RuntimeError('Terminal ledger unavailable'), countdown=60,
                         max_retries=None)
    except Retry:
        raise
    except Exception:
        # If even the durable retry cannot be published, preserve the original
        # RabbitMQ delivery. Existing business send guards remain authoritative.
        raise Reject('Terminal persistence retry was not published', requeue=True) from None


@app.task(
    name='tasks.process_s3_event',
    bind=True,
    acks_late=True,
    autoretry_for=(Exception,),
    dont_autoretry_for=(OperationConflict, ReconciliationRequired, CisInformationChangeRejected, Reject),
    max_retries=None,  # Persistence-only retries must not lose a terminal event.
    retry_kwargs={'max_retries': None}, # Waiting/outages are not business rejections.
    retry_backoff=60,                 # 60, 120, 240, 300...
    retry_backoff_max=300,
    retry_jitter=True
)
def process_s3_event(self, data, terminal_failure=None):
    bucket = data.get('bucket')
    key = data.get('key') or ""

    if not bucket or not key:
        return "Error: Missing bucket or key"

    full_key = f"{bucket}/{key}"
    event_prefix = config.get("event_key_prefix", "")
    if event_prefix:
        event_prefix = event_prefix.strip("/") + "/"
        if not key.startswith(event_prefix):
            return "Skipped: Outside configured prefix"
        key = key[len(event_prefix):]
    # Check the terminal ledger before guarded business work. A lost acknowledgement
    # or replay cannot turn a recorded terminal failure back into a business retry.
    try:
        feedback_ledger = worker_ledger(config, bucket, key)
        if terminal_failure is not None:
            if feedback_ledger is None:
                raise RuntimeError('Terminal persistence requires enabled configuration')
            feedback_ledger.record(bucket, data['key'], source_stage(key), terminal_failure)
        recorded = feedback_ledger and feedback_ledger.get(event_identity(bucket, data['key']))
    except Exception:
        # Celery preserves this intent in the durable retry message. No business
        # route is entered while the ledger cannot be read/written.
        return _retry_terminal_persistence(self, data, terminal_failure)
    if recorded:
        raise TerminalRecorded('Equipment processing requires reconciliation')
    print(f"\n[ROUTER] Новое событие S3: {full_key}")

    try:
        # Условие №1: Создание заказа
        if bucket == INPUT_BUCKET and key.startswith("Задания/"):
            prnsrv_result = None
            source_storage = get_storage(f"s3://{full_key}", config.get('s3_config'))
            source = json.loads(source_storage.read_text(f"s3://{full_key}"))
            if classify_input(source) == 'codes':
                return 'Emission block received; awaiting its production task'
            unlimited_quantity = is_unlimited_equipment_quantity(source.get('Quantity'))
            if config.get('prnsrv_local_generator_enabled') is True and not unlimited_quantity and not source.get('directMode'):
                from xtrek.prnsrv_local_runtime import process_local_prnsrv_s3_event
                prnsrv_result = process_local_prnsrv_s3_event(
                    bucket=bucket,
                    key=key,
                    runtime_config=config,
                    event_id=str(getattr(getattr(self, 'request', None), 'id', '') or ''),
                )
            result = logic_create_order(full_key, source_data=source)
            if prnsrv_result is not None:
                result = f"{result}; local PRNSRV: {prnsrv_result.get('result', prnsrv_result.get('state'))}"
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
        # Условия №30-32: Расформирование агрегата
        elif (
            bucket == INTERNAL_BUCKET
            and key.startswith("equipment-reports-disaggregation/")
        ):
            result = logic_create_disaggregation(full_key)
            print(f"[OK] {result}")
        elif bucket == INTERNAL_BUCKET and key.startswith("disaggregationTasks/"):
            result = logic_send_disaggregation(full_key)
            print(f"[OK] {result}")
        elif (
            bucket == INTERNAL_BUCKET
            and key.startswith("disaggregationReceipts/")
        ):
            result = logic_update_disaggregation(full_key)
            print(f"[OK] {result}")
        # Условия №33-35: Изъятие без расформирования агрегата
        elif (
            bucket == INTERNAL_BUCKET
            and key.startswith("equipment-reports-reaggregation-removing/")
        ):
            result = logic_create_reaggregation_removing(full_key)
            print(f"[OK] {result}")
        elif bucket == INTERNAL_BUCKET and key.startswith("reaggregationTasks/"):
            result = logic_send_reaggregation(full_key)
            print(f"[OK] {result}")
        elif (
            bucket == INTERNAL_BUCKET
            and key.startswith("reaggregationReceipts/")
        ):
            result = logic_update_reaggregation(full_key)
            print(f"[OK] {result}")
        # Условие №21: Обработка виртуального заказа на производство
        elif bucket == INTERNAL_BUCKET and key.startswith("productionOrders/"):
            result = logic_start_virtualProdTask_emission(full_key)
            print(f"[OK] {result}")
        # Условие №92: Обработка чека об агрегации
        elif bucket == INTERNAL_BUCKET and key.startswith("aggReceipts/"):
            result = logic_update_agg(full_key)
            print(f"[OK] {result}")
        elif bucket == INTERNAL_BUCKET and key.startswith("cisInformationChangeTasks/"):
            result = logic_send_cis_information_change(full_key)
        elif bucket == INTERNAL_BUCKET and key.startswith("cisInformationChangeReceipts/"):
            result = logic_update_cis_information_change(full_key)
        # Условие №93: Обработка чека об агрегации наборов
        elif bucket == INTERNAL_BUCKET and key.startswith("aggSetReceipts/"):
            result = logic_update_agg_set(full_key)
            print(f"[OK] {result}")

        else:
            print(f"[SKIP] Бакет или папка не соответствуют фильтрам. Игнорирую.")
            return "Skipped: No match"

    except Exception as e:
        category = classify(e, retries=getattr(getattr(self, 'request', None), 'retries', 0),
                            retry_limit=200)
        if category and feedback_ledger:
            try:
                feedback_ledger.record(bucket, data['key'], source_stage(key), category)
            except Exception:
                return _retry_terminal_persistence(self, data, category)
            raise TerminalRecorded('Equipment processing requires reconciliation') from None
        print(f"[ERROR] Processing failed: {type(e).__name__}")
        raise


@app.task(name='tasks.deliver_pallet_report', bind=True, acks_late=True,
          reject_on_worker_lost=True)
def deliver_pallet_report(self, report_key):
    """Local RabbitMQ task: validated PNG attachment delivery on the VPS worker."""
    from xtrek.pallet_label_celery_runtime import deliver_pallet_report as deliver
    return deliver(report_key=report_key, runtime_config=load_config('suz_worker_config'))
