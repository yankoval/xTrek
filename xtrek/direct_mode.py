"""Shared input classification and validation for existing-code commissioning.

This module never submits documents or requests emission.
"""
import json
import re
from datetime import date
from uuid import UUID

MAX_CODES = 30_000
MAX_DOCUMENT_BYTES = 30_000_000


def uuid_key(value):
    if not isinstance(value, str):
        raise ValueError('emissionDocumentId must be a UUID string')
    parsed = str(UUID(value))
    if value.lower() != parsed:
        raise ValueError('UUID must use the canonical hyphenated format')
    return parsed


def classify_input(value):
    if not isinstance(value, dict):
        raise ValueError('Input must be a JSON object')
    has_codes = 'codes' in value
    has_task = 'Gtin' in value or 'PasportData' in value
    if has_codes and has_task:
        raise ValueError('Ambiguous input: codes and production task fields')
    if has_codes:
        validate_codes(value)
        return 'codes'
    if has_task and isinstance(value.get('Gtin'), str) and isinstance(value.get('PasportData'), dict):
        if 'directMode' in value and not isinstance(value['directMode'], bool):
            raise ValueError('directMode must be a JSON boolean')
        if value.get('directMode'):
            uuid_key(value.get('emissionDocumentId'))
        return 'task'
    raise ValueError('Unknown input schema')


def validate_codes(value, gtin=None):
    codes = value.get('codes')
    if not isinstance(codes, list) or not 1 <= len(codes) <= MAX_CODES:
        raise ValueError('A direct block must contain 1..30000 codes')
    seen = set()
    found_gtins = set()
    for code in codes:
        if not isinstance(code, str) or not re.fullmatch(r'01[0-9]{14}21[^\x00-\x20]+\x1d93[^\x00-\x20]+', code):
            raise ValueError('Invalid full chemistry marking code')
        short = code.split('\x1d', 1)[0]
        if short in seen:
            raise ValueError('Duplicate marking code in block')
        seen.add(short)
        found_gtins.add(code[2:16])
    if len(found_gtins) != 1 or (gtin is not None and found_gtins != {gtin}):
        raise ValueError('Block GTIN differs from production task')
    for name in ('omsId', 'blockId', 'orderId'):
        if name in value and value[name] is not None:
            if not isinstance(value[name], str):
                raise ValueError(name + ' must be a string')
            if value[name]:
                uuid_key(value[name])
    return len(codes)


def validate_direct_task(task, today=None, *, check_current_dates=True):
    if classify_input(task) != 'task' or task.get('directMode') is not True:
        raise ValueError('Expected a directMode production task')
    gtin = task['Gtin']
    if not re.fullmatch(r'[0-9]{14}', gtin):
        raise ValueError('directMode requires a 14-digit GTIN')
    passport = task['PasportData']
    if not re.fullmatch(r'(?:[0-9]{10}|[0-9]{12})', str(passport.get('Manufacturer_inn', ''))):
        raise ValueError('Manufacturer_inn is required for directMode')
    dates = []
    for field in ('Batch_date_production', 'Batch_date_expired'):
        raw = passport.get(field)
        if not isinstance(raw, str) or not re.fullmatch(r'[0-9]{4}-[0-9]{2}-[0-9]{2}', raw):
            raise ValueError(field + ' must be an explicit ISO date')
        dates.append(date.fromisoformat(raw))
    if today is None:
        from zoneinfo import ZoneInfo
        from datetime import datetime
        today = datetime.now(ZoneInfo('Europe/Moscow')).date()
    production, expiry = dates
    if expiry <= production or (check_current_dates and (production > today or expiry < today)):
        raise ValueError('Invalid production/expiration date interval')
    return task


def validate_document_size(document):
    text = json.dumps(document, ensure_ascii=True, separators=(',', ':'))
    if len(text.encode('utf-8')) > MAX_DOCUMENT_BYTES:
        raise ValueError('Document exceeds the 30 MB directMode limit')
    return text


def validate_pair(task, block, *, check_current_dates=True):
    """One complete SUZ order, one GTIN and one passport date interval."""
    validate_direct_task(task, check_current_dates=check_current_dates)
    order_id = uuid_key(task['emissionDocumentId'])
    if uuid_key(block.get('orderId')) != order_id:
        raise ValueError('Code orderId differs from emissionDocumentId')
    count = validate_codes(block, task['Gtin'])
    quantity = task.get('Quantity')
    if isinstance(quantity, bool) or str(quantity) != str(count):
        raise ValueError('Task Quantity must equal the complete code block size')
    if 'blocks' in block or block.get('blockCount', 1) != 1:
        raise ValueError('Only one complete emission block is supported')
    return order_id


def publish_direct_codes(config, production_order_id, task, block):
    """Atomically bind immutable codes to one immutable production passport.

    The binding is inside the same object as codes, so a crash or missing tags
    cannot orphan the ownership claim. An existing unbound kodes object is never
    adopted implicitly. Retries accept only identical content and ownership.
    """
    from .operation_state import fingerprint, publish_once, OperationConflict
    from .storage import get_storage
    order_id = validate_pair(task, block)
    if config.get('emissions_path'):
        emission_path = config['emissions_path'].rstrip('/') + '/' + order_id + '.json'
        emission_store = get_storage(config['emissions_path'], config.get('s3_config'))
        if emission_store.exists(emission_path):
            existing_order = json.loads(emission_store.read_text(emission_path)).get('productionOrderId')
            if existing_order and existing_order != production_order_id:
                raise OperationConflict('SUZ order is already bound by an existing emission')
    value = dict(block, orderId=order_id, directMode=True,
                 productionOrderId=production_order_id,
                 productionTaskFingerprint=fingerprint(json.dumps(task)))
    path = config['kodes'].rstrip('/') + '/' + order_id + '.json'
    storage = get_storage(config['kodes'], config.get('s3_config'))
    publish_once(storage, path, validate_document_size(value),
                 initial_tags={'productionOrderId': production_order_id})
    return order_id


def direct_context(config, order_id, storage_factory=None):
    """Return and revalidate a durable direct binding, or None for legacy codes."""
    from .operation_state import fingerprint, OperationConflict
    if not config.get('kodes'):
        return None
    from .storage import get_storage
    get_storage = storage_factory or get_storage
    path = config['kodes'].rstrip('/') + '/' + order_id + '.json'
    storage = get_storage(config['kodes'], config.get('s3_config'))
    if not storage.exists(path):
        return None
    block = json.loads(storage.read_text(path))
    if block.get('directMode') is not True:
        return None
    production_id = block.get('productionOrderId')
    if not isinstance(production_id, str) or not re.fullmatch(r'[A-Za-z0-9_.-]+', production_id):
        raise OperationConflict('Invalid direct production order binding')
    task_path = config['production_orders_path'].rstrip('/') + '/' + production_id + '.json'
    task = json.loads(get_storage(task_path, config.get('s3_config')).read_text(task_path))
    if fingerprint(json.dumps(task)) != block.get('productionTaskFingerprint'):
        raise OperationConflict('Direct production task changed after code binding')
    if validate_pair(task, block, check_current_dates=False) != order_id:
        raise OperationConflict('Direct code filename differs from SUZ orderId')
    return production_id, task


def validate_direct_document(config, order_id, document, kind, storage_factory=None):
    """Recheck the exact document immediately before publication/signing."""
    context = direct_context(config, order_id, storage_factory)
    if not context:
        return
    from .storage import get_storage
    get_storage = storage_factory or get_storage
    from .operation_state import OperationConflict
    production_id, task = context
    validate_direct_task(task)
    passport = task['PasportData']
    path = config['kodes'].rstrip('/') + '/' + order_id + '.json'
    codes = json.loads(get_storage(path, config.get('s3_config')).read_text(path))['codes']
    if document.get('productionOrderId') != production_id:
        raise OperationConflict('Document belongs to another production task')
    if kind == 'utilisation':
        attributes = document.get('attributes', {})
        valid = (document.get('sntins') == codes
                 and document.get('productGroup') == 'chemistry'
                 and attributes.get('productionDate') == passport['Batch_date_production']
                 and attributes.get('expirationDate') == passport['Batch_date_expired'])
    elif kind == 'introduce':
        if task.get('GtinType') == 'SET':
            raise OperationConflict('Direct SET requires component introduction and set aggregation')
        valid = (document.get('production_date') == passport['Batch_date_production']
                 and [p.get('uit_code') for p in document.get('products', [])]
                 == [code.split('\x1d')[0] for code in codes]
                 and all(str(document.get(field)) == str(passport['Manufacturer_inn'])
                         for field in ('owner_inn', 'producer_inn', 'participant_inn')))
    else:
        raise ValueError('Unknown document kind')
    if not valid:
        raise OperationConflict('Document codes, participant or dates differ from direct task')
    validate_document_size(document)
    return True
