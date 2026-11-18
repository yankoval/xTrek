"""Read-only evidence for resuming an applied equipment report, never a send."""
import hashlib
import json
from collections import Counter

from .aggregation_builder import iter_equipment_report_boxes
from .operation_state import fingerprint, ReconciliationRequired
from .storage import get_storage
from .terminal_feedback import WorkflowPending, TerminalBusinessError


def report_completed(report_id, source_path, config):
    """Recognize the same completed scan from durable aggregation evidence.

    Current CIS states are not completion evidence: goods can be retired after
    successful aggregation. The preparation guard binds the saved result to the
    exact source contents, so a replacement report cannot inherit completion.
    This probe never writes a tag, guard or terminal event.
    """
    if not config.get('aggs'):
        return False

    def read(path):
        stored = get_storage(path, config.get('s3_config')).read_lock_object(path)
        return json.loads(stored[0]) if stored else None

    def document(root):
        return read(root.rstrip('/') + '/' + report_id + '.json') if root else None

    try:
        status = document(config['aggs'])
        if isinstance(status, list) and len(status) == 1:
            status = status[0]
        if not isinstance(status, dict) or status.get('status') != 'CHECKED_OK':
            return False
        if (status.get('productionOrderId') != report_id
                or status.get('type') != 'AGGREGATION_DOCUMENT'
                or not status.get('number')):
            return False
        receipt = document(config.get('agg-receipts'))
        if (not isinstance(receipt, dict)
                or receipt.get('productionOrderId') != report_id
                or receipt.get('document_id') != status['number']):
            return False
        root = config.get('operation_state_path')
        if not root:
            return False
        guard = read(root.rstrip('/') + '/prepare-aggregation/'
                     + hashlib.sha256(report_id.encode()).hexdigest() + '.json')
        if (not isinstance(guard, dict) or guard.get('phase') != 'complete'
                or guard.get('operation') != 'prepare-aggregation'
                or guard.get('business_key') != report_id):
            return False
        source = get_storage(source_path, config.get('s3_config')).read_text(source_path)
        # Preparation supports both automatic INN selection and --inn. The
        # successful document supplies the latter candidate; never ignore the
        # parameter or the source fingerprint when matching the saved guard.
        overrides = [None]
        sender_inn = status.get('senderInn')
        if (isinstance(sender_inn, str) and sender_inn.isascii()
                and sender_inn.isdigit() and len(sender_inn) in {10, 12}):
            overrides.append(sender_inn)
        source_hash = fingerprint(source)
        return any(guard.get('input_hash') == fingerprint(json.dumps({
            'source': source_hash, 'parameters': {'inn_override': inn}}))
                   for inn in overrides)
    except Exception as exc:
        # An unavailable completion source must not turn a historical success
        # into a fresh status rejection. Retry without notifying the operator.
        raise WorkflowPending('Waiting for verified report completion') from exc


def applied_checkpoint(report_id, config):
    """Require the exact accepted request/receipt before continuing downstream.

    APPLIED alone proves neither ownership of a submission nor its outcome.
    Missing evidence waits. Conflicting/uncertain evidence requires manual review.
    """
    def read(root, key):
        if not root:
            raise WorkflowPending('Waiting for configured progress source')
        path = root.rstrip('/') + '/' + key + '.json'
        storage = get_storage(path, config.get('s3_config'))
        if not storage.exists(path):
            raise WorkflowPending('Waiting for accepted utilisation evidence')
        return storage.read_text(path)

    order = json.loads(read(config.get('production_orders_path'), report_id))
    if order.get('GtinType') not in {'SET', 'UNIT'}:
        raise WorkflowPending('Waiting for verified product type')
    task_text = read(config.get('utilisation_tasks_path'), report_id)
    task = json.loads(task_text)
    report = json.loads(read(config.get('equipment-reports'), report_id))
    source_codes = Counter(code.split('\x1d')[0] for box in iter_equipment_report_boxes(report)
                           for code in box['productNumbersFull'])
    submitted = task.get('sntins')
    if (not source_codes or not isinstance(submitted, list)
            or any(not isinstance(code, str) for code in submitted)
            or source_codes != Counter(code.split('\x1d')[0] for code in submitted)
            or task.get('productionOrderId') != report_id):
        raise ReconciliationRequired('Accepted utilisation differs from report')
    receipt = json.loads(read(config.get('utilisation_receipts'), report_id))
    if (not receipt.get('reportId') or receipt.get('productionOrderId') != report_id
            or receipt.get('orderId') != report_id):
        raise ReconciliationRequired('Utilisation receipt identity mismatch')
    root = config.get('operation_state_path')
    if not root:
        raise WorkflowPending('Waiting for durable submission evidence')
    path = root.rstrip('/') + '/send-utilisation/' + hashlib.sha256(report_id.encode()).hexdigest() + '.json'
    stored = get_storage(root, config.get('s3_config')).read_lock_object(path)
    if not stored:
        raise WorkflowPending('Waiting for durable submission evidence')
    guard = json.loads(stored[0])
    if guard.get('phase') in {'uncertain', 'external', 'rejected'}:
        raise ReconciliationRequired('Utilisation submission requires reconciliation')
    if guard.get('phase') not in {'accepted', 'complete'}:
        raise WorkflowPending('Waiting for active utilisation submission')
    expected_hash = fingerprint(json.dumps({'source': fingerprint(task_text),
                                          'parameters': {'oms_id': None, 'client_token': None}}))
    saved = guard.get('result', {}).get('value')
    saved_id = saved.get('reportId') if isinstance(saved, dict) else saved
    if (guard.get('operation') != 'send-utilisation' or guard.get('business_key') != report_id
            or guard.get('input_hash') != expected_hash or saved_id != receipt['reportId']):
        raise ReconciliationRequired('Utilisation checkpoint conflicts with source')
    status = json.loads(read(config.get('utilisation_reports'), report_id))
    if status.get('reportId') != receipt['reportId'] or status.get('productionOrderId') != report_id:
        raise ReconciliationRequired('Utilisation status identity mismatch')
    if status.get('reportStatus') in {'FAILED', 'REJECTED'}:
        error = TerminalBusinessError('Utilisation rejected')
        error.category = 'utilisation_rejected'
        raise error
    if status.get('reportStatus') != 'SUCCESS':
        raise WorkflowPending('Waiting for utilisation SUCCESS')
    return order['GtinType']


def has_report_progress(report_id, config):
    """Distinguish a new scan from this report's own progress, without writes.

    Missing configuration/unavailable storage is unknown, never proof that the
    equipment scan is a duplicate. Existing evidence retains the old guarded
    continuation path; this helper does not establish acceptance or success.
    """
    for name in ('utilisation_tasks_path', 'utilisation_receipts', 'utilisation_reports'):
        root = config.get(name)
        if not root:
            raise WorkflowPending('Waiting for repeat-scan progress sources')
        path = root.rstrip('/') + '/' + report_id + '.json'
        if get_storage(path, config.get('s3_config')).exists(path):
            return True
    root = config.get('operation_state_path')
    if not root:
        raise WorkflowPending('Waiting for repeat-scan submission evidence')
    path = root.rstrip('/') + '/send-utilisation/' + hashlib.sha256(report_id.encode()).hexdigest() + '.json'
    return get_storage(root, config.get('s3_config')).read_lock_object(path) is not None
