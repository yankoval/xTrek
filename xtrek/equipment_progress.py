"""Read-only evidence for resuming an applied equipment report, never a send."""
import hashlib
import json
from collections import Counter

from .aggregation_builder import iter_equipment_report_boxes
from .operation_state import fingerprint, ReconciliationRequired
from .storage import get_storage
from .terminal_feedback import WorkflowPending, TerminalBusinessError


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
