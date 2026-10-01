import json
import base64
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from xtrek import create_emission_task_sample as workflow
from xtrek import sign
from xtrek.storage import LocalStorage


class FakeReceiptStorage:
    def __init__(self, objects=None, acquire_result=True):
        self.objects = dict(objects or {})
        self.acquire_result = acquire_result
        self.acquired = []
        self.released = []

    def exists(self, path):
        return path in self.objects

    def read_text(self, path):
        return self.objects[path]

    def acquire_lock(self, path, content=""):
        self.acquired.append((path, json.loads(content)))
        return self.acquire_result

    def release_lock(self, path):
        self.released.append(path)
        return path


def test_claim_reuses_existing_receipt_without_acquiring_lock(monkeypatch):
    receipts_path = "s3://internal/introduceReceipts"
    receipt_path = f"{receipts_path}/ORDER-1.json"
    receipt = {
        "document_id": "DOC-1",
        "productionOrderId": "V-ORDER-1",
    }
    storage = FakeReceiptStorage({receipt_path: json.dumps(receipt)})
    monkeypatch.setattr(workflow, "get_storage", lambda path, config: storage)

    result, claimed_storage, claimed_receipt_path, lock_path = (
        workflow._claim_document_submission(
            receipts_path,
            {},
            "introduce",
            "ORDER-1",
        )
    )

    assert result == receipt
    assert claimed_storage is storage
    assert claimed_receipt_path == receipt_path
    assert lock_path is None
    assert storage.acquired == []


def test_claim_uses_atomic_lock_outside_watched_receipt_prefix(monkeypatch):
    storage = FakeReceiptStorage()
    monkeypatch.setattr(workflow, "get_storage", lambda path, config: storage)

    result, _, receipt_path, lock_path = workflow._claim_document_submission(
        "s3://internal/aggReceipts",
        {},
        "aggregation",
        "T-1",
    )

    assert result is None
    assert receipt_path == "s3://internal/aggReceipts/T-1.json"
    assert lock_path == (
        "s3://internal/documentSubmissionLocks/aggregation/T-1.lock"
    )
    assert storage.acquired[0][0] == lock_path
    assert storage.acquired[0][1]["operation"] == "aggregation"
    assert storage.acquired[0][1]["taskId"] == "T-1"


def test_claim_fails_closed_when_another_worker_holds_lock(monkeypatch):
    storage = FakeReceiptStorage(acquire_result=False)
    monkeypatch.setattr(workflow, "get_storage", lambda path, config: storage)

    with pytest.raises(RuntimeError, match="уже выполняется другим воркером"):
        workflow._claim_document_submission(
            "s3://internal/aggSetReceipts",
            {},
            "aggregation-set",
            "T-1",
        )

    assert storage.released == []


def test_claim_fails_closed_for_malformed_existing_receipt(monkeypatch):
    receipts_path = "s3://internal/aggReceipts"
    receipt_path = f"{receipts_path}/T-1.json"
    storage = FakeReceiptStorage({receipt_path: json.dumps({"error": "bad"})})
    monkeypatch.setattr(workflow, "get_storage", lambda path, config: storage)

    with pytest.raises(RuntimeError, match="Повторная отправка заблокирована"):
        workflow._claim_document_submission(
            receipts_path,
            {},
            "aggregation",
            "T-1",
        )

    assert storage.acquired == []


@pytest.mark.parametrize(
    ("function_name", "identifier", "config", "args", "receipts_path"),
    [
        (
            "sign_and_send_introduce",
            "ORDER-1",
            {
                "introduce-tasks": "s3://internal/introduceTasks",
                "introduce-receipts": "s3://internal/introduceReceipts",
                "s3_config": {},
            },
            ("ORDER-1", "chemistry", "s3://internal/sign", 120),
            "s3://internal/introduceReceipts",
        ),
        (
            "sign_and_send_aggregation_set",
            "T-1",
            {
                "agg_set_tasks": "s3://internal/aggSetTasks",
                "agg_set_receipts": "s3://internal/aggSetReceipts",
                "s3_config": {},
            },
            ("T-1", "chemistry", "s3://internal/sign", 120),
            "s3://internal/aggSetReceipts",
        ),
        (
            "sign_and_send_aggregation",
            "T-1",
            {
                "agg-tasks": "s3://internal/aggTasks",
                "agg-receipts": "s3://internal/aggReceipts",
                "s3_config": {},
            },
            ("T-1", "chemistry", "s3://internal/sign", 120),
            "s3://internal/aggReceipts",
        ),
    ],
)
def test_send_functions_skip_before_signing_when_receipt_exists(
    monkeypatch,
    function_name,
    identifier,
    config,
    args,
    receipts_path,
):
    receipt_path = f"{receipts_path}/{identifier}.json"
    receipt = {
        "document_id": "ALREADY-SENT-DOC",
        "productionOrderId": identifier,
    }
    storage = FakeReceiptStorage({receipt_path: json.dumps(receipt)})
    storage_requests = []

    monkeypatch.setattr(workflow, "load_config", lambda name: config)

    def get_storage(path, s3_config):
        storage_requests.append(path)
        assert path == receipts_path
        return storage

    monkeypatch.setattr(workflow, "get_storage", get_storage)

    result = getattr(workflow, function_name)(*args)

    assert result == receipt
    assert storage_requests == [receipts_path]
    assert storage.acquired == []


# Exercise actual workflow payload/receipt handling with both signature routes.
# Only API, token, crypto and storage transport boundaries are replaced.
_SIGN_INN = '7701234567'
_SIGN_THUMB = 'A1' * 20
_SIGN_SIGNATURE = 'c2lnbmF0dXJl'

_DOCUMENT_CASES = [
    ('emission', 'emission_orders_path', 'emission_receipts',
     {'products': [{'gtin': '04600000000000'}], 'attributes': {'contactPerson': 'Тест'}}, None),
    ('utilisation', 'utilisation_tasks_path', 'utilisation_receipts',
     {'attributes': {'participantId': _SIGN_INN}, 'sntins': ['010460000000000021AAA\x1d93TEST'],
      'productionOrderId': 'PROD-1'}, None),
    ('introduce', 'introduce-tasks', 'introduce-receipts',
     {'participant_inn': _SIGN_INN, 'productionOrderId': 'PROD-1'}, 'LP_INTRODUCE_GOODS'),
    ('aggregation', 'agg-tasks', 'agg-receipts',
     {'participantId': _SIGN_INN, 'aggregationUnits': []}, 'AGGREGATION_DOCUMENT'),
    ('aggregation_set', 'agg_set_tasks', 'agg_set_receipts',
     {'participantId': _SIGN_INN, 'aggregationUnits': []}, 'SETS_AGGREGATION'),
    ('disaggregation', 'disaggregation-tasks', 'disaggregation-receipts',
     {'participant_inn': _SIGN_INN, 'products_list': [{'uitu': '00000123456789012345'}]},
     'DISAGGREGATION_DOCUMENT'),
    ('reaggregation', 'reaggregation-tasks', 'reaggregation-receipts',
     {'participant_inn': _SIGN_INN, 'reaggregation_type': 'REMOVING',
      'uitu': '00000123456789012345', 'uit_uitu_list': [{'uit_uitu': '010460000000000021AAA'}]},
     'REAGGREGATION_DOCUMENT'),
    *[('cis_information_change', 'cis-information-change-tasks', 'cis-information-change-receipts',
       {'participantInn': _SIGN_INN, 'codes': [{'code': ['010460000000000021AAA'], field: value}]},
       'CIS_INFORMATION_CHANGE') for field, value in
      [('productionDate', '2026-08-01'), ('expirationDate', '2099-12-31')]],
]


@pytest.fixture(params=_DOCUMENT_CASES, ids=lambda case: case[0] + str(case[3].get('codes', '')))
def signing_workflow(request, tmp_path, monkeypatch):
    operation, tasks_key, receipts_key, payload, document_type = request.param
    source_dir = tmp_path / 'tasks'
    source_dir.mkdir()
    task_id = 'TASK-1'
    content = json.dumps(payload, ensure_ascii=False, indent=2) + '\n'
    (source_dir / (task_id + '.json')).write_text(content)
    config = {tasks_key: str(source_dir), receipts_key: str(tmp_path / 'receipts'),
              'sign': 's3://exchange/sign', 'SIGNING_TIMEOUT': 1,
              'production_orders_path': str(tmp_path / 'production')}
    production = tmp_path / 'production'
    production.mkdir()
    (production / (task_id + '.json')).write_text(json.dumps({
        'PasportData': {'Manufacturer_inn': _SIGN_INN}}))
    monkeypatch.setattr(workflow, 'load_config', lambda _: config)
    # GTIN owner differs: emission must choose Manufacturer_inn.
    monkeypatch.setattr(workflow, 'get_inn_by_gtin', lambda *a, **kw: '7707654321')
    monkeypatch.setattr(workflow, 'OrganizationManager', MagicMock())
    tokens = MagicMock()
    tokens.get_token_value_for.return_value = 'token'
    monkeypatch.setattr(workflow, 'TokenProcessor', MagicMock(return_value=tokens))
    business_storage = LocalStorage()
    for method in ('mark_processing', 'mark_finished', 'mark_error'):
        setattr(business_storage, method, MagicMock())
    def business(path, config):
        assert not path.startswith('s3://exchange'), 'workflow touched signing exchange'
        return business_storage
    monkeypatch.setattr(workflow, 'get_storage', business)
    signed_bytes = []
    def local_sign(data, **options):
        assert options['thumbprint'] == _SIGN_THUMB
        signed_bytes.append(data)
        return _SIGN_SIGNATURE
    local = MagicMock(side_effect=local_sign)
    monkeypatch.setattr(sign, 'sign_document', local)
    exchange = MagicMock()
    def upload(source, remote):
        signed_bytes.append(Path(source).read_bytes())
    exchange.upload.side_effect = upload
    exchange.exists.return_value = True
    exchange.download.side_effect = lambda remote, dest: Path(dest).write_text(_SIGN_SIGNATURE)
    exchange_factory = MagicMock(return_value=exchange)
    monkeypatch.setattr(sign, 'get_storage', exchange_factory)
    monkeypatch.setattr(sign.time, 'sleep', lambda _: None)
    api = MagicMock()
    def send_json(body, **kwargs):
        doc = json.loads(body)
        assert doc['type'] == document_type
        assert doc['signature'] == _SIGN_SIGNATURE
        assert base64.b64decode(doc['product_document']) == signed_bytes[-1]
        return {'document_id': 'DOC-1'}
    def send_files(body, signature, **kwargs):
        assert Path(body).read_bytes() == signed_bytes[-1]
        assert Path(signature).read_text() == _SIGN_SIGNATURE
        if operation == 'emission':
            return workflow.EmissionOrderreceipts('ORDER-1', 1, 'OMS')
        return 'REPORT-1'
    api.documents_create.side_effect = send_json
    api.order_create.side_effect = send_files
    api.utilisation_send.side_effect = send_files
    monkeypatch.setattr(workflow, 'HonestSignAPI', MagicMock(return_value=api))
    monkeypatch.setattr(workflow, 'SUZ', MagicMock(return_value=api))
    send = getattr(workflow, 'sign_and_send_' + operation)
    def execute():
        if operation in ('emission', 'utilisation'):
            return send(task_id, 's3://exchange/sign', 1, oms_id='OMS', client_token='CON')
        return send(task_id, 'chemistry', 's3://exchange/sign', 1)
    return dict(config=config, execute=execute, local=local, exchange=exchange_factory,
                signed_bytes=signed_bytes, api=api, storage=business_storage, tokens=tokens,
                receipt=tmp_path / 'receipts' / (task_id + '.json'), operation=operation,
                source_bytes=content.encode(), payload=payload)


@pytest.mark.parametrize('mode', ['local', 'storage'])
def test_every_document_uses_selected_signer_and_exact_sent_bytes(signing_workflow, mode):
    ctx = signing_workflow
    if mode == 'local':
        ctx['config']['signing'] = {'local_by_inn': {_SIGN_INN: {'thumbprint': _SIGN_THUMB}}}
    assert ctx['execute']()
    assert ctx['receipt'].is_file()
    assert len(ctx['signed_bytes']) == 1
    if ctx['operation'] in ('emission', 'utilisation'):
        payload = dict(ctx['payload'])
        payload.pop('productionOrderId', None)
        assert ctx['signed_bytes'][0] == json.dumps(payload, separators=(',', ':')).encode()
    else:
        assert ctx['signed_bytes'][0] == ctx['source_bytes']
    assert ctx['tokens'].get_token_value_for.call_args.args[0] == _SIGN_INN
    if mode == 'local':
        ctx['local'].assert_called_once()
        ctx['exchange'].assert_not_called()
    else:
        ctx['local'].assert_not_called()
        ctx['exchange'].assert_called_once()


def test_local_signing_failure_never_submits_or_falls_back(signing_workflow):
    ctx = signing_workflow
    ctx['config']['signing'] = {'local_by_inn': {_SIGN_INN: {'thumbprint': _SIGN_THUMB}}}
    ctx['local'].side_effect = sign.SigningError('certificate unavailable')
    with pytest.raises(sign.SigningError, match='certificate unavailable'):
        ctx['execute']()
    ctx['exchange'].assert_not_called()
    assert not ctx['api'].mock_calls
    assert not ctx['receipt'].exists()
    assert not list(ctx['receipt'].parent.parent.rglob('*.lock'))
    if ctx['operation'] in ('emission', 'utilisation'):
        ctx['storage'].mark_error.assert_called_once()
        ctx['storage'].mark_finished.assert_not_called()


@pytest.mark.parametrize('signing_workflow', [case for case in _DOCUMENT_CASES
    if case[0] not in ('emission', 'utilisation')], indirect=True)
@pytest.mark.parametrize('failure', ['response_lost', 'receipt_save'])
def test_local_signature_preserves_submission_lock_on_ambiguous_send(signing_workflow, failure):
    ctx = signing_workflow
    ctx['config']['signing'] = {'local_by_inn': {_SIGN_INN: {'thumbprint': _SIGN_THUMB}}}
    if failure == 'response_lost':
        ctx['api'].documents_create.side_effect = TimeoutError('response lost')
    else:
        ctx['storage'].upload = MagicMock(side_effect=OSError('receipt save failed'))
        ctx['storage'].write_text = MagicMock(side_effect=OSError('receipt save failed'))
    with pytest.raises((TimeoutError, OSError)):
        ctx['execute']()
    assert list(ctx['receipt'].parent.parent.rglob('*.lock'))
    assert not ctx['receipt'].exists()
    with pytest.raises(RuntimeError, match='IDEMPOTENCY'):
        ctx['execute']()
    ctx['api'].documents_create.assert_called_once()
    ctx['local'].assert_called_once()
    ctx['exchange'].assert_not_called()


@pytest.mark.parametrize('signing_workflow', [case for case in _DOCUMENT_CASES
    if case[0] not in ('emission', 'utilisation')], indirect=True)
def test_receipt_prevents_second_local_signature_and_send(signing_workflow):
    ctx = signing_workflow
    ctx['config']['signing'] = {'local_by_inn': {_SIGN_INN: {'thumbprint': _SIGN_THUMB}}}
    assert ctx['execute']() == ctx['execute']()
    ctx['api'].documents_create.assert_called_once()
    ctx['local'].assert_called_once()
    ctx['exchange'].assert_not_called()
