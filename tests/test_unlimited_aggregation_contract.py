"""Synthetic sources only. Required SSCC is journaled; every other POST is blocked."""
import io
import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from xtrek import create_emission_task_sample as flow
from xtrek import equipment_task_contract as contract
from xtrek import utils, operation_state
from parallel_flow_store import SharedStorage
from xtrek.task_notifier import TaskNotifier, NotifierSettings, TASK_BUCKET
from xtrek.terminal_feedback import TerminalLedger
from test_equipment_report_prerequisites import router
from test_terminal_feedback import settings, enable
from test_prnsrv_local import install_fake_prnsrv
from test_task_notifier import FakeS3, Response

SSCC = '046070517921585754'


QUANTITIES = [{}, {'Quantity': None}, {'Quantity': ''}, {'Quantity': '  '}]


@pytest.mark.parametrize('fields', QUANTITIES)
def test_source_to_json_to_max_unlimited_with_one_durable_sscc(
        router, settings, tmp_path, monkeypatch, fields):
    tasks, task, _ = router
    runtime = install_fake_prnsrv(monkeypatch)
    monkeypatch.chdir(tmp_path)
    source = {'Article': 'ART', 'Gtin': '00000000000000',
              'PasportData': {'Batch_number': 'LOT', 'Product_PackQty': '4'},
              'numBoxesInPallet': 20, **fields}
    key = 'Задания/00000000-0000-4000-8000-000000000001.json'
    data = {'bucket': TASK_BUCKET, 'key': key}
    tasks.config.update({'input_bucket': TASK_BUCKET,
                         'production_orders_path': 's3://internal-bucket/productionOrders',
                         'equipment-tasks': f's3://{TASK_BUCKET}/equipment-tasks',
                         'equipment-reports': 's3://internal-bucket/equipment-reports',
                         'allowed_gtins': [source['Gtin']], 's3_config': {},
                         'prnsrv_local_generator_enabled': True,
                         'operation_state_path': 's3://isolated/state',
                         'sscc_auth_mode': 'mtls', 'sscc_prefix': '460705179',
                         'sscc_service_url': 'https://synthetic.invalid', 'sscc_extension': '0'})
    monkeypatch.setattr(tasks, 'INPUT_BUCKET', TASK_BUCKET)
    enable(tasks, settings, tmp_path)
    storage = SharedStorage(tmp_path)
    storage.write_text(f's3://{TASK_BUCKET}/{key}', json.dumps(source))
    monkeypatch.setattr(flow, 'load_config', lambda *_: tasks.config)
    monkeypatch.setattr(flow, 'get_storage', lambda *_: storage)
    monkeypatch.setattr(operation_state, 'get_storage', lambda *_: storage)
    monkeypatch.setattr('xtrek.storage.get_storage', lambda *_: storage)
    allocations = {}

    def allocate(_url, _prefix, count, _extension, **kwargs):
        assert count == 1 and kwargs['auth_mode'] == 'mtls'
        request_id = kwargs['request_id']
        assert len(request_id) == 64
        return allocations.setdefault(request_id, [SSCC])

    allocator = MagicMock(side_effect=allocate)
    monkeypatch.setattr(flow, 'get_sscc_from_service', allocator)
    monkeypatch.setattr(flow, 'get_inn_by_gtin', lambda *_args, **_kwargs: 'test-inn')
    monkeypatch.setattr(flow, 'OrganizationManager', MagicMock())
    monkeypatch.setattr(flow, 'TokenProcessor', MagicMock())
    monkeypatch.setattr(flow, 'NK', MagicMock())
    monkeypatch.setattr(flow, 'get_product_info_robust', lambda *_: {'result': [{'is_set': False}]})
    monkeypatch.setattr(runtime, '_load_source', lambda *_: source)
    forbidden = MagicMock(side_effect=AssertionError('Forbidden business/printing boundary'))
    for name in ('create_emission_task',
                 'create_utilisation_task_from_report', 'sign_and_send_utilisation',
                 'sign_and_send_emission', 'sign_and_send_introduce', 'sign_and_send_aggregation'):
        monkeypatch.setattr(flow, name, forbidden)
    for name in ('create_emission_task', 'create_utilisation_task_from_report',
                 'sign_and_send_emission', 'sign_and_send_utilisation', 'generate_prn_files',
                 'sign_and_send_introduce', 'sign_and_send_aggregation'):
        monkeypatch.setattr(tasks, name, forbidden)
    monkeypatch.setattr(runtime, 'LocalPrnsrvAllocator', forbidden)
    monkeypatch.setattr(runtime, 'LocalPrnsrvFunction', forbidden)
    monkeypatch.setattr('requests.sessions.Session.request', forbidden)

    assert task.apply(args=[data]).successful()
    order_files = list(storage.path(tasks.config['production_orders_path']).glob('*.json'))
    equipment_files = list(storage.path(tasks.config['equipment-tasks']).glob('*.json'))
    assert len(order_files) == len(equipment_files) == 1
    order, phone_task = json.loads(order_files[0].read_text()), json.loads(equipment_files[0].read_text())
    assert ('Quantity' in order) == ('Quantity' in fields)
    assert order.get('Quantity') == fields.get('Quantity')
    # Same released v1 schema as the formerly working blank-Quantity task.
    assert phone_task['reportSchemaVersion'] == 1
    assert phone_task['plannedPalletCount'] == 1
    assert phone_task['palletSsccReserve'] == 0
    assert phone_task['palletNumbers'] == [SSCC]
    assert not {'quantityMode', 'notificationOnly', 'numBoxesInPallet'} & phone_task.keys()
    assert not TerminalLedger(settings['ledger_path']).pending()
    assert flow.create_equipment_aggregation_task(phone_task['id']) == phone_task['id']
    assert task.apply(args=[data]).successful()
    allocator.assert_called_once()
    assert len(allocations) == 1
    artifact_paths = [p.relative_to(tmp_path).as_posix() for p in (tmp_path / 'objects').rglob('*') if p.is_file()]
    assert all(not path.endswith(('.csv', '.vdf', '.prn')) for path in artifact_paths)
    assert not any('/emissionOrders/' in path or '/utilisationTasks/' in path for path in artifact_paths)

    # Feed the generated contract into the actual JSON-link notifier. It never
    # invokes the scanned-report validator, even though there are no readyBox codes.
    monkeypatch.setattr(utils, '_validate_equipment_report_input', forbidden)
    s3 = FakeS3()
    s3.get_object = lambda **_: {'ETag': '"etag-1"', 'VersionId': 'version-1',
        'Body': io.BytesIO(json.dumps(phone_task).encode())}
    task_key = 'equipment-tasks/' + phone_task['id'] + '.json'
    url = f'https://storage.yandexcloud.net/{TASK_BUCKET}/{task_key}?signature=fixture'
    s3.generate_presigned_url = lambda *_a, **_kw: url
    history = []

    def get(url_arg, **kwargs):
        if url_arg == url:
            return Response(206)
        if url_arg.endswith('/me'):
            return Response(data={'user_id': 'fixture-bot'})
        if url_arg.endswith('/messages'):
            return Response(data={'messages': history})
        raise AssertionError('Unexpected URL')

    def post(url_arg, **kwargs):
        assert url_arg.endswith('/messages')
        history.append({'timestamp': 1800000000000,
            'sender': {'is_bot': True, 'user_id': 'fixture-bot'},
            'recipient': {'chat_id': 'fixture-chat'},
            'body': {'mid': 'fixture-mid', 'text': phone_task['id'] + ' ' + url}})
        return Response(data={'message': {'body': {'mid': 'fixture-mid'}}})

    post_mock = MagicMock(side_effect=post)
    monkeypatch.setattr('xtrek.task_notifier.requests.get', get)
    monkeypatch.setattr('xtrek.task_notifier.requests.post', post_mock)
    notifier = TaskNotifier(s3=s3, settings=NotifierSettings(
        'fixture-token', 'fixture-chat', 'fixture-ca', tmp_path / 'notifier.sqlite3'))
    for _ in range(2):
        assert notifier.process_object(bucket_id=TASK_BUCKET, object_key=task_key).state == 'delivered'
    post_mock.assert_called_once()
    forbidden.assert_not_called()
    assert not TerminalLedger(settings['ledger_path']).pending()


@pytest.mark.parametrize('fields', QUANTITIES)
def test_direct_emission_entrypoint_is_noop_for_unlimited_source(monkeypatch, fields):
    storage = MagicMock()
    storage.exists.return_value = True
    storage.read_text.return_value = json.dumps({'Gtin': '00000000000000', **fields})
    monkeypatch.setattr(flow, 'load_config', lambda *_: {'production_orders_path': 'prod', 'emission_orders_path': 'emit'})
    monkeypatch.setattr(flow, 'get_storage', lambda *_: storage)
    external = MagicMock(side_effect=AssertionError('No business API or artifact'))
    monkeypatch.setattr(flow, 'get_inn_by_gtin', external)
    monkeypatch.setattr(flow, '_publish_json_result', external)
    assert flow.create_emission_task('T-unlimited', 'chemistry', 'scan') == 'T-unlimited'
    external.assert_not_called()
    storage.upload.assert_not_called()


@pytest.mark.parametrize('fields', QUANTITIES)
def test_quantity_semantics_do_not_relax_scanned_report_validation(fields):
    with pytest.raises(utils.EquipmentReportValidationError):
        utils._validate_equipment_report_input({'readyBox': [], **fields})
    with pytest.raises(utils.EquipmentReportValidationError):
        utils._validate_equipment_report_input({'readyBox': [{'productNumbersFull': []}], **fields})
    # The report contains actual scanned data, regardless of source quantity.
    utils._validate_equipment_report_input({'readyBox': [{'productNumbersFull': ['synthetic-code']}], **fields})


@pytest.mark.parametrize('quantity', [0, '0', False, -1, 'invalid', 10, '10'])
def test_explicit_quantity_is_not_reclassified_as_unlimited(quantity):
    assert not contract.unlimited_aggregation_source({'Quantity': quantity})


@pytest.mark.parametrize('fields', QUANTITIES)
@pytest.mark.parametrize('failure', ['after-allocation-before-upload', 'lost-allocator-reply'])
def test_sscc_survives_retry_with_same_request_identity(tmp_path, monkeypatch, fields, failure):
    monkeypatch.chdir(tmp_path)
    storage = SharedStorage(tmp_path)
    config = {'production_orders_path': 's3://internal/productionOrders',
              'equipment-tasks': 's3://input/equipment-tasks',
              'equipment-reports': 's3://internal/equipment-reports',
              'operation_state_path': 's3://isolated/state',
              'sscc_auth_mode': 'mtls', 'sscc_service_url': 'https://synthetic.invalid',
              'sscc_prefix': '460705179', 'sscc_extension': '0'}
    source = {'Gtin': '00000000000000', 'PasportData': {}, 'numBoxesInPallet': 20, **fields}
    storage.write_text(config['production_orders_path'] + '/T-unlimited.json', json.dumps(source))
    monkeypatch.setattr(flow, 'load_config', lambda *_: config)
    monkeypatch.setattr(flow, 'get_storage', lambda *_: storage)
    monkeypatch.setattr(operation_state, 'get_storage', lambda *_: storage)
    issued, calls = {}, []

    def allocate(_url, _prefix, count, _extension, **kwargs):
        assert count == 1
        request_id = kwargs['request_id']
        calls.append(request_id)
        codes = issued.setdefault(request_id, [SSCC])
        if failure == 'lost-allocator-reply' and len(calls) == 1:
            raise TimeoutError('Reply lost after durable service allocation')
        return codes

    monkeypatch.setattr(flow, 'get_sscc_from_service', allocate)
    publish = flow._publish_json_result
    publish_calls = []

    def publish_with_crash(*args, **kwargs):
        publish_calls.append(True)
        if failure == 'after-allocation-before-upload' and len(publish_calls) == 1:
            raise OSError('Publication failed after SSCC was saved')
        return publish(*args, **kwargs)

    monkeypatch.setattr(flow, '_publish_json_result', publish_with_crash)
    assert flow.create_equipment_aggregation_task('T-unlimited') is None
    # Reopen storage to simulate a new worker with no in-memory state.
    reopened = SharedStorage(tmp_path)
    monkeypatch.setattr(flow, 'get_storage', lambda *_: reopened)
    monkeypatch.setattr(operation_state, 'get_storage', lambda *_: reopened)
    for _ in range(2):
        assert flow.create_equipment_aggregation_task('T-unlimited') == 'T-unlimited'
    assert len(issued) == 1
    assert len(calls) == (1 if failure == 'after-allocation-before-upload' else 2)
    assert len(set(calls)) == 1  # Safe mTLS retry gets the same issued SSCC, never a new one.
    task = json.loads(reopened.read_text(config['equipment-tasks'] + '/T-unlimited.json'))
    assert task['palletNumbers'] == [SSCC]
