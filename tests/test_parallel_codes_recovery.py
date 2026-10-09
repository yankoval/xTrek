import json
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from xtrek import create_emission_task_sample as flow, operation_state
from xtrek.operation_state import ReconciliationRequired
from parallel_flow_store import SharedStorage


@pytest.fixture
def codes_case(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    config = {'operation_state_path': 's3://isolated/state', 'emissions_path': 's3://test/emissions',
              'kodes': 's3://test/kodes'}
    storage = SharedStorage(tmp_path)
    source = 's3://test/emissions/ORDER.json'
    storage.write_text(source, json.dumps({'gtin': '04600000000000', 'omsId': 'OMS', 'productionOrderId': 'PROD'}))
    monkeypatch.setattr(flow, 'load_config', lambda _: config)
    monkeypatch.setattr(flow, 'get_storage', lambda *args: storage)
    monkeypatch.setattr(operation_state, 'get_storage', lambda *args: storage)
    organization = SimpleNamespace(inn='7701234567', oms_id='OMS', connection_id='CON')
    monkeypatch.setattr(flow, 'OrganizationManager', lambda *args: SimpleNamespace(list=lambda: [organization]))
    monkeypatch.setattr(flow, 'TokenProcessor', lambda **kwargs: SimpleNamespace(get_token_value_for=lambda *a, **kw: 'synthetic'))
    api = MagicMock()
    api.order_status.return_value = [{'bufferStatus': 'ACTIVE', 'availableCodes': 2, 'totalCodes': 2}]
    result = {'orderId': 'ORDER', 'codes': ['01GTIN21ONE\x1d93TEST', '01GTIN21TWO\x1d93TEST']}
    api.codes.return_value = result
    monkeypatch.setattr(flow, 'SUZ', lambda **kwargs: api)
    return storage, api, result


def test_codes_duplicate_uses_exact_saved_result_without_second_consuming_get(codes_case):
    storage, api, result = codes_case
    assert flow.get_emission_kodes('ORDER') == result
    for _ in range(29):
        assert flow.get_emission_kodes('ORDER') == result
    api.codes.assert_called_once()
    assert json.loads(storage.read_text('s3://test/kodes/ORDER.json')) == result


def test_codes_restore_confirmed_response_after_output_storage_failure(codes_case):
    storage, api, result = codes_case
    write = storage.write_lock_object
    def fail_output(path, *args):
        if '/kodes/' in path:
            raise OSError('output bucket unavailable')
        return write(path, *args)
    storage.write_lock_object = fail_output
    with pytest.raises(OSError):
        flow.get_emission_kodes('ORDER')
    storage.write_lock_object = write
    assert flow.get_emission_kodes('ORDER') == result
    api.codes.assert_called_once()
    assert storage.get_tags('s3://test/kodes/ORDER.json')['productionOrderId'] == 'PROD'


def test_lost_consuming_response_blocks_second_get(codes_case):
    storage, api, _ = codes_case
    api.codes.side_effect = TimeoutError('codes were consumed before response was lost')
    with pytest.raises(TimeoutError):
        flow.get_emission_kodes('ORDER')
    with pytest.raises(ReconciliationRequired):
        flow.get_emission_kodes('ORDER')
    api.codes.assert_called_once()
    assert not storage.exists('s3://test/kodes/ORDER.json')


def test_exhausted_order_without_local_codes_replays_existing_blocks_only(codes_case):
    storage, api, result = codes_case
    api.order_status.return_value = [{'bufferStatus': 'EXHAUSTED', 'availableCodes': 0, 'totalCodes': 2}]
    api.order_codes_blocks.return_value = {'orderId': 'ORDER', 'gtin': '04600000000000',
        'blocks': [{'blockId': 'FIRST', 'quantity': 1}, {'blockId': 'SECOND', 'quantity': 1}]}
    api.order_codes_retry.side_effect = [{'codes': [result['codes'][0]]}, {'codes': [result['codes'][1]]}]
    assert flow.get_emission_kodes('ORDER') == result
    api.codes.assert_not_called()
    assert api.order_codes_retry.call_count == 2


@pytest.mark.parametrize('blocks', [None, {}, [], [{'blockId': 'SAME'}, {'blockId': 'SAME'}],
                                  {'orderId': 'OTHER', 'gtin': '04600000000000', 'blocks': [{'blockId': 'ONE'}]}])
def test_unknown_or_invalid_block_response_never_consumes_new_codes(codes_case, blocks):
    storage, api, _ = codes_case
    api.order_status.return_value = [{'bufferStatus': 'EXHAUSTED', 'totalCodes': 2}]
    api.order_codes_blocks.return_value = blocks
    with pytest.raises(ReconciliationRequired):
        flow.get_emission_kodes('ORDER')
    api.codes.assert_not_called()
    assert not storage.exists('s3://test/kodes/ORDER.json')


def test_rejected_order_stops_retries_and_never_fetches_codes(codes_case):
    _, api, _ = codes_case
    api.order_status.return_value = [{'bufferStatus': 'REJECTED', 'totalCodes': 0}]
    assert flow.get_emission_kodes('ORDER')['bufferStatus'] == 'REJECTED'
    assert flow.get_emission_kodes('ORDER')['bufferStatus'] == 'REJECTED'
    api.order_status.assert_called_once()
    api.codes.assert_not_called()
