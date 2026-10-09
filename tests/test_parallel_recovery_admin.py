import json
import time
from unittest.mock import MagicMock

import pytest

from parallel_flow_store import SharedStorage
from xtrek import create_emission_task_sample as flow, operation_state
from xtrek.operation_admin import prove_dead, recover
from xtrek.operation_state import OperationConflict, OperationState, ReconciliationRequired


def dead_operation(tmp_path, phase):
    storage = SharedStorage(tmp_path)
    guard = OperationState(storage, 's3://test/state', 'synthetic', 'JOB', 'hash')
    guard.acquire()
    value = dict(guard.value, phase=phase, owner=dict(guard.owner, pid=2147483647))
    storage.write_lock_object(guard.path, json.dumps(value), guard.etag)
    return storage, guard, prove_dead(value['owner'])


@pytest.mark.parametrize('phase', ['held', 'external', 'uncertain'])
def test_fresh_death_proof_recovers_without_business_request(tmp_path, phase):
    storage, original, proof = dead_operation(tmp_path, phase)
    result = None if phase == 'held' else {'orderId': 'VERIFIED'}
    assert proof['dead']
    assert recover(storage, original.path, proof, result)['business_api_calls'] == 0
    replacement = OperationState(storage, 's3://test/state', 'synthetic', 'JOB', 'hash')
    assert replacement.acquire() == result
    assert replacement.cached == (result is not None)


@pytest.mark.parametrize('change', ['stale', 'foreign_host', 'wrong_owner', 'alive'])
def test_invalid_proof_cannot_release_an_operation(tmp_path, change):
    storage, original, proof = dead_operation(tmp_path, 'external')
    if change == 'stale':
        proof['observed_at'] = time.time() - 121
    elif change == 'foreign_host':
        proof['observer']['host'] = 'other-host'
    elif change == 'wrong_owner':
        proof['owner'] = dict(proof['owner'], pid=123)
    else:
        proof['dead'] = False
    before = storage.read_lock_object(original.path)
    with pytest.raises(OperationConflict):
        recover(storage, original.path, proof, {'orderId': 'VERIFIED'})
    assert storage.read_lock_object(original.path) == before


@pytest.fixture
def allocation(tmp_path, monkeypatch):
    config = {'operation_state_path': 's3://test/state', 'production_orders_path': 's3://test/production',
              'sscc_service_url': 'https://synthetic.invalid', 'sscc_prefix': '460705179', 'sscc_extension': '0'}
    storage = SharedStorage(tmp_path)
    data = {'Quantity': '1'}
    storage.write_text('s3://test/production/JOB.json', json.dumps(data))
    monkeypatch.setattr(flow, 'load_config', lambda _: config)
    monkeypatch.setattr(flow, 'get_storage', lambda *a: storage)
    monkeypatch.setattr(operation_state, 'get_storage', lambda *a: storage)
    api = MagicMock(return_value=['046070517921585754'])
    monkeypatch.setattr(flow, 'get_sscc_from_service', api)
    return config, storage, data, api


def test_saved_sscc_assignment_survives_following_output_failure(allocation):
    config, storage, data, api = allocation
    first = flow._allocate_task_pallets('JOB', data, config)
    # A later failure in equipment output does not discard the allocation.
    for _ in range(29):
        assert flow._allocate_task_pallets('JOB', data, config) == first
    api.assert_called_once()


def test_lost_sscc_response_never_allocates_replacement_numbers(allocation):
    config, storage, data, api = allocation
    api.side_effect = TimeoutError('allocation accepted before response was lost')
    with pytest.raises(TimeoutError):
        flow._allocate_task_pallets('JOB', data, config)
    with pytest.raises(ReconciliationRequired):
        flow._allocate_task_pallets('JOB', data, config)
    api.assert_called_once()


def test_equipment_route_does_not_hide_ambiguous_sscc_operation(allocation):
    config, storage, data, api = allocation
    config.update({'equipment-tasks': 's3://test/equipment', 'equipment-reports': 's3://test/reports'})
    api.side_effect = TimeoutError('allocation accepted before response was lost')
    assert flow.create_equipment_aggregation_task('JOB') is None
    with pytest.raises(ReconciliationRequired):
        flow.create_equipment_aggregation_task('JOB')
    api.assert_called_once()
