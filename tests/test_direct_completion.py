"""Completion is durable only after a confirmed business document and tag write."""
import json
from unittest.mock import MagicMock

import pytest
from test_tasks_unit_set_flow import import_tasks
from xtrek.storage import LocalStorage


@pytest.mark.parametrize('kind', ['UNIT', 'SET'])
@pytest.mark.parametrize('as_list', [False, True])
def test_completion_retries_tag_failure_without_resubmission(monkeypatch, tmp_path, kind, as_list):
    tasks = import_tasks(monkeypatch)
    pid = 'T-direct-test'
    path = tmp_path / (pid + '.json')
    path.write_text(json.dumps({'directMode': True, 'GtinType': kind}))
    tasks.config['production_orders_path'] = str(tmp_path)
    local = LocalStorage()
    local.set_tags(str(path), {'existing': 'preserved'})
    store = MagicMock(wraps=local)
    store.set_tags.side_effect = [OSError('temporary tag failure'), None]
    monkeypatch.setattr(tasks, 'get_storage', lambda *_: store)
    monkeypatch.setattr(tasks, 'direct_context', lambda *_: (pid, {'GtinType': kind}))
    monkeypatch.setattr(tasks, 'get_production_order_data', lambda _: {'directMode': True, 'GtinType': kind})
    response = {'status': 'CHECKED_OK', 'productionOrderId': pid}
    response = [response] if as_list else response
    name = 'update_introduce_status' if kind == 'UNIT' else 'update_aggregation_set_status'
    monkeypatch.setattr(tasks, name, lambda *_: response)
    forbidden = MagicMock(side_effect=AssertionError('Completion must not submit documents'))
    for name in ['sign_and_send_introduce', 'sign_and_send_aggregation_set', 'sign_and_send_aggregation']:
        monkeypatch.setattr(tasks, name, forbidden)
    route = tasks.logic_update_introduce if kind == 'UNIT' else tasks.logic_update_agg_set
    key = 'receipts/' + ('order' if kind == 'UNIT' else pid) + '.json'
    with pytest.raises(OSError):
        route(key)
    assert local.get_tags(str(path)) == {'existing': 'preserved'}
    store.set_tags.side_effect = local.set_tags
    assert 'completed' in route(key)
    assert 'completed' in route(key)
    assert local.get_tags(str(path)) == {'existing': 'preserved', 'check': 'finished'}
    forbidden.assert_not_called()


@pytest.mark.parametrize('status', ['IN_PROGRESS', 'CHECKED_NOT_OK'])
@pytest.mark.parametrize('kind', ['UNIT', 'SET'])
def test_unconfirmed_document_never_marks_finished(monkeypatch, kind, status):
    tasks = import_tasks(monkeypatch)
    store = MagicMock()
    monkeypatch.setattr(tasks, 'get_storage', lambda *_: store)
    name = 'update_introduce_status' if kind == 'UNIT' else 'update_aggregation_set_status'
    monkeypatch.setattr(tasks, name, lambda *_: {'status': status})
    route = tasks.logic_update_introduce if kind == 'UNIT' else tasks.logic_update_agg_set
    with pytest.raises(Exception):
        route('receipts/T-direct-test.json')
    store.set_tags.assert_not_called()
