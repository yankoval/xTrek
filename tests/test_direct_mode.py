from datetime import date
import pytest
from xtrek.direct_mode import classify_input, validate_codes, validate_direct_task

ID = '58835a34-2c28-4397-bb61-8151f9ac1723'
CODE = '010467040450000021ABC\x1d93XYZ'

def task():
    return {'Gtin': CODE[2:16], 'PasportData': {'Manufacturer_inn': '7733154124',
            'Batch_date_production': '2026-09-01', 'Batch_date_expired': '2029-09-01'},
            'directMode': True, 'emissionDocumentId': ID}

def test_inputs():
    assert classify_input(task()) == 'task'
    assert classify_input({'codes': [CODE]}) == 'codes'
    assert validate_direct_task(task(), date(2026, 10, 15))

@pytest.mark.parametrize('value', [{}, [], {'codes': []}, {'codes': [CODE], **task()},
                                  {**task(), 'directMode': 'true'}])
def test_reject_unknown_or_ambiguous(value):
    with pytest.raises(ValueError): classify_input(value)

@pytest.mark.parametrize('codes', [[CODE, CODE], [CODE.split('\x1d')[0]], [CODE]*30001])
def test_invalid_blocks(codes):
    with pytest.raises(ValueError): validate_codes({'codes': codes})

@pytest.mark.parametrize('field,value', [('Batch_date_production', '2027-01-01'),
    ('Batch_date_production', '2026-02-30'), ('Batch_date_expired', '2026-08-01'),
    ('Batch_date_production', ''), ('Batch_date_production', '01.09.2026')])
def test_invalid_dates(field, value):
    value_task = task(); value_task['PasportData'][field] = value
    with pytest.raises(ValueError): validate_direct_task(value_task, date(2026, 10, 15))

@pytest.fixture
def binding(tmp_path):
    import json
    from xtrek.direct_mode import publish_direct_codes
    config = {'kodes': str(tmp_path / 'kodes'), 'production_orders_path': str(tmp_path / 'productionOrders')}
    production_id = 'T-direct-123'
    value = dict(task(), Quantity=1)
    from xtrek.operation_state import publish_once
    from xtrek.storage import get_storage
    path = config['production_orders_path'] + '/' + production_id + '.json'
    publish_once(get_storage(path), path, json.dumps(value))
    block = {'codes': [CODE], 'orderId': ID, 'blockId': '34f0bd3d-bb76-4d79-97f6-2d459aeadf03'}
    publish_direct_codes(config, production_id, value, block)
    return config, production_id, value, block


def test_idempotent_binding_and_no_overwrite(binding):
    from xtrek.direct_mode import publish_direct_codes, direct_context
    from xtrek.operation_state import OperationConflict
    config, production_id, value, block = binding
    assert publish_direct_codes(config, production_id, value, block) == ID
    assert direct_context(config, ID) == (production_id, value)
    for owner, payload in [('another-task', block), (production_id, dict(block, codes=[CODE.replace('ABC','ABD')]))]:
        with pytest.raises(OperationConflict): publish_direct_codes(config, owner, value, payload)


def test_changed_passport_detected_before_submission(binding):
    import json
    from pathlib import Path
    from xtrek.direct_mode import direct_context
    from xtrek.operation_state import OperationConflict
    config, production_id, value, block = binding
    value['PasportData']['Batch_date_production'] = '2026-09-02'
    Path(config['production_orders_path'], production_id + '.json').write_text(json.dumps(value))
    with pytest.raises(OperationConflict): direct_context(config, ID)


@pytest.mark.parametrize('change', [dict(orderId='34f0bd3d-bb76-4d79-97f6-2d459aeadf03'),
    dict(blockCount=2), dict(blocks=[]), dict(codes=[CODE, CODE.replace('ABC','ABD')])])
def test_pair_rejects_partial_multi_or_other_order(change):
    from xtrek.direct_mode import validate_pair
    with pytest.raises(ValueError):
        validate_pair(dict(task(), Quantity=1), dict({'codes':[CODE], 'orderId':ID}, **change))


def test_document_must_match_binding(binding):
    from xtrek.direct_mode import validate_direct_document
    from xtrek.operation_state import OperationConflict
    config, production_id, value, _ = binding
    doc = {'productionOrderId': production_id, 'productGroup':'chemistry', 'sntins':[CODE],
           'attributes': {'productionDate':'2026-09-01','expirationDate':'2029-09-01'}}
    assert validate_direct_document(config, ID, doc, 'utilisation')
    doc['attributes']['productionDate'] = '2026-09-02'
    with pytest.raises(OperationConflict): validate_direct_document(config, ID, doc, 'utilisation')


def test_test_contour_routes_in_single_bucket():
    from xtrek.s3_poller import configured_routes
    routes = configured_routes('test-bucket', 'test-bucket', 'direct-test/')
    assert len(routes) == 1
    assert 'direct-test/Задания/' in routes['test-bucket']
    assert 'direct-test/kodes/' in routes['test-bucket']
    assert all(p.startswith('direct-test/') for p in routes['test-bucket'])


def test_common_commands_resolve_order_id_without_new_emission(binding, monkeypatch, tmp_path):
    import json
    from pathlib import Path
    from unittest.mock import MagicMock
    from xtrek import create_emission_task_sample as flow
    config, production_id, value, block = binding
    config.update({'utilisation_tasks_path':str(tmp_path/'utilisationTasks'),
                   'introduce-tasks':str(tmp_path/'introduceTasks'),
                   'product_group_settings':{'chemistry':{'alcoholVolume':'0'}}})
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(flow, 'load_config', lambda *_:config)
    assert flow._find_production_order_id_by_suz_order_id(ID) == production_id
    assert flow.create_utilisation_task(ID, 'chemistry') == ID
    report = json.loads(Path(config['utilisation_tasks_path'], ID+'.json').read_text())
    assert report['attributes'] == {'productionDate':'2026-09-01','expirationDate':'2029-09-01','alcoholVolume':'0'}
    monkeypatch.setattr(flow, 'get_inn_by_gtin', lambda *a,**k:'7733154124')
    monkeypatch.setattr(flow, 'OrganizationManager', MagicMock())
    monkeypatch.setattr(flow, 'TokenProcessor', MagicMock())
    nk=MagicMock()
    monkeypatch.setattr(flow, 'NK', lambda **_:nk)
    monkeypatch.setattr(flow, 'get_product_info_robust', lambda *a, **k: {'result':[{'good_attrs':[
        {'attr_id':13933,'attr_value':'3305900009'}]}]})
    nk.get_active_permit_documents_by_gtin.return_value = [{'number':'TEST','date':'2026-01-01','type':'CONFORMITY_DECLARATION'}]
    assert flow.create_introduce_task(ID, 'chemistry') == ID
    intro=json.loads(Path(config['introduce-tasks'],ID+'.json').read_text())
    assert intro['productionOrderId'] == production_id
    assert intro['production_date'] == '2026-09-01'
    assert intro['products'][0]['uit_code'] == CODE.split('\x1d')[0]
    assert not (tmp_path/'emissions').exists()

    # Exercise the shared sender with a fake signer and API, never real credentials.
    from contextlib import contextmanager
    from types import SimpleNamespace
    import base64
    config['introduce-receipts'] = str(tmp_path/'introduceReceipts')
    api = MagicMock(); api.documents_create.return_value = 'test-document-id'
    monkeypatch.setattr(flow, 'HonestSignAPI', lambda **_:api)
    @contextmanager
    def prepare(inn, body, filename, *args):
        path=tmp_path/filename; path.write_bytes(body)
        yield SimpleNamespace(body_path=path, signature='TEST-SIGNATURE')
    signer = SimpleNamespace(prepare=prepare)
    result = flow.sign_and_send_introduce(ID, 'chemistry', str(tmp_path), 1, document_signer=signer)
    assert result['document_id'] == 'test-document-id'
    wrapper = json.loads(api.documents_create.call_args.args[0])
    sent = json.loads(base64.b64decode(wrapper['product_document']))
    assert 'productionOrderId' not in sent
    assert sent['production_date'] == value['PasportData']['Batch_date_production']


@pytest.mark.parametrize('suffix', ['.json', '.processing', '.finished', '.ACTIVE'])
def test_local_emission_lookup_preserves_status_files_and_ignores_sidecars(monkeypatch, tmp_path, suffix):
    import json
    from xtrek import create_emission_task_sample as flow
    (tmp_path / (ID + '.json.tags')).write_text('{"productionOrderId":"wrong"}')
    (tmp_path / (ID + '.json.cas')).touch()
    (tmp_path / (ID + suffix)).write_text(json.dumps({'productionOrderId': 'legacy-task'}))
    monkeypatch.setattr(flow, 'load_config', lambda *_: {'emissions_path': str(tmp_path)})
    assert flow._find_production_order_id_by_suz_order_id(ID) == 'legacy-task'
