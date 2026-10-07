import json
from contextlib import contextmanager
from dataclasses import MISSING, fields
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from xtrek import create_emission_task_sample as flow
from xtrek.suz_api_models import PasportData
from xtrek.utilisation_attributes import build_utilisation_attributes


DEFAULTS = {'product_group_settings': {'chemistry': {'alcoholVolume': '0'}}}
CODE = '010463004077589521test123\x1d93test'


@pytest.mark.parametrize('passport,config,expected', [
    ({}, DEFAULTS, '0'),
    ({'alcoholVolume': None}, DEFAULTS, '0'),
    ({'alcoholVolume': '4.5'}, DEFAULTS, '4.5'),
    ({'alcoholVolume': 0}, {'product_group_settings': {'chemistry': {'alcoholVolume': '4'}}}, '0'),
    ({'alcoholVolume': '0'}, {}, '0'),
    ({}, {'product_group_settings': {'chemistry': {'alcoholVolume': '2.5'}}}, '2.5'),
    ({}, {'product_group_settings': {'chemistry': {'alcoholVolume': 0}}}, '0'),
    ({'alcoholVolume': 4.5}, DEFAULTS, '4.5'),
    ({'alcoholVolume': '99.9'}, DEFAULTS, '99.9'),
])
def test_passport_takes_precedence_over_config(passport, config, expected):
    result = build_utilisation_attributes('chemistry', passport, config, '2026-10-01', '2028-10-01')
    assert result == {'productionDate': '2026-10-01', 'expirationDate': '2028-10-01',
                      'alcoholVolume': expected}


@pytest.mark.parametrize('value', ['', '4,5', '-1', '100', '4.55', '0\n', True, [], float('nan')])
@pytest.mark.parametrize('source', ['passport', 'config'])
def test_invalid_values_are_not_replaced_with_default(value, source):
    passport = {'alcoholVolume': value} if source == 'passport' else {}
    config = DEFAULTS if source == 'passport' else {'product_group_settings': {'chemistry': {'alcoholVolume': value}}}
    with pytest.raises(ValueError, match='alcoholVolume'):
        build_utilisation_attributes('chemistry', passport, config)


@pytest.mark.parametrize('config', [{}, {'product_group_settings': {}},
    {'product_group_settings': {'chemistry': {'alcoholVolume': None}}}])
def test_no_hidden_hardcoded_default(config):
    with pytest.raises(ValueError, match='Missing.*default'):
        build_utilisation_attributes('chemistry', {}, config)


def test_group_scope_and_splitmark_exception():
    assert build_utilisation_attributes('milk', {}, DEFAULTS, '2026-10-01') == {'productionDate': '2026-10-01'}
    assert build_utilisation_attributes('chemistry', {'alcoholVolume': '4'}, DEFAULTS,
                                        '2026-10-01', '2028-10-01', utilisation_type='SPLITMARK') == {}
    with pytest.raises(ValueError, match='trailing space'):
        build_utilisation_attributes('chemistry', {'alcoholVolume ': '4'}, DEFAULTS)


def test_passport_optional_field_remains_absent_in_old_payloads():
    required = {f.name: '' for f in fields(PasportData) if f.default is MISSING}
    assert PasportData(**required).alcoholVolume is None
    assert 'alcoholVolume' not in PasportData(**required).to_dict()
    assert PasportData(**required, alcoholVolume='4.5').to_dict()['alcoholVolume'] == '4.5'


class MemoryStorage:
    def __init__(self):
        self.objects = {}
        self.uploads = []
    def exists(self, path):
        return path in self.objects
    def read_text(self, path):
        return self.objects[path]
    def upload(self, local, remote):
        self.objects[remote] = Path(local).read_text()
        self.uploads.append(remote)
    def mark_processing(self, path):
        pass
    def mark_finished(self, path):
        pass
    def mark_error(self, path):
        pass
    def put(self, path, body):
        self.objects[path] = json.dumps(body)


@pytest.fixture
def context(monkeypatch, tmp_path):
    config = dict(DEFAULTS, production_orders_path='mem/prod', kodes='mem/codes',
                  utilisation_tasks_path='mem/util', utilisation_receipts='mem/receipts',
                  **{'equipment-tasks': 'mem/equipment', 'equipment-reports': 'mem/reports'})
    storage = MemoryStorage()
    storage.put('mem/prod/po.json', {'virtual': True, 'Gtin': '04630040775895',
        'PasportData': {'Batch_date_production': '01.10.2026', 'Batch_date_expired': '01.10.2028',
                        'Manufacturer_inn': '7701234567'}})
    storage.put('mem/codes/order.json', {'codes': [CODE]})
    storage.put('mem/equipment/po.json', {'task-export-signed-link': 'https://example.test/report.json'})
    storage.put('mem/reports/report.json', {'readyBox': [{'boxNumber': '00046012345678901234',
                                                       'productNumbersFull': [CODE]}]})
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(flow, 'load_config', lambda *_: config)
    monkeypatch.setattr(flow, 'get_storage', lambda *_: storage)
    monkeypatch.setattr(flow, '_find_production_order_id_by_suz_order_id', lambda _: 'po')
    return config, storage


def generate(kind):
    if kind == 'equipment':
        return flow.create_utilisation_task_from_report('po', 'chemistry'), 'mem/util/po.json'
    if kind == 'virtual':
        return flow.create_virtual_utilisation_task('order', 'chemistry'), 'mem/util/order.json'
    return flow.create_utilisation_task('order', 'chemistry', '2026-09-30', '2028-09-30'), 'mem/util/order.json'


@pytest.mark.parametrize('kind', ['equipment', 'codes', 'virtual'])
@pytest.mark.parametrize('value', [None, '4.5', 0])
def test_actual_generators_use_passport_even_with_explicit_dates(context, kind, value):
    _, storage = context
    if value is not None:
        order = json.loads(storage.objects['mem/prod/po.json'])
        order['PasportData']['alcoholVolume'] = value
        storage.put('mem/prod/po.json', order)
    result, path = generate(kind)
    assert result is not None
    body = json.loads(storage.objects[path])
    assert body['attributes']['alcoholVolume'] == ('0' if value is None else str(value))
    assert body['attributes']['productionDate'] == ('2026-09-30' if kind == 'codes' else '2026-10-01')
    assert body['sntins'] == [CODE]
    assert body['productionOrderId'] == 'po'
    if kind == 'virtual':
        assert result.PasportData.alcoholVolume == value


@pytest.mark.parametrize('kind', ['equipment', 'codes', 'virtual'])
def test_existing_utilisation_tasks_are_never_rewritten(context, kind):
    config, storage = context
    path = 'mem/util/po.json' if kind == 'equipment' else 'mem/util/order.json'
    old = '{ "productGroup": "chemistry", "attributes": {}, "sntins": [] }'
    storage.objects[path] = old
    config.pop('product_group_settings')
    result, _ = generate(kind)
    assert result is not None
    assert storage.objects[path] == old
    assert not storage.uploads


@pytest.mark.parametrize('kind', ['equipment', 'codes', 'virtual'])
def test_invalid_passport_stops_creation_without_storage_writes(context, kind, caplog):
    _, storage = context
    order = json.loads(storage.objects['mem/prod/po.json'])
    order['PasportData']['alcoholVolume'] = '4,5'
    storage.put('mem/prod/po.json', order)
    result, path = generate(kind)
    assert result is None
    assert path not in storage.objects
    assert not storage.uploads
    assert 'alcoholVolume' in caplog.text


@pytest.mark.parametrize('legacy', [False, True])
def test_sender_signs_exact_saved_attributes_without_backfill(context, monkeypatch, tmp_path, legacy):
    _, storage = context
    if legacy:
        storage.put('mem/util/order.json', {'productGroup': 'chemistry', 'productionOrderId': 'po',
                                          'sntins': [CODE], 'attributes': {}})
    else:
        assert flow.create_utilisation_task('order', 'chemistry') == 'order'
    before = storage.objects['mem/util/order.json']
    signed_bytes = []

    @contextmanager
    def prepare(inn, body, filename, *_):
        signed_bytes.append(body)
        body_path, signature_path = tmp_path / filename, tmp_path / 'signature'
        body_path.write_bytes(body)
        signature_path.write_text('test-signature')
        yield SimpleNamespace(body_path=body_path, signature_path=signature_path)

    def send(body_file, signature_file, **kwargs):
        assert Path(body_file).read_bytes() == signed_bytes[0]
        assert Path(signature_file).read_text() == 'test-signature'
        return 'test-report-id'

    monkeypatch.setattr(flow, 'OrganizationManager', MagicMock())
    monkeypatch.setattr(flow, 'TokenProcessor', lambda **_: SimpleNamespace(get_token_value_for=lambda *a, **k: 'test-token'))
    monkeypatch.setattr(flow, 'SUZ', lambda **_: SimpleNamespace(utilisation_send=send))
    result = flow.sign_and_send_utilisation('order', str(tmp_path), 1, 'test-oms', 'test-connection',
                                            document_signer=SimpleNamespace(prepare=prepare))
    assert result == 'test-report-id'
    outgoing = json.loads(signed_bytes[0])
    assert outgoing['attributes'] == json.loads(before)['attributes']
    assert ('alcoholVolume' in outgoing['attributes']) is not legacy
    assert 'productionOrderId' not in outgoing
    assert storage.objects['mem/util/order.json'] == before
