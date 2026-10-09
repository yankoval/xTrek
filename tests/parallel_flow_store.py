"""Shared synthetic object/API journal for separate-process workflow tests."""
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

from xtrek.storage import LocalStorage
from xtrek.operation_state import OperationConflict


class SharedStorage:
    def __init__(self, root):
        self.root = Path(root)
        self.local = LocalStorage()

    def path(self, path):
        return self.root / 'objects' / str(path).replace('s3://', '')

    def exists(self, path):
        return self.path(path).exists()

    def read_text(self, path):
        return self.path(path).read_text()

    def write_text(self, path, content):
        self.local.write_text(str(self.path(path)), content)

    def read_lock_object(self, path):
        return self.local.read_lock_object(str(self.path(path)))

    def write_lock_object(self, path, content, etag):
        return self.local.write_lock_object(str(self.path(path)), content, etag)

    def upload(self, source, destination):
        self.write_text(destination, Path(source).read_bytes())

    def download(self, source, destination):
        Path(destination).write_bytes(self.path(source).read_bytes())

    def get_tags(self, path):
        value = self.path(path + '.tags')
        return json.loads(value.read_text()) if value.exists() else {}

    def set_tags(self, path, tags):
        self.write_text(path + '.tags', json.dumps(dict(self.get_tags(path), **tags)))
        return path

    def mark_processing(self, path):
        return self.set_tags(path, {'status': 'processing'})

    def mark_finished(self, path):
        return self.set_tags(path, {'status': 'finished'})

    def mark_error(self, path):
        return self.set_tags(path, {'status': 'error'})

    def upload_once(self, source, destination):
        content = Path(source).read_text()
        if self.write_lock_object(destination, content, None) is None:
            if self.read_text(destination) != content:
                raise OperationConflict('Different print file')


class APIJournal:
    def __init__(self, root):
        self.path = str(Path(root) / 'external.db')
        with sqlite3.connect(self.path) as db:
            db.execute('CREATE TABLE IF NOT EXISTS calls (id INTEGER PRIMARY KEY, operation TEXT, key TEXT, payload TEXT, worker INTEGER)')

    def call(self, operation, key, payload):
        import os
        with sqlite3.connect(self.path) as db:
            cursor = db.execute('INSERT INTO calls (operation,key,payload,worker) VALUES (?,?,?,?)',
                                (operation, key, json.dumps(payload), os.getpid()))
            return cursor.lastrowid

    def rows(self, operation=None):
        with sqlite3.connect(self.path) as db:
            if operation:
                return db.execute('SELECT operation,key,payload,worker FROM calls WHERE operation=?', (operation,)).fetchall()
            return db.execute('SELECT operation,key,payload,worker FROM calls').fetchall()


def setup_flow(root, config, storage=None, journal=None):
    # Every real network boundary is prohibited. Only these explicit API models
    # can be called; no production defaults, keys, bot or printers are reachable.
    import requests
    from xtrek import create_emission_task_sample as flow, operation_state, sign, prn_util
    from xtrek.suz_api_models import EmissionOrderreceipts
    storage = storage if storage is not None else SharedStorage(root)
    journal = journal if journal is not None else APIJournal(root)
    def forbidden(*args, **kwargs):
        raise AssertionError('Live HTTP is forbidden in process test')
    requests.sessions.Session.request = forbidden
    for module in [flow, operation_state, prn_util]:
        module.get_storage = lambda *args, **kwargs: storage
    flow.load_config = lambda _: config
    prn_util.load_config = lambda _: config
    flow.get_inn_by_gtin = lambda *args, **kwargs: '7701234567'
    organization = SimpleNamespace(inn='7701234567', oms_id='TEST-OMS', connection_id='TEST-CON', name='synthetic')
    flow.OrganizationManager = lambda *args, **kwargs: SimpleNamespace(list=lambda: [organization], find=lambda **kw: organization)
    flow.TokenProcessor = lambda **kwargs: SimpleNamespace(get_token_value_for=lambda *a, **kw: 'SYNTHETIC', refresh_token_for=lambda *a, **kw: 'SYNTHETIC')
    flow.NK = lambda **kwargs: SimpleNamespace(get_set_by_gtin=lambda gtin: {
        'result': [{'is_set': gtin.endswith('1'), 'set_gtins': [
            {'gtin': '04600000000002', 'quantity': 2},
            {'gtin': '04600000000003', 'quantity': 1}]}]})
    flow.get_product_info_robust = lambda _, gtin: {'result': [{'is_set': gtin.endswith('1'),
                                                             'good_name': 'Synthetic product'}]}
    sign.sign_document = lambda *args, **kwargs: 'c2ln'
    def sscc(url, prefix, quantity, extension, **kwargs):
        row = journal.call('sscc', prefix, {'count': quantity})
        result = []
        for index in range(quantity):
            stem = str(46000000000000000 + row * 100 + index)
            digit = (10 - sum(int(n) * (3 if i % 2 == 0 else 1) for i, n in enumerate(reversed(stem))) % 10) % 10
            result.append(stem + str(digit))
        return result
    flow.get_sscc_from_service = sscc
    class SyntheticSUZ:
        def __init__(self, **kwargs):
            pass
        def order_create(self, body, signature):
            payload = json.loads(Path(body).read_text())
            key = payload['attributes']['productionOrderId']
            journal.call('emission', key, payload)
            return EmissionOrderreceipts('SUZ-' + key, 1, 'TEST-OMS')
        def order_status(self, order_id, gtin):
            key = order_id.removeprefix('SUZ-')
            data = json.loads(storage.read_text(config['emission_orders_path'] + '/' + key + '.json'))
            quantity = data['products'][0]['quantity']
            return [{'orderId': order_id, 'gtin': gtin, 'omsId': 'TEST-OMS',
                     'bufferStatus': 'ACTIVE', 'availableCodes': quantity, 'totalCodes': quantity,
                     'leftInBuffer': quantity, 'unavailableCodes': 0, 'totalPassed': 0,
                     'poolsExhausted': False}]
        def codes(self, order_id, quantity, gtin):
            journal.call('codes', order_id, {'quantity': quantity})
            return {'orderId': order_id, 'codes': ['01' + gtin + '21' + order_id + str(i) + '\x1d93TEST' for i in range(quantity)]}
    flow.SUZ = SyntheticSUZ
    prn_util.generate_amica_vdf = lambda **kw: Path(kw['output_vdf_path']).write_text('synthetic VDF\n' + Path(kw['new_csv_path']).read_text())
    return flow, prn_util, storage, journal


def process_jobs(root, config, keys, barrier):
    import os
    import time
    from xtrek.operation_state import OperationBusy
    flow, print_flow, storage, journal = setup_flow(root, config)
    directory = Path(root) / ('worker-' + str(os.getpid()))
    directory.mkdir()
    os.chdir(directory)
    barrier.wait(timeout=20)
    for key in keys:
        for _ in range(200):
            try:
                production = flow.process_incoming_task('input/Задания/' + key + '.json')
                assert production
                assert flow.create_equipment_aggregation_task(production)
                assert flow.create_emission_task(production, 'chemistry', 'synthetic')
                if not storage.exists(config['emission_orders_path'] + '/' + production + '.json'):
                    break
                receipt = flow.sign_and_send_emission(production, config['sign'], 1)
                status = flow.update_emission_order_status(production)
                assert status.bufferStatus == 'ACTIVE'
                codes = flow.get_emission_kodes(receipt.orderId)
                assert codes and print_flow.generate_prn_files(receipt.orderId)
                production_data = json.loads(storage.read_text(config['production_orders_path'] + '/' + production + '.json'))
                if production_data['Gtin'].endswith('1'):
                    flow.create_virtual_production_tasks(production)
                    for gtin in ['04600000000002', '04600000000003']:
                        component = 'V-' + production + '-' + gtin
                        assert flow.create_emission_task(component, 'chemistry', 'synthetic')
                        receipt = flow.sign_and_send_emission(component, config['sign'], 1)
                        assert flow.update_emission_order_status(component).bufferStatus == 'ACTIVE'
                        assert flow.get_emission_kodes(receipt.orderId)
                        assert print_flow.generate_prn_files(receipt.orderId)
                break
            except OperationBusy:
                time.sleep(.01)
        else:
            raise AssertionError('Job did not complete within bounded retry')
