import json
import multiprocessing
from pathlib import Path

import pytest

from parallel_flow_store import SharedStorage, APIJournal, process_jobs


def prepare(root, count):
    storage = SharedStorage(root)
    config = {'operation_state_path': 's3://isolated/state',
              'production_orders_path': 's3://internal/productionOrders',
              'equipment-tasks': 's3://input/equipment-tasks',
              'equipment-reports': 's3://internal/equipment-reports',
              'emission_orders_path': 's3://internal/emissionOrders',
              'emission_receipts': 's3://internal/emissionReceipts',
              'emissions_path': 's3://internal/emissions', 'kodes': 's3://internal/kodes',
              'prn_tasks': 's3://print/solmarkTasks', 'prn_templates': 's3://print/templates',
              'unit_enabled_inns': ['7701234567'],
              'sscc_service_url': 'https://synthetic.invalid', 'sscc_prefix': '460705179',
              'sscc_extension': '0', 'sign': str(root / 'sign'),
              'signing': {'local_by_inn': {'7701234567': {'thumbprint': 'AB' * 20}}}}
    keys = []
    for i in range(count):
        key = 'JOB-' + str(i)
        gtin = '0460000000000' + ('1' if i % 3 == 2 else '0')
        storage.write_text('s3://input/Задания/' + key + '.json', json.dumps({
            'Gtin': gtin, 'Quantity': '0' if i % 3 == 0 else '1', 'Article': 'TEST',
            'PasportData': {'Batch_number': 'LOT', 'Product_PackQty': '2', 'Manufacturer_inn': '7701234567'}}))
        keys.append(key)
    for name in ['32x32_20x20.VDF', 'amica.json', 'mapping-empty.json']:
        storage.write_text(config['prn_templates'] + '/' + name, '{}')
    APIJournal(root)
    return config, storage, keys


@pytest.mark.parametrize('duplicates', [False, True], ids=['30-mixed-jobs', 'same-job-30-times'])
def test_three_processes_real_equipment_emission_codes_and_print_workflows(tmp_path, duplicates):
    config, storage, keys = prepare(tmp_path, 2 if duplicates else 30)
    if duplicates:
        keys = [keys[1]] * 30  # Positive quantity exercises all four stages.
    context = multiprocessing.get_context('spawn')
    barrier = context.Barrier(3)
    processes = [context.Process(target=process_jobs, args=(str(tmp_path), config, keys[i::3], barrier)) for i in range(3)]
    for process in processes:
        process.start()
    for process in processes:
        process.join(45)
        assert process.exitcode == 0
    journal = APIJournal(tmp_path)
    assert len(journal.rows('sscc')) == (1 if duplicates else 30)
    assert len(journal.rows('emission')) == (1 if duplicates else 40)
    assert len(journal.rows('codes')) == (1 if duplicates else 40)
    assert len(set(row[1] for row in journal.rows('emission'))) == len(journal.rows('emission'))
    if not duplicates:
        assert len(set(row[3] for row in journal.rows())) == 3
    for _, key, _, _ in journal.rows('emission'):
        order_id = 'SUZ-' + key
        codes = json.loads(storage.read_text(config['kodes'] + '/' + order_id + '.json'))['codes']
        if not key.startswith('V-'):
            csv = storage.read_text(config['prn_tasks'] + '/' + order_id + '.csv')
            assert csv.rstrip('\n').split('\n') == ['C1', *codes]
            assert storage.exists(config['prn_tasks'] + '/' + order_id + '.vdf')
            equipment = json.loads(storage.read_text(config['equipment-tasks'] + '/' + key + '.json'))
            assert equipment['palletNumbers'] and len(set(equipment['palletNumbers'])) == len(equipment['palletNumbers'])
        else:
            quantity = json.loads(storage.read_text(config['production_orders_path'] + '/' + key + '.json'))['Quantity']
            assert len(codes) == int(quantity) == (4 if key.endswith('2') else 2)
            assert not storage.exists(config['prn_tasks'] + '/' + order_id + '.csv')
