"""Blocking contracts: real production loops, synthetic history, no external I/O.

The current production loops violate several contracts. These failures must stay
visible until their algorithms are fixed; do not xfail or raise the budgets.
"""
from contextlib import nullcontext
from datetime import timedelta
import hashlib
import json
from pathlib import Path
import sqlite3
import sys
from types import ModuleType, SimpleNamespace

import pytest

from .meter import MeteredS3, NOW, assert_growth_budget, assert_idle_budget

HISTORIES = (1000, 10000)
INTERNAL = '20ab2a0c-2726-4ba1-9c7c-7deae82941ff'
FRESH = 'equipment-reports/T-ffffffff-ffff-4fff-8fff-ffffffffffff.json'
LATE = 'equipment-reports/T-eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee.json'
OVERWRITTEN = 'equipment-reports/T-' + '0' * 32 + '.json'


class EndCycles(BaseException):
    """Escape production retry handlers only after the measured cycle finishes."""


class Clock:
    def __init__(self, s3):
        self.value = NOW.timestamp()
        self.s3 = s3
        self.cycles = 0
        self.warm_usage = None
        self.idle_usage = None

    def time(self):
        return self.value

    monotonic = time

    def sleep(self, seconds):
        self.value += seconds
        self.cycles += 1
        if self.cycles == 1:
            self.warm_usage = self.s3.usage()
            self.s3.reset()
        elif self.cycles == 2:
            self.idle_usage = self.s3.usage()
            # Check that an idle optimization does not disable later discovery.
            self.s3.add(INTERNAL, LATE, modified=NOW + timedelta(seconds=self.value - NOW.timestamp()))
        elif self.cycles == 3:
            # An old key may be overwritten; lexicographic high-watermarks lose it.
            self.s3.add(INTERNAL, OVERWRITTEN, body=b'{"revision": 2}',
                        modified=NOW + timedelta(seconds=self.value - NOW.timestamp()))
        elif self.cycles == 4:
            raise EndCycles


def add_reports(s3, count):
    for index in range(count):
        s3.add(INTERNAL, f'equipment-reports/T-{index:032x}.json',
               modified=NOW - timedelta(days=30))
    s3.add(INTERNAL, FRESH)


def measure_pallet(root, count, monkeypatch):
    import boto3
    import xtrek.pallet_label_report_poller as module

    s3 = MeteredS3()
    add_reports(s3, count)
    clock = Clock(s3)
    sent = []
    connection = SimpleNamespace(transport_options={})
    app = SimpleNamespace(
        conf=SimpleNamespace(broker_url='amqp://localhost'),
        connection_for_write=lambda: nullcontext(connection),
        amqp=SimpleNamespace(Producer=lambda _: nullcontext(object())),
        send_task=lambda *args, **kwargs: sent.append((args, kwargs)),
    )
    # Avoid importing the business worker; only its external broker/config
    # boundary is replaced. Discovery, pagination, filtering and ledger are real.
    tasks = ModuleType('xtrek.tasks')
    tasks.app, tasks.config = app, {'s3_config': {}}
    monkeypatch.setitem(sys.modules, 'xtrek.tasks', tasks)
    monkeypatch.setattr(boto3, 'client', lambda *args, **kwargs: s3)
    monkeypatch.setattr(module, 'time', clock)
    settings = root / 'pallet.json'
    settings.write_text(json.dumps({
        'enabled': True, 'report_bucket': INTERNAL,
        'poller_ledger_path': str(root / 'pallet.sqlite3'),
        'source_cutoff_utc': (NOW - timedelta(days=1)).isoformat(),
        'backlog_report_ids': [], 'backlog_source_uuids': [],
    }))
    monkeypatch.setenv('XTREK_PALLET_DELIVERY_CONFIG', str(settings))
    with pytest.raises(EndCycles):
        module.main()
    assert [call[1]['args'] for call in sent] == [[FRESH], [LATE], [OVERWRITTEN]], (
        'Warm discovery, a later event and an overwritten old key must each publish once')
    assert clock.warm_usage['calls']['LIST'] == (count + 1 + 999) // 1000
    return clock.idle_usage


def measure_scanner(root, count, monkeypatch):
    import boto3
    import xtrek.s3_poller as module

    s3 = MeteredS3()
    add_reports(s3, count)
    clock = Clock(s3)
    monkeypatch.setattr(boto3, 'client', lambda *args, **kwargs: s3)
    monkeypatch.setattr(module, 'time', clock)
    monkeypatch.setattr(sys, 'argv', ['s3_poller', '--role', 'scan'])
    # One routed prefix isolates growth of this archive; do not substitute scan().
    monkeypatch.setattr(module, 'configured_routes', lambda *args: {INTERNAL: ('equipment-reports/',)})
    for key, value in {
        'XTREK_LOCAL_S3_POLLING': '1', 'XTREK_S3_POLL_LEDGER': str(root / 'scan.sqlite3'),
        'XTREK_S3_POLL_SECONDS': '120', 'XTREK_S3_CUTOFF_UTC': (NOW - timedelta(days=1)).isoformat(),
        'XTREK_LOCAL_ALLOWED_INNS': 'synthetic', 'XTREK_S3_ACCESS_KEY': 'offline',
        'XTREK_S3_SECRET_KEY': 'offline',
    }.items():
        monkeypatch.setenv(key, value)
    with pytest.raises(EndCycles):
        module.main()
    with sqlite3.connect(root / 'scan.sqlite3') as db:
        assert sorted(row[0] for row in db.execute('SELECT object_key FROM s3_outbox')) == sorted(
            [FRESH, LATE, OVERWRITTEN]), 'Idle savings must preserve new and overwritten events'
    assert clock.warm_usage['calls']['LIST'] > 0
    return clock.idle_usage


def measure_notifier(root, count, monkeypatch):
    from xtrek.task_notifier import NotifierSettings, TASK_BUCKET, TASK_PREFIX, TaskNotifier, scan_and_deliver

    s3 = MeteredS3()
    settings = NotifierSettings('offline', 'synthetic-chat', 'offline-ca', root / 'notifier.sqlite3')
    notifier = TaskNotifier(s3=s3, settings=settings)
    rows = []
    for index in range(count):
        key = f'{TASK_PREFIX}{index:032x}.json'
        s3.add(TASK_BUCKET, key)
        rows.append((TASK_BUCKET, Path(key).stem, key, settings.max_chat_id,
                     NOW.isoformat(), NOW.isoformat(), int(NOW.timestamp() * 1000),
                     s3.objects[TASK_BUCKET, key]['ETag'], f'message-{index}'))
    # Persist the exact terminal state reached by a prior successful delivery.
    with notifier._db() as db:
        db.executemany('''INSERT INTO logical_deliveries
            (bucket_id,task_id,object_key,recipient,mode,state,created_at,updated_at,
             first_seen_ms,etag,message_id)
            VALUES (?,?,?,?,'json-link','delivered',?,?,?,?,?)''', rows)
    cutoff = NOW - timedelta(days=1)
    assert scan_and_deliver(notifier, s3=s3, cutoff_utc=cutoff) == count
    s3.reset()
    assert scan_and_deliver(notifier, s3=s3, cutoff_utc=cutoff) == count
    with notifier._db() as db:
        assert db.execute("SELECT count(*) FROM logical_deliveries WHERE state='delivered'").fetchone()[0] == count
    return s3.usage()


def measure_backup(root, count, monkeypatch):
    from xtrek.sscc_backup import BackupStore, SLOTS
    from xtrek.sscc_local import Ledger, _check_digit

    path = root / 'allocations.sqlite3'
    prefix = '460705179'
    Ledger.initialize(str(path), prefix)
    params = hashlib.sha256(json.dumps({'prefix': prefix, 'extension': 3, 'count': 1},
                                      sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    rows = []
    for index in range(count):
        body = f'3{prefix}{index:07d}'
        rows.append((hashlib.sha256(f'fixture-{index}'.encode()).hexdigest(), params,
                     json.dumps({'ssccs': [body + _check_digit(body)]}), index, 1))
    with sqlite3.connect(path) as db:
        db.executemany('INSERT INTO allocations VALUES (?,?,?,?,?)', rows)
        db.execute('UPDATE counters SET next_serial=?', (count,))
    ledger = Ledger(str(path), prefix)  # Check realistic contiguous history.
    s3 = MeteredS3()
    store = BackupStore(s3, 'offline-backups', prefix, 'offline-state')
    for i in range(9):
        ledger.allocate(f'new-{i}', prefix, 1)
        store.run(path, NOW + timedelta(days=i // 3, minutes=i))
    assert len(s3.objects) == len(SLOTS) == 5, 'Warm-up must retain verified snapshots'
    s3.reset()
    assert store.run(path, NOW + timedelta(days=2, minutes=13))['state'] == 'unchanged'
    return s3.usage()


MEASUREMENTS = {
    'pallet': measure_pallet,
    'scanner': measure_scanner,
    'notifier': measure_notifier,
    'backup': measure_backup,
}


@pytest.fixture(scope='module', params=MEASUREMENTS)
def measured(request, tmp_path_factory):
    """Compute each real workload once, reuse it for idle and ×10 assertions."""
    results = []
    for count in HISTORIES:
        root = tmp_path_factory.mktemp(f'cost-{request.param}-{count}')
        with pytest.MonkeyPatch.context() as patch:
            # Module fixture runs before function-scoped autouse fixtures.
            import socket
            def forbidden(*args, **kwargs):
                raise AssertionError('Cost measurement attempted a real network connection')
            patch.setattr(socket.socket, 'connect', forbidden)
            patch.setattr(socket.socket, 'connect_ex', forbidden)
            patch.setattr(socket, 'create_connection', forbidden)
            patch.setenv('AWS_EC2_METADATA_DISABLED', 'true')
            results.append(MEASUREMENTS[request.param](root, count, patch))
    return request.param, results


def test_empty_repeat_cycle_stays_within_budget(measured, record_property):
    name, (small, large) = measured
    record_property('s3_cost_measurements', json.dumps({'1000': small, '10000': large}))
    assert_idle_budget(large, context=f'{name}, 10000 historical entries')


def test_history_x10_does_not_multiply_background_cost(measured, record_property):
    name, (small, large) = measured
    record_property('s3_cost_measurements', json.dumps({'1000': small, '10000': large}))
    assert_growth_budget(small, large, context=name)
