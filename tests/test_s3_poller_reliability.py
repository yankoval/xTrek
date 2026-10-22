"""Fault tests use temporary databases and fake S3/broker only."""
import io
import json
import multiprocessing
import os
import sqlite3
import subprocess
import sys
import time
from datetime import datetime, timezone
from unittest.mock import Mock

from botocore.exceptions import ClientError, ConnectionClosedError, EndpointConnectionError, ReadTimeoutError
import pytest

import xtrek.s3_poller as poller
from xtrek.s3_poller import EventLedger, S3EventPoller, event_identity, publish_pending

BUCKET = poller.INPUT_BUCKET
CUTOFF = datetime(2025, 1, 1, tzinfo=timezone.utc)
MODIFIED = datetime(2026, 1, 1, tzinfo=timezone.utc)
SOURCE = json.dumps({'Quantity': '2', 'PasportData': {'Manufacturer_inn': 'allowed'}}).encode()


def obj(key='Задания/order.json', etag='one', modified=MODIFIED):
    return {'Key': key, 'ETag': '"' + etag + '"', 'Size': len(SOURCE), 'LastModified': modified}


def error(code):
    return ClientError({'Error': {'Code': code, 'Message': 'DO-NOT-LOG-secret-business-body'}}, 'GetObject')


class S3:
    def __init__(self, objects=()):
        self.objects = {o['Key']: o for o in objects}
        self.gets, self.lists = [], []
        self.failures, self.bodies = {}, {}
        self.list_failures = {}
        self.page = None

    def list_objects_v2(self, **kwargs):
        self.lists.append(kwargs)
        prefix = kwargs['Prefix']
        if prefix in self.list_failures:
            raise self.list_failures[prefix]
        if self.page is not None:
            return self.page
        return {'Contents': [o for o in self.objects.values() if o['Key'].startswith(prefix)]}

    def get_object(self, **kwargs):
        self.gets.append(kwargs)
        key = kwargs['Key']
        if key in self.failures:
            raise self.failures[key]
        current = self.objects[key]
        if kwargs.get('IfMatch') != current['ETag']:
            raise error('PreconditionFailed')
        return {'Body': io.BytesIO(self.bodies.get(key, SOURCE)), 'ETag': current['ETag'],
                'ContentLength': current['Size'], 'LastModified': current['LastModified']}


@pytest.fixture
def case(tmp_path, monkeypatch):
    monkeypatch.setattr(poller, 'ROUTED_PREFIXES', {BUCKET: ('Задания/', 'receipts/')})
    ledger = EventLedger(str(tmp_path / 'outbox.db'))
    s3 = S3()
    scanner = S3EventPoller(s3, ledger, CUTOFF, allowed_inns=frozenset({'allowed'}))
    yield ledger, s3, scanner
    ledger.close()


def add(s3, *objects):
    s3.objects.update({o['Key']: o for o in objects})


def make_due(ledger):
    with ledger.db:
        ledger.db.execute('UPDATE discovery_retry SET next_retry=0')
        ledger.db.execute('UPDATE discovery_prefix_retry SET next_retry=0')


def test_581_known_inputs_use_zero_gets_and_no_writes_to_outbox(case):
    ledger, s3, scanner = case
    objects = [obj(f'Задания/{i}.json') for i in range(581)]
    add(s3, *objects)
    for o in objects:
        ledger.enqueue(event_identity(BUCKET, o['Key'], o, ()), BUCKET, o['Key'])
    queries = []
    ledger.db.set_trace_callback(queries.append)
    assert scanner.scan(max_pages=1) == 0
    assert s3.gets == []
    assert scanner.metrics['known_skips'] == scanner.metrics['records'] == 581
    assert scanner.metrics['list_calls'] == len(s3.lists) == 1
    assert not any(q.startswith(('INSERT INTO s3_outbox', 'INSERT OR IGNORE INTO s3_outbox', 'UPDATE discovery_retry')) for q in queries)


@pytest.mark.parametrize('failure', [error('InternalError'),
                                   ReadTimeoutError(endpoint_url='https://secret.invalid')])
def test_get_failure_does_not_block_receipt_or_publisher_and_survives_restart(case, failure):
    ledger, s3, scanner = case
    bad, good, receipt = obj(), obj('Задания/good.json'), obj('receipts/ready.json')
    add(s3, bad, good, receipt)
    s3.failures[bad['Key']] = failure
    ledger.enqueue('already-pending', BUCKET, 'receipts/earlier.json')
    assert scanner.scan(max_pages=2) == 2
    assert scanner.metrics['get_calls'] == len(s3.gets) == 2
    assert scanner.metrics['object_errors'] == 1
    assert scanner.metrics['status'] == 'degraded'
    sender = Mock()
    assert publish_pending(ledger, sender) == 3
    assert all(c.kwargs['retry'] is False for c in sender.send_task.call_args_list)
    identity = event_identity(BUCKET, bad['Key'], bad, ())
    reopened = EventLedger(str(ledger.path))
    assert reopened.retry_record(identity)[0] == 'pending'
    assert reopened.db.execute('SELECT attempts,reason FROM discovery_retry').fetchone() == (1, 'transient')
    s3.failures.clear()
    make_due(reopened)
    recovered = S3EventPoller(s3, reopened, CUTOFF, allowed_inns=frozenset({'allowed'}))
    assert recovered.scan(max_pages=0) == 1
    assert recovered.metrics['object_retries'] == 1
    assert reopened.known(identity)
    reopened.close()


@pytest.mark.parametrize('code,reason', [('AccessDenied', 'auth'), ('SignatureDoesNotMatch', 'auth'),
                                        ('ExpiredToken', 'auth'), ('NoSuchKey', 'missing'),
                                        ('InternalError', 'transient')])
def test_error_classification_and_backoff_are_durable_and_safe(case, code, reason, caplog):
    ledger, s3, scanner = case
    add(s3, obj())
    s3.failures[obj()['Key']] = error(code)
    before = time.time()
    with caplog.at_level('INFO'):
        scanner.scan(max_pages=1)
    row = ledger.db.execute('SELECT reason,attempts,next_retry,state FROM discovery_retry').fetchone()
    assert row[0:2] == (reason, 1) and row[3] == 'pending'
    assert row[2] > before + (239 if reason in {'auth', 'missing'} else 3)
    scanner.scan(max_pages=2)
    assert len(s3.gets) == 1  # repeated LIST cannot bypass backoff
    assert 'DO-NOT-LOG' not in caplog.text
    assert 'order.json' not in caplog.text
    assert scanner.metrics['status'] == 'degraded'


@pytest.mark.parametrize('body', [b'{bad json', b'[]', b'{"Quantity":2,"PasportData":[]}', b'\xff'])
def test_bad_json_is_deferred_not_admitted(case, body):
    ledger, s3, scanner = case
    add(s3, obj())
    s3.bodies[obj()['Key']] = body
    assert scanner.scan(max_pages=1) == 0
    assert ledger.db.execute('SELECT reason FROM discovery_retry').fetchone()[0] == 'invalid_json'


def test_known_version_skips_get_but_overwrite_and_backfill_still_validate_inn(case):
    ledger, s3, scanner = case
    first = obj()
    add(s3, first)
    assert scanner.scan(max_pages=1) == 1
    ledger.published(event_identity(BUCKET, first['Key'], first, ()))
    changed = obj(etag='changed')
    add(s3, changed)
    scanner.allowlist = frozenset({(BUCKET, changed['Key'])})
    s3.bodies[changed['Key']] = b'{"Quantity":"1","PasportData":{"Manufacturer_inn":"denied"}}'
    assert scanner.scan(max_pages=2) == 0
    assert len(s3.gets) == 2
    assert not ledger.known(event_identity(BUCKET, changed['Key'], changed, ()))
    assert scanner.metrics['inn_denied'] == 1


@pytest.mark.parametrize('same_etag', [False, True])
def test_overwrite_between_list_and_get_cannot_authorize_old_identity(case, same_etag):
    ledger, s3, scanner = case
    old = obj()
    add(s3, old)
    current = obj(etag='one' if same_etag else 'two', modified=datetime(2026, 2, 1, tzinfo=timezone.utc))
    add(s3, current)
    s3.page = {'Contents': [old]}
    assert scanner.scan(max_pages=1) == 0
    assert ledger.db.execute('SELECT reason,state FROM discovery_retry').fetchone() == ('replaced', 'replaced')
    s3.page = None
    assert scanner.scan(max_pages=2) == 1
    assert not ledger.known(event_identity(BUCKET, old['Key'], old, ()))
    assert ledger.known(event_identity(BUCKET, current['Key'], current, ()))


@pytest.mark.parametrize('exception_type', [EndpointConnectionError, ReadTimeoutError, ConnectionClosedError])
def test_list_outage_retains_page_tokens_and_delivers_existing_outbox(case, exception_type, caplog):
    ledger, s3, scanner = case
    ledger.save_scan_cursor(0, BUCKET, 'Задания/', 'page-2')
    s3.list_failures = {p: exception_type(endpoint_url='https://secret.invalid')
                        for p in ('Задания/', 'receipts/')}
    ledger.enqueue('ready', BUCKET, 'receipts/ready.json')
    assert scanner.scan() == 0
    assert scanner.metrics['list_calls'] == 2
    assert scanner.metrics['status'] == 'degraded'
    assert ledger.page_token(BUCKET, 'Задания/') == 'page-2'
    assert publish_pending(ledger, Mock()) == 1
    reopened = EventLedger(str(ledger.path))
    assert not reopened.prefix_ready(BUCKET, 'Задания/')
    reopened.close()
    assert 'error_type=' + exception_type.__name__ in caplog.text
    assert 'secret.invalid' not in caplog.text
    assert 'page-2' not in caplog.text


@pytest.mark.parametrize('response', [None, 'not-a-mapping', [], {}, {'Error': None},
                                    {'Error': []}, {'Error': 'not-a-mapping'},
                                    {'Error': {'Code': [], 'ArgumentName': []}}])
def test_malformed_error_response_is_transient(response):
    exc = RuntimeError('DO-NOT-LOG-secret-business-body')
    exc.response = response
    assert poller.error_kind(exc) == 'transient'


def test_list_read_timeout_resumes_saved_page_once_after_restart(case, caplog):
    ledger, s3, scanner = case
    source, receipt = obj(), obj('receipts/ready.json')
    add(s3, source, receipt)
    ledger.save_scan_cursor(0, BUCKET, 'Задания/', 'resume-this-page')
    s3.list_failures['Задания/'] = ReadTimeoutError(endpoint_url='https://secret.invalid')
    before = time.time()
    assert scanner.scan(max_pages=2) == 1  # The other prefix still progresses.
    assert scanner.metrics['status'] == 'degraded'
    assert scanner.metrics['errors_transient'] == 1
    assert ledger.page_token(BUCKET, 'Задания/') == 'resume-this-page'
    reason, attempts, next_retry = ledger.db.execute(
        'SELECT reason,attempts,next_retry FROM discovery_prefix_retry').fetchone()
    assert (reason, attempts) == ('transient', 1)
    assert next_retry > before + 3
    sender = Mock()
    assert publish_pending(ledger, sender) == 1

    reopened = EventLedger(str(ledger.path))
    try:
        recovered = S3EventPoller(s3, reopened, CUTOFF, allowed_inns=frozenset({'allowed'}))
        s3.list_failures.clear()
        make_due(reopened)
        s3.lists.clear()
        assert recovered.scan(max_pages=2) == 1
        assert s3.lists[0]['ContinuationToken'] == 'resume-this-page'
        assert recovered.metrics['status'] == 'ok'
        assert reopened.db.execute('SELECT COUNT(*) FROM discovery_prefix_retry').fetchone()[0] == 0
        assert publish_pending(reopened, sender) == 1
        assert recovered.scan(max_pages=2) == 0
        assert publish_pending(reopened, sender) == 0
        expected = {event_identity(BUCKET, o['Key'], o, ()) for o in (source, receipt)}
        assert {c.kwargs['task_id'] for c in sender.send_task.call_args_list} == expected
        assert sender.send_task.call_count == 2
    finally:
        reopened.close()
    assert 'operation=ListObjectsV2 error_type=ReadTimeoutError reason=transient' in caplog.text
    assert 'secret.invalid' not in caplog.text
    assert 'resume-this-page' not in caplog.text


@pytest.mark.parametrize('details,expected_token', [
    ({'Code': 'InvalidContinuationToken'}, None),
    ({'Code': 'InvalidArgument', 'ArgumentName': 'continuation-token'}, None),
    ({'Code': 'InvalidArgument', 'ArgumentName': 'ContinuationToken'}, None),
    ({'Code': 'InvalidArgument'}, 'keep-me'),
    ({'Code': 'InvalidArgument', 'ArgumentName': []}, 'keep-me'),
    ({'Code': 'ExpiredToken'}, 'keep-me'),
])
def test_only_explicit_invalid_cursor_clears_saved_token(case, details, expected_token):
    ledger, s3, scanner = case
    ledger.save_scan_cursor(0, BUCKET, 'Задания/', 'keep-me')
    s3.list_failures['Задания/'] = ClientError({'Error': details}, 'ListObjectsV2')
    assert scanner.scan(max_pages=1) == 0
    assert ledger.page_token(BUCKET, 'Задания/') == expected_token
    assert scanner.metrics['status'] == 'degraded'


def test_failed_source_does_not_starve_receipts(case):
    ledger, s3, scanner = case
    s3.list_failures['Задания/'] = error('AccessDenied')
    add(s3, obj('receipts/one.json'))
    assert scanner.scan(max_pages=2) == 1
    assert [c['Prefix'] for c in s3.lists] == ['Задания/', 'receipts/']
    assert scanner.metrics['checkpoints'] == 1


def test_request_budget_defers_rest_before_advancing_page(case):
    ledger, s3, scanner = case
    scanner.max_gets = 4
    objects = [obj(f'Задания/{i}.json') for i in range(15)]
    add(s3, *objects)
    s3.page = {'Contents': objects, 'IsTruncated': True, 'NextContinuationToken': 'next'}
    assert scanner.scan(max_pages=1) == 4
    assert len(s3.gets) == 4
    assert ledger.page_token(BUCKET, 'Задания/') == 'next'
    assert ledger.db.execute("SELECT COUNT(*) FROM discovery_retry WHERE attempts=0").fetchone()[0] == 11
    make_due(ledger)
    assert scanner.scan(max_pages=0) == 1  # retry share <= quarter of GET budget
    assert scanner.metrics['object_retries'] == 1


def test_time_budget_checkpoints_deferred_objects_without_more_gets(case, monkeypatch):
    ledger, s3, scanner = case
    add(s3, *[obj(f'Задания/{i}.json') for i in range(5)])
    clock = [0]
    monkeypatch.setattr(poller.time, 'monotonic', lambda: clock[0])
    original = s3.get_object
    def slow(**kw):
        clock[0] += scanner.cycle_seconds + 1
        return original(**kw)
    monkeypatch.setattr(s3, 'get_object', slow)
    assert scanner.scan(max_pages=20) == 1
    assert len(s3.gets) == 1
    assert scanner.metrics['deferred'] == 4
    assert scanner.metrics['checkpoints'] == 1
    assert len(s3.lists) == 1


@pytest.mark.parametrize('fault', ['retry', 'checkpoint'])
def test_sqlite_failure_never_advances_checkpoint_or_reports_success(case, monkeypatch, fault):
    ledger, s3, scanner = case
    add(s3, obj())
    s3.failures[obj()['Key']] = error('InternalError')
    s3.page = {'Contents': [obj()], 'IsTruncated': True, 'NextContinuationToken': 'next'}
    if fault == 'retry':
        ledger.db.execute("CREATE TRIGGER disk_full BEFORE INSERT ON discovery_retry BEGIN SELECT RAISE(ABORT, 'disk full'); END")
    else:
        ledger.db.execute("CREATE TRIGGER disk_full BEFORE UPDATE ON scan_control BEGIN SELECT RAISE(ABORT, 'disk full'); END")
    with pytest.raises(sqlite3.Error):
        scanner.scan(max_pages=1)
    assert ledger.page_token(BUCKET, 'Задания/') is None
    assert ledger.scan_cursor() == 0
    assert scanner.metrics['status'] == 'failed'
    if fault == 'checkpoint':
        assert ledger.db.execute('SELECT COUNT(*) FROM discovery_retry').fetchone()[0] == 1
    ledger.db.execute('DROP TRIGGER disk_full')
    ledger.db.commit()
    assert scanner.scan(max_pages=1) == 0
    assert ledger.page_token(BUCKET, 'Задания/') == 'next'


def _blocked_scanner(path, started, release):
    ledger = EventLedger(path)
    class Blocked:
        def list_objects_v2(self, **kwargs):
            started.set()
            release.wait(15)
            raise error('InternalError')
    scanner = S3EventPoller(Blocked(), ledger, CUTOFF)
    scanner.scan(max_pages=1)
    ledger.close()


def test_publisher_process_progresses_while_scanner_is_blocked(tmp_path):
    path = str(tmp_path / 'outbox.db')
    ledger = EventLedger(path)
    ledger.enqueue('ready', BUCKET, 'receipts/ready.json')
    ctx = multiprocessing.get_context('spawn')
    started, release = ctx.Event(), ctx.Event()
    process = ctx.Process(target=_blocked_scanner, args=(path, started, release))
    process.start()
    try:
        assert started.wait(10)
        assert publish_pending(ledger, Mock()) == 1
        assert process.is_alive()
    finally:
        release.set()
        process.join(20)
    assert process.exitcode == 0
    ledger.close()


def test_exclusive_role_lock_and_recovery(tmp_path):
    path = str(tmp_path / 'outbox.db')
    one, two = EventLedger(path), EventLedger(path)
    one.enqueue('id', 'b', 'k')
    with one.role_lock('publish'):
        with pytest.raises(RuntimeError, match='already active'):
            publish_pending(two, Mock())
        with two.role_lock('scan'):
            pass
    assert publish_pending(two, Mock()) == 1
    one.close()
    two.close()


def _crash_scan(path, stage):
    poller.ROUTED_PREFIXES = {BUCKET: ('Задания/', 'receipts/')}
    ledger = EventLedger(path)
    s3 = S3([obj()])
    s3.page = {'Contents': [obj()], 'IsTruncated': True, 'NextContinuationToken': 'next'}
    if stage != 'enqueue':
        s3.failures[obj()['Key']] = error('InternalError')
    method = 'enqueue' if stage == 'enqueue' else 'save_scan_cursor' if stage == 'checkpoint' else 'defer'
    original = getattr(ledger, method)
    def crash(*args, **kw):
        original(*args, **kw)
        os._exit(23)
    setattr(ledger, method, crash)
    S3EventPoller(s3, ledger, CUTOFF, allowed_inns=frozenset({'allowed'})).scan(max_pages=1)


@pytest.mark.parametrize('stage', ['retry', 'checkpoint', 'enqueue'])
def test_hard_restart_at_discovery_boundaries(tmp_path, stage):
    path = str(tmp_path / 'outbox.db')
    ctx = multiprocessing.get_context('spawn')
    process = ctx.Process(target=_crash_scan, args=(path, stage))
    process.start()
    process.join(15)
    assert process.exitcode == 23
    ledger = EventLedger(path)
    if stage == 'enqueue':
        assert publish_pending(ledger, Mock()) == 1
    else:
        assert ledger.db.execute('SELECT attempts,reason FROM discovery_retry').fetchone() == (1, 'transient')
        assert ledger.page_token(BUCKET, 'Задания/') == ('next' if stage == 'checkpoint' else None)
        make_due(ledger)
        scanner = S3EventPoller(S3([obj()]), ledger, CUTOFF, allowed_inns=frozenset({'allowed'}))
        assert scanner.scan(max_pages=0) == 1
        assert publish_pending(ledger, Mock()) == 1
    ledger.close()


def test_lost_broker_confirm_redelivery_uses_existing_operation_guards(tmp_path):
    from xtrek.operation_state import OperationState
    from xtrek.storage import LocalStorage
    ledger = EventLedger(str(tmp_path / 'outbox.db'))
    ledger.enqueue('stable-id', BUCKET, 'Задания/order.json')
    deliveries = []
    class Broker:
        def send_task(self, name, **kwargs):
            deliveries.append(kwargs)
            if len(deliveries) == 1:
                raise TimeoutError('confirm lost after broker accepted message')
    with pytest.raises(TimeoutError):
        publish_pending(ledger, Broker())
    assert ledger.db.execute('SELECT state,published_at FROM s3_outbox').fetchone() == ('pending', None)
    with ledger.db:
        ledger.db.execute('UPDATE s3_outbox SET next_attempt=0')
    assert publish_pending(ledger, Broker()) == 1
    effects = []
    for delivery in deliveries:
        assert delivery['task_id'] == 'stable-id'
        ledger.task_state(delivery['task_id'], 'started')
        for name in ('emission', 'sign-document', 'allocate-sscc'):
            state = OperationState(LocalStorage(), str(tmp_path / 'operations'), name, 'business-key', 'hash')
            state.acquire()
            if not state.cached:
                state.external()
                effects.append(name)
                state.accepted({'result': name})
                state.finish({'result': name})
        ledger.task_state(delivery['task_id'], 'succeeded')
    assert effects == ['emission', 'sign-document', 'allocate-sscc']
    assert ledger.db.execute('SELECT COUNT(*) FROM task_attempts').fetchone()[0] == 4
    assert ledger.db.execute('SELECT state,attempts FROM s3_outbox').fetchone() == ('completed', 2)
    ledger.close()


def test_fast_worker_completion_is_not_overwritten_by_confirm(case):
    ledger, _, _ = case
    ledger.enqueue('fast', 'b', 'k')
    sender = Mock()
    sender.send_task.side_effect = lambda *a, **kw: ledger.task_state(kw['task_id'], 'succeeded')
    assert publish_pending(ledger, sender) == 1
    assert ledger.db.execute('SELECT state,published_at FROM s3_outbox').fetchone()[0] == 'completed'


def test_publisher_configuration_is_minimal_and_requires_confirms(monkeypatch):
    monkeypatch.setenv('XTREK_LOCAL_BROKER_URL', 'amqp://poller:fake@127.0.0.1/test-only')
    app = poller.create_publisher_app()
    assert app.conf.broker_transport_options['confirm_publish'] is True
    assert app.conf.task_queues[0].durable
    assert app.conf.task_default_delivery_mode == 'persistent'
    assert not app.conf.task_publish_retry
    # Inspect in a fresh interpreter, independently of other tests importing tasks.
    result = subprocess.run([sys.executable, '-c',
        "import sys; from xtrek.s3_poller import create_publisher_app; create_publisher_app(); "
        "assert 'xtrek.tasks' not in sys.modules; assert 'boto3' not in sys.modules"], capture_output=True)
    assert result.returncode == 0, result.stderr.decode()


def test_backoff_is_bounded_even_after_many_failures():
    for reason, cap in [('transient', 300), ('auth', 3600), ('missing', 3600), ('invalid_json', 3600)]:
        assert 0.8 * cap <= poller.retry_delay(100000, reason) <= cap


def test_sdk_attempt_counters_match_actual_transport_requests(case, monkeypatch):
    import boto3
    from botocore.awsrequest import AWSResponse
    from botocore.config import Config
    ledger, _, _ = case
    client = boto3.client('s3', endpoint_url='https://s3.test.invalid',
                          aws_access_key_id='fake', aws_secret_access_key='fake',
                          config=Config(retries={'total_max_attempts': 1}, connect_timeout=5, read_timeout=10))
    calls = []
    class Raw:
        def __init__(self, body):
            self.body = body
        def stream(self, **kwargs):
            yield self.body
    listing = ('<ListBucketResult><IsTruncated>false</IsTruncated><Contents>'
               '<Key>Задания/order.json</Key><ETag>"one"</ETag><Size>79</Size>'
               '<LastModified>2026-01-01T00:00:00Z</LastModified>'
               '</Contents></ListBucketResult>').encode()
    def send(request):
        calls.append(request)
        if 'list-type=2' in request.url:
            return AWSResponse(request.url, 200, {}, Raw(listing))
        return AWSResponse(request.url, 500, {}, Raw(b'<Error><Code>InternalError</Code></Error>'))
    monkeypatch.setattr(client._endpoint.http_session, 'send', send)
    scanner = S3EventPoller(client, ledger, CUTOFF, allowed_inns=frozenset({'allowed'}))
    assert scanner.scan(max_pages=1) == 0
    assert len(calls) == 2
    for prefix in ('list', 'get'):
        assert scanner.metrics[prefix + '_calls'] == scanner.metrics[prefix + '_sdk_attempts'] == 1
    assert scanner.metrics['errors_transient'] == 1


def test_expired_credentials_do_not_clear_continuation_token(case):
    ledger, s3, scanner = case
    ledger.save_scan_cursor(0, BUCKET, 'Задания/', 'keep-me')
    s3.list_failures['Задания/'] = error('ExpiredToken')
    scanner.scan(max_pages=1)
    assert ledger.page_token(BUCKET, 'Задания/') == 'keep-me'
    assert ledger.db.execute('SELECT reason FROM discovery_prefix_retry').fetchone() == ('auth',)


def test_failed_cycle_health_retains_last_success_and_pending_age(case, monkeypatch, caplog):
    ledger, s3, scanner = case
    ledger.enqueue('pending', 'b', 'k')
    scanner.scan(max_pages=1)
    previous = scanner.metrics['last_successful_list']
    reopened = EventLedger(str(ledger.path))
    other = S3EventPoller(s3, reopened, CUTOFF)
    assert other.last_list == previous
    def fail(*a):
        raise sqlite3.OperationalError('disk full')
    monkeypatch.setattr(reopened, 'save_scan_cursor', fail)
    with caplog.at_level('INFO'), pytest.raises(sqlite3.Error):
        other.scan(max_pages=1)
    metrics = json.loads(reopened.db.execute("SELECT metrics FROM poller_health WHERE role='scan'").fetchone()[0])
    assert metrics['status'] == 'failed'
    assert metrics['pending'] == 1 and metrics['oldest_pending_age_seconds'] >= 0
    assert metrics['list_calls'] == 1 and metrics['checkpoints'] == 0
    assert '"status": "failed"' in caplog.text
    reopened.close()


def test_publish_confirm_then_disk_failure_keeps_event_for_safe_retry(case, monkeypatch):
    ledger, _, _ = case
    ledger.enqueue('pending', 'b', 'k')
    ledger.db.execute("CREATE TRIGGER disk_full BEFORE UPDATE OF published_at ON s3_outbox BEGIN SELECT RAISE(ABORT, 'disk full'); END")
    sender = Mock()
    with pytest.raises(sqlite3.Error):
        publish_pending(ledger, sender)
    assert sender.send_task.call_count == 1
    assert ledger.db.execute('SELECT state,published_at,attempts FROM s3_outbox').fetchone() == ('pending', None, 1)
    assert ledger.claim() is None  # durable lease survives lost local acknowledgement
    assert ledger.claim(now=time.time() + 121)[0] == 'pending'


def test_additive_migration_preserves_legacy_data_and_rollback_sql(tmp_path):
    path = str(tmp_path / 'legacy.db')
    db = sqlite3.connect(path)
    db.executescript('''
        CREATE TABLE s3_outbox (
            event_id TEXT PRIMARY KEY, bucket TEXT NOT NULL, object_key TEXT NOT NULL,
            payload TEXT NOT NULL, state TEXT NOT NULL DEFAULT 'pending',
            attempts INTEGER NOT NULL DEFAULT 0, next_attempt REAL NOT NULL DEFAULT 0,
            lease_until REAL NOT NULL DEFAULT 0, last_error TEXT,
            created_at REAL NOT NULL, published_at REAL);
        CREATE TABLE task_attempts (event_id TEXT NOT NULL, state TEXT NOT NULL,
            updated_at REAL NOT NULL, detail TEXT, PRIMARY KEY(event_id,state,updated_at));
        CREATE TABLE prefix_scan_state (bucket TEXT NOT NULL,prefix TEXT NOT NULL,
            continuation_token TEXT,PRIMARY KEY(bucket,prefix));
        CREATE TABLE scan_control (singleton INTEGER PRIMARY KEY CHECK(singleton=1),prefix_index INTEGER NOT NULL);
        INSERT INTO s3_outbox VALUES('old','b','k','{"bucket":"b","key":"k"}','pending',7,123,456,'TimeoutError',1,NULL);
        INSERT INTO task_attempts VALUES('old','started',2,NULL);
        INSERT INTO prefix_scan_state VALUES('b','prefix/','token');
        INSERT INTO scan_control VALUES(1,3);
    ''')
    before = {t: db.execute('SELECT * FROM ' + t).fetchall() for t in
              ('s3_outbox', 'task_attempts', 'prefix_scan_state', 'scan_control')}
    db.close()
    ledger = EventLedger(path)
    for table, expected in before.items():
        assert ledger.db.execute('SELECT * FROM ' + table).fetchall() == expected
    ledger.defer('retry', BUCKET, obj()['Key'], obj(), 'transient')
    # Rollback is allowed only after draining discovery_retry; preserve current
    # DB, not a stale backup. Old SQL can read/write with additive tables present.
    assert ledger.health()['retry_pending'] == 1
    ledger.resolve_retry('retry')
    ledger.close()
    legacy = sqlite3.connect(path)
    legacy.execute("UPDATE s3_outbox SET state='published',published_at=5,lease_until=0 WHERE event_id='old'")
    legacy.execute("INSERT INTO task_attempts VALUES('old','succeeded',6,NULL)")
    legacy.commit()
    legacy.close()
    upgraded_again = EventLedger(path)
    assert upgraded_again.retry_record('retry')[0] == 'resolved'
    assert upgraded_again.db.execute('SELECT attempts,published_at FROM s3_outbox').fetchone() == (7, 5)
    assert upgraded_again.page_token('b', 'prefix/') == 'token'
    assert upgraded_again.scan_cursor() == 3
    upgraded_again.close()


def test_real_sqlite_full_keeps_uncheckpointed_page(case):
    ledger, s3, scanner = case
    huge = dict(obj(), ETag='x' * 1000000)
    add(s3, huge)
    s3.failures[huge['Key']] = error('InternalError')
    pages = ledger.db.execute('PRAGMA page_count').fetchone()[0]
    ledger.db.execute(f'PRAGMA max_page_count={pages}')
    with pytest.raises(sqlite3.OperationalError, match='full'):
        scanner.scan(max_pages=1)
    assert ledger.page_token(BUCKET, 'Задания/') is None
    assert ledger.scan_cursor() == 0
    assert scanner.metrics['status'] == 'failed'
    assert ledger.db.execute('SELECT COUNT(*) FROM discovery_retry').fetchone()[0] == 0


def test_slow_retry_still_allows_a_discovery_page(case, monkeypatch):
    ledger, s3, scanner = case
    add(s3, obj(), obj('receipts/ready.json'))
    ledger.defer(event_identity(BUCKET, obj()['Key'], obj(), ()), BUCKET, obj()['Key'], obj(), 'transient')
    make_due(ledger)
    ledger.save_scan_cursor(1, BUCKET, 'Задания/', None)
    clock = [0]
    monkeypatch.setattr(poller.time, 'monotonic', lambda: clock[0])
    original = s3.get_object
    def slow(**kw):
        clock[0] += scanner.cycle_seconds + 1
        return original(**kw)
    monkeypatch.setattr(s3, 'get_object', slow)
    assert scanner.scan(max_pages=2) == 2
    assert len(s3.lists) == 1
    assert s3.lists[0]['Prefix'] == 'receipts/'


def test_failed_health_commit_does_not_log_ok(case, monkeypatch, caplog):
    ledger, s3, scanner = case
    def fail(*a):
        raise sqlite3.OperationalError('full')
    monkeypatch.setattr(ledger, 'record_health', fail)
    with caplog.at_level('INFO'), pytest.raises(sqlite3.Error):
        scanner.scan(max_pages=1)
    assert scanner.metrics['status'] == 'failed'
    assert '"status": "ok"' not in caplog.text


def test_previously_resolved_retry_is_reactivated_on_new_failure(case):
    ledger, s3, scanner = case
    item = obj()
    identity = event_identity(BUCKET, item['Key'], item, ())
    add(s3, item)
    ledger.defer(identity, BUCKET, item['Key'], item, 'transient')
    ledger.resolve_retry(identity)  # e.g. signer policy rejected it on a retry
    s3.failures[item['Key']] = error('InternalError')
    scanner.scan(max_pages=1)
    assert ledger.retry_record(identity)[0] == 'pending'
    assert ledger.db.execute('SELECT attempts FROM discovery_retry').fetchone()[0] == 2


def test_incomplete_get_metadata_remains_retryable(case, monkeypatch):
    ledger, s3, scanner = case
    add(s3, obj())
    monkeypatch.setattr(s3, 'get_object', lambda **kw: {'Body': io.BytesIO(SOURCE)})
    scanner.scan(max_pages=1)
    assert ledger.db.execute('SELECT reason,state FROM discovery_retry').fetchone() == ('transient', 'pending')


def test_publisher_health_failure_is_logged_as_failure(case, monkeypatch, caplog):
    ledger, _, _ = case
    monkeypatch.setattr(ledger, 'record_health', Mock(side_effect=sqlite3.OperationalError('full')))
    with caplog.at_level('INFO'), pytest.raises(sqlite3.Error):
        publish_pending(ledger, Mock())
    assert '"status": "ok"' not in caplog.text
    assert '"status": "failed"' in caplog.text


def test_publish_role_boots_without_s3_or_worker_configuration(tmp_path):
    path = str(tmp_path / 'outbox.db')
    env = {'XTREK_LOCAL_S3_POLLING': '1', 'XTREK_S3_POLL_LEDGER': path,
           'XTREK_LOCAL_BROKER_URL': 'amqp://fake:fake@127.0.0.1/test-only'}
    # Empty outbox: no broker connection or production environment is used.
    result = subprocess.run([sys.executable, '-m', 'xtrek.s3_poller', '--role', 'publish', '--once'],
                            env=env, capture_output=True, timeout=15)
    assert result.returncode == 0, result.stderr.decode()
    ledger = EventLedger(path)
    metrics = json.loads(ledger.db.execute("SELECT metrics FROM poller_health WHERE role='publish'").fetchone()[0])
    assert metrics['status'] == 'ok' and metrics['publish_attempts'] == 0
    ledger.close()


def _crash_publisher(path, journal):
    ledger = EventLedger(path)
    class Accepted:
        def send_task(self, *args, **kwargs):
            with open(journal, 'w') as stream:
                stream.write(kwargs['task_id'])
                stream.flush()
                os.fsync(stream.fileno())
            os._exit(23)
    publish_pending(ledger, Accepted())


def test_hard_publisher_restart_preserves_lease_and_stable_delivery_id(tmp_path):
    path, journal = str(tmp_path / 'outbox.db'), str(tmp_path / 'accepted')
    ledger = EventLedger(path)
    ledger.enqueue('stable', 'b', 'k')
    ledger.close()
    ctx = multiprocessing.get_context('spawn')
    process = ctx.Process(target=_crash_publisher, args=(path, journal))
    process.start()
    process.join(15)
    assert process.exitcode == 23
    ledger = EventLedger(path)
    assert ledger.claim() is None
    with ledger.db:
        ledger.db.execute('UPDATE s3_outbox SET lease_until=0')  # advance lease expiry without a 120s sleep
    sender = Mock()
    assert publish_pending(ledger, sender) == 1
    assert sender.send_task.call_args.kwargs['task_id'] == (tmp_path / 'accepted').read_text() == 'stable'
    assert ledger.db.execute('SELECT attempts FROM s3_outbox').fetchone()[0] == 2
    ledger.close()


@pytest.mark.parametrize('get_modified,get_etag,get_size,allowed,expected', [
    ('Thu, 01 Jan 2026 00:00:00 GMT', '"one"', len(SOURCE), True, 'enqueue'),
    ('Thu, 01 Jan 2026 00:00:00 GMT', '"one"', len(SOURCE), False, 'denied'),
    ('Thu, 01 Jan 2026 00:00:01 GMT', '"one"', len(SOURCE), True, 'replaced'),
    ('Wed, 31 Dec 2025 23:59:59 GMT', '"one"', len(SOURCE), True, 'replaced'),
    ('Thu, 01 Jan 2026 00:00:00 GMT', '"changed"', len(SOURCE), True, 'replaced'),
    ('Thu, 01 Jan 2026 00:00:00 GMT', '"one"', len(SOURCE) + 1, True, 'replaced'),
])
def test_fractional_list_timestamp_vs_http_get_preserves_identity_and_policy(
        case, monkeypatch, get_modified, get_etag, get_size, allowed, expected):
    """Real SDK parser: LIST preserves milliseconds; HTTP-date cannot carry them."""
    monkeypatch.setattr(poller, 'ROUTED_PREFIXES', {BUCKET: ('Задания/',)})
    import boto3
    from botocore.awsrequest import AWSResponse
    from botocore.config import Config
    ledger, _, _ = case
    client = boto3.client('s3', endpoint_url='https://s3.test.invalid',
                          aws_access_key_id='fake', aws_secret_access_key='fake',
                          config=Config(retries={'total_max_attempts': 1}))
    listing = (f'<ListBucketResult><IsTruncated>false</IsTruncated><Contents>'
               f'<Key>Задания/order.json</Key><ETag>"one"</ETag><Size>{len(SOURCE)}</Size>'
               '<LastModified>2026-01-01T00:00:00.870Z</LastModified>'
               '</Contents></ListBucketResult>').encode()
    calls = []
    class Raw(io.BytesIO):
        def stream(self, amt=1024, **kw):
            while chunk := self.read(amt):
                yield chunk
    def send(request):
        calls.append('LIST' if 'list-type=2' in request.url else 'GET')
        if calls[-1] == 'LIST':
            return AWSResponse(request.url, 200, {}, Raw(listing))
        assert request.headers['If-Match'] == b'"one"'
        return AWSResponse(request.url, 200,
                           {'etag': get_etag, 'last-modified': get_modified,
                            'content-length': str(get_size)}, Raw(SOURCE))
    monkeypatch.setattr(client._endpoint.http_session, 'send', send)
    scanner = S3EventPoller(client, ledger, CUTOFF,
                            allowed_inns=frozenset({'allowed' if allowed else 'other'}))
    assert scanner.scan(max_pages=1) == (1 if expected == 'enqueue' else 0)
    assert calls == ['LIST', 'GET']
    # This is the exact identity from the pre-fix implementation, retaining .870.
    identity = '1ad05fdc53b075c810cf85cde73ae006d9b9d8c775497fe83d7c680a147c336d'
    listed = obj(modified=MODIFIED.replace(microsecond=870000))
    assert event_identity(BUCKET, listed['Key'], listed, ()) == identity
    if expected == 'enqueue':
        assert ledger.known(identity)
        assert ledger.db.execute('SELECT event_id FROM s3_outbox').fetchall() == [(identity,)]
        assert ledger.db.execute('SELECT COUNT(*) FROM discovery_retry').fetchone()[0] == 0
        # Restart: an unchanged identity is known, with no second GET or enqueue.
        reopened = EventLedger(str(ledger.path))
        try:
            resumed = S3EventPoller(client, reopened, CUTOFF, allowed_inns=frozenset({'allowed'}))
            assert resumed.scan(max_pages=1) == 0
            assert calls == ['LIST', 'GET', 'LIST']
            assert resumed.metrics['known_skips'] == 1
            assert publish_pending(reopened, Mock()) == 1
        finally:
            reopened.close()
    elif expected == 'denied':
        assert not ledger.known(identity)
        assert scanner.metrics['inn_denied'] == 1
        assert ledger.db.execute('SELECT COUNT(*) FROM discovery_retry').fetchone()[0] == 0
    else:
        assert not ledger.known(identity)
        assert ledger.retry_record(identity)[0] == 'replaced'


def test_nonzero_get_fraction_is_compared_without_truncation(case, monkeypatch):
    ledger, s3, scanner = case
    item = obj(modified=MODIFIED.replace(microsecond=870000))
    add(s3, item)
    original = s3.get_object
    def mismatched(**kw):
        response = original(**kw)
        response['LastModified'] = MODIFIED.replace(microsecond=871000)
        return response
    monkeypatch.setattr(s3, 'get_object', mismatched)
    assert scanner.scan(max_pages=1) == 0
    assert ledger.db.execute('SELECT reason,state FROM discovery_retry').fetchone() == ('replaced', 'replaced')


def test_fractional_list_metadata_survives_durable_retry_restart(case, monkeypatch):
    ledger, s3, scanner = case
    item = obj(modified=MODIFIED.replace(microsecond=870000))
    add(s3, item)
    s3.failures[item['Key']] = error('InternalError')
    assert scanner.scan(max_pages=1) == 0
    stored = json.loads(ledger.db.execute('SELECT object_meta FROM discovery_retry').fetchone()[0])
    assert stored['LastModified'] == '2026-01-01T00:00:00.870000+00:00'
    make_due(ledger)
    s3.failures.clear()
    original = s3.get_object
    def seconds_only(**kw):
        response = original(**kw)
        response['LastModified'] = MODIFIED
        return response
    monkeypatch.setattr(s3, 'get_object', seconds_only)
    reopened = EventLedger(str(ledger.path))
    try:
        resumed = S3EventPoller(s3, reopened, CUTOFF, allowed_inns=frozenset({'allowed'}))
        assert resumed.scan(max_pages=0) == 1
        identity = '1ad05fdc53b075c810cf85cde73ae006d9b9d8c775497fe83d7c680a147c336d'
        assert reopened.known(identity)
        assert reopened.retry_record(identity)[0] == 'resolved'
        assert item['LastModified'].microsecond == 870000
        assert reopened.db.execute('SELECT object_meta FROM discovery_retry').fetchone()[0] == json.dumps(stored, ensure_ascii=False)
    finally:
        reopened.close()
