"""Real isolated RabbitMQ + real Celery router; HTTP MAX fault simulator.
Run from workspace with source/tests on PYTHONPATH. No production credentials.
"""
import io
import json
import os
import socket
import threading
import time
from datetime import datetime, timezone, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest.mock import Mock

import pytest
import requests
from celery import Celery
from celery.contrib.testing.worker import start_worker
from celery.worker.request import Request

BROKER = os.environ.get('XTREK_TEST_BROKER_URL', 'amqp://pilot:isolated-test-only@127.0.0.1:25672/pilot')
pytestmark = pytest.mark.skipif(os.environ.get('XTREK_TEST_RABBITMQ') != '1', reason='requires isolated RabbitMQ')
RESULTS = []


def wait_for(check, timeout=15):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if check():
            return
        time.sleep(.05)
    raise AssertionError('condition timed out')


@pytest.fixture
def rig(tmp_path, monkeypatch, request):
    from test_tasks_unit_set_flow import import_tasks
    from xtrek import max_queue_pilot as pilot
    from urllib.parse import urlsplit
    broker = urlsplit(BROKER)
    assert broker.hostname == '127.0.0.1' and broker.port == 25672 and broker.path == '/pilot'
    key = 'equipment-tasks/T-isolated-' + tmp_path.name + '.json'
    now = datetime.now(timezone.utc) - timedelta(seconds=2)
    cohort = tmp_path / 'cohort.json'
    cohort.write_text(json.dumps({'not_before': (now-timedelta(seconds=10)).isoformat(), 'keys': [key]}))
    monkeypatch.setenv('XTREK_MAX_QUEUE_PILOT_COHORT', str(cohort))
    monkeypatch.setenv('XTREK_MAX_QUEUE_PILOT_ENABLED', '1')
    monkeypatch.setenv('XTREK_LOCAL_S3_POLLING', '1')
    monkeypatch.setenv('XTREK_LOCAL_BROKER_URL', BROKER)
    monkeypatch.setenv('XTREK_LOCAL_QUEUE_NAME', 'xtrek_s3_events')
    monkeypatch.setenv('XTREK_S3_POLL_LEDGER', str(tmp_path / 'outbox.sqlite3'))
    monkeypatch.setenv('XTREK_MAX_PILOT_BOT_TOKEN', 'synthetic-token')
    monkeypatch.setenv('XTREK_MAX_PILOT_CA_FILE', 'synthetic-ca')
    # Block unintended outbound sockets even if a test seam is accidentally missed.
    original_connect = socket.socket.connect
    def connect(sock, address):
        assert address[0] in ('127.0.0.1', '::1'), 'non-loopback network forbidden'
        return original_connect(sock, address)
    monkeypatch.setattr(socket.socket, 'connect', connect)
    tasks = import_tasks(monkeypatch, 'xtrek.tasks', Celery)
    tasks.config['s3_config'] = {}
    monkeypatch.setattr(tasks, '_record_local_event_state', Mock())
    business = {}
    for name in list(vars(tasks)):
        if name.startswith('logic_'):
            business[name] = Mock(side_effect=AssertionError('business route entered'))
            monkeypatch.setattr(tasks, name, business[name])
    monkeypatch.setattr(tasks.document_signer, 'self_test', lambda: None)
    calls, events = [], []
    release = threading.Event()
    behavior = {'mode': 'ok'}
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args): pass
        def do_GET(self):
            events.append('FORBIDDEN_GET'); self.send_error(405)
        def do_POST(self):
            body = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
            calls.append(body); events.append('POST')
            if behavior['mode'] == 'hold':
                release.wait(10)
            if behavior['mode'] == 'drop' and len(calls) == 1:
                self.connection.close(); return
            status = 503 if behavior['mode'] == 'always503' or (behavior['mode'] == '503' and len(calls) == 1) else 200
            payload = {} if behavior['mode'] == 'missing_mid' and len(calls) == 1 else {'message': {'body': {'mid': 'mid.isolated-' + str(len(calls))}}}
            if status == 200 and payload:
                events.append('MID')
            raw = json.dumps(payload).encode()
            self.send_response(status); self.send_header('Content-Length', str(len(raw))); self.end_headers(); self.wfile.write(raw)
    server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    def post(url, **kw):
        assert url == 'https://platform-api2.max.ru/messages'
        assert kw['params']['chat_id'] == pilot.CHAT
        assert kw['allow_redirects'] is False
        assert set(kw['json']) == {'text', 'format', 'notify'}
        assert 'attachments' not in kw['json']
        kw.pop('verify')
        # The isolated endpoint never uses system proxies (also fork-safe on macOS).
        with requests.Session() as session:
            session.trust_env = False
            return session.post(f'http://127.0.0.1:{server.server_port}/messages', **kw)
    monkeypatch.setattr(requests, 'post', post)
    body = json.dumps({'id': Path(key).stem, 'date': now.isoformat()}).encode()
    class Storage:
        def head_object(self, **kw):
            return {'ETag': 'fixture', 'ContentLength': len(body), 'LastModified': now}
        def get_object(self, **kw):
            assert kw['IfMatch'] == 'fixture'
            return {'Body': io.BytesIO(body)}
        def generate_presigned_url(self, *args, **kw):
            stamp = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
            return f'https://{pilot.HOST}/{pilot.BUCKET}/{key}?X-Amz-Date={stamp}&X-Amz-Expires=172800&X-Amz-Signature=' + 'a'*64
    monkeypatch.setattr(pilot, 'publisher_storage', lambda cfg: Storage())
    original_ack = Request.acknowledge
    def ack(req):
        events.append('ACK'); return original_ack(req)
    monkeypatch.setattr(Request, 'acknowledge', ack)
    monkeypatch.setattr(pilot, 'retry_pause', lambda seconds: time.sleep(.02))
    monkeypatch.setattr(tasks.process_s3_event, 'retry', Mock(side_effect=AssertionError('Celery retry forbidden')))
    tasks.app.conf.update(worker_hijack_root_logger=False)
    with tasks.app.connection() as connection:
        connection.default_channel.queue_delete('xtrek_s3_events')
    import billiard
    previous_method = billiard.get_start_method()
    if getattr(request, 'param', 'solo') == 'prefork':
        billiard.set_start_method('fork', force=True)
    try:
        with start_worker(tasks.app, pool=getattr(request, 'param', 'solo'), concurrency=1, perform_ping_check=False) as worker:
            behavior['worker'] = worker
            yield tasks, key, behavior, calls, events, release, business
    finally:
        release.set(); server.shutdown(); server.server_close()
        tasks.app.close()
        billiard.set_start_method(previous_method, force=True)
        RESULTS.append({'case': tmp_path.name, 'events': events, 'posts': len(calls),
                        'business_calls': sum(x.call_count for x in business.values())})


def publish(rig):
    tasks, key, *_ = rig
    from xtrek import max_queue_pilot as pilot
    tasks.process_s3_event.apply_async(args=[{'bucket': pilot.BUCKET, 'key': key}])


def test_ack_waits_for_mid(rig):
    tasks, key, behavior, calls, events, release, business = rig
    behavior['mode'] = 'hold'; publish(rig)
    wait_for(lambda: len(calls) == 1)
    assert 'ACK' not in events
    release.set(); wait_for(lambda: 'ACK' in events)
    assert events.index('MID') < events.index('ACK')
    assert not any(x.called for x in business.values())


@pytest.mark.parametrize('mode', ['503', 'missing_mid', 'drop'])
def test_retry_only_notification(rig, mode):
    tasks, key, behavior, calls, events, release, business = rig
    behavior['mode'] = mode; publish(rig)
    wait_for(lambda: 'ACK' in events)
    assert len(calls) == 2
    assert events == ['POST', 'POST', 'MID', 'ACK']
    assert not any(x.called for x in business.values())
    assert 'FORBIDDEN_GET' not in events


def test_strict_no_ack_before_mid_even_on_retry(rig):
    tasks, key, behavior, calls, events, release, business = rig
    behavior['mode'] = '503'; publish(rig)
    wait_for(lambda: 'ACK' in events)
    assert events.index('MID') < events.index('ACK'), events


def test_retry_never_publishes_replacement(rig, monkeypatch):
    tasks, key, behavior, calls, events, release, business = rig
    behavior['mode'] = '503'
    original = tasks.process_s3_event.apply_async
    def fail(*args, **kw):
        raise OSError('injected retry publication failure')
    original(args=[{'bucket': __import__('xtrek.max_queue_pilot', fromlist=['BUCKET']).BUCKET, 'key': key}], countdown=.3)
    monkeypatch.setattr(tasks.process_s3_event, 'apply_async', fail)
    wait_for(lambda: 'ACK' in events)
    assert len(calls) == 2
    assert events.index('MID') < events.index('ACK')
    assert not any(x.called for x in business.values())


def test_exhaustion_preserves_notification(rig, monkeypatch):
    """Acceptance check: a temporary outage must not silently drop the task."""
    from xtrek import max_queue_pilot as pilot
    tasks, key, behavior, calls, events, release, business = rig
    assert pilot.MAX_ATTEMPTS == 5
    behavior['mode'] = 'always503'
    rejected = []
    original = Request.reject
    def reject(req, requeue=False):
        rejected.append(requeue)
        assert 'ACK' not in events
        behavior['mode'] = 'ok'
        return original(req, requeue=requeue)
    monkeypatch.setattr(Request, 'reject', reject)
    publish(rig)
    wait_for(lambda: 'ACK' in events, timeout=45)
    assert len(calls) == 6
    assert rejected == [True]
    assert events == ['POST'] * 6 + ['MID', 'ACK']
    assert not any(x.called for x in business.values())


def test_missing_configuration_retains_event_until_repaired(rig, monkeypatch):
    tasks, key, behavior, calls, events, release, business = rig
    monkeypatch.delenv('XTREK_MAX_PILOT_BOT_TOKEN')
    rejected = []
    original = Request.reject
    def reject(req, requeue=False):
        assert not calls and 'ACK' not in events
        rejected.append(requeue)
        monkeypatch.setenv('XTREK_MAX_PILOT_BOT_TOKEN', 'synthetic-token')
        return original(req, requeue=requeue)
    monkeypatch.setattr(Request, 'reject', reject)
    publish(rig)
    wait_for(lambda: 'ACK' in events)
    assert rejected == [True]
    assert events == ['POST', 'MID', 'ACK']
    assert not any(x.called for x in business.values())


def teardown_module():
    path = os.environ.get('XTREK_TEST_RESULT_PATH')
    if path:
        Path(path).write_text(json.dumps(RESULTS, indent=2))


@pytest.mark.parametrize('rig', ['prefork'], indirect=True)
def test_worker_killed_before_mid_redelivers_original(rig):
    import signal
    tasks, key, behavior, calls, events, release, business = rig
    behavior['mode'] = 'hold'
    publish(rig)
    wait_for(lambda: len(calls) == 1)
    assert 'ACK' not in events
    child = behavior['worker'].pool._pool._pool[0]
    os.kill(child.pid, signal.SIGKILL)
    wait_for(lambda: len(calls) >= 2, timeout=30)
    assert 'ACK' not in events
    release.set()
    wait_for(lambda: 'ACK' in events)
    assert events.index('MID') < events.index('ACK')
