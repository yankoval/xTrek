"""Authenticated notification intake; shares the scanner's durable outbox.

No business work or AMQP connections take place in this service. Public TLS is
terminated by a reverse proxy; this server deliberately binds only to loopback.
"""
import hashlib
import hmac
import json
import logging
import os
import re
import shlex
import threading
import time
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from .s3_poller import (EventLedger, INPUT_BUCKET, INTERNAL_BUCKET, configured_routes,
                        eligible_object, event_identity, source_inn_allowed)

LOG = logging.getLogger(__name__)
PATH = '/xtrek/s3-events'
MAX_BODY = 65536


def signature(secret, timestamp, body):
    return hmac.new(secret, timestamp.encode('ascii') + b'\n' + body, hashlib.sha256).hexdigest()


def authenticate(secret, timestamp, supplied, body, now=None):
    if not re.fullmatch(r'[0-9]{10,12}', timestamp or ''):
        return False
    if abs((time.time() if now is None else now) - int(timestamp)) > 300:
        return False
    if not re.fullmatch(r'[0-9a-f]{64}', supplied or ''):
        return False
    return hmac.compare_digest(signature(secret, timestamp, body), supplied)


def read_cutoff(env):
    # Read the scanner's actual persistent setting on each request, so the next
    # manual cutoff change does not leave a long-running receiver at an old date.
    path = env.get('XTREK_S3_ADMISSION_ENV_FILE')
    value = env.get('XTREK_S3_CUTOFF_UTC')
    if path:
        values = []
        for line in Path(path).read_text(encoding='utf-8').splitlines():
            if line.strip().startswith('XTREK_S3_CUTOFF_UTC='):
                words = shlex.split(line.strip(), comments=True)
                if len(words) != 1:
                    raise ValueError('Invalid cutoff setting')
                values.append(words[0].split('=', 1)[1])
        if len(values) != 1:
            raise ValueError('Missing or ambiguous cutoff setting')
        value = values[0]
    cutoff = datetime.fromisoformat((value or '').replace('Z', '+00:00'))
    if cutoff.tzinfo is None:
        raise ValueError('Cutoff requires timezone')
    return cutoff


class Intake:
    def __init__(self, s3, env, ledger_factory=EventLedger):
        self.s3, self.env, self.ledger_factory = s3, dict(env), ledger_factory
        self.input_bucket = env.get('XTREK_S3_INPUT_BUCKET', INPUT_BUCKET)
        self.prefix = env.get('XTREK_S3_EVENT_PREFIX', '')
        self.allowed_inns = frozenset(x.strip() for x in env.get('XTREK_LOCAL_ALLOWED_INNS', '').split(',') if x.strip())
        if not self.allowed_inns:
            raise ValueError('Explicit signer allowlist required')
        supported = configured_routes(self.input_bucket, env.get('XTREK_S3_INTERNAL_BUCKET', INTERNAL_BUCKET), self.prefix)
        self.routes = json.loads(env['XTREK_S3_INGRESS_ROUTES_JSON'])
        if not isinstance(self.routes, dict) or not self.routes:
            raise ValueError('Explicit ingress routes required')
        for bucket, prefixes in self.routes.items():
            if not isinstance(prefixes, list) or not prefixes or any(p not in supported.get(bucket, ()) for p in prefixes):
                raise ValueError('Ingress route outside scanner routes')
        self.db_path = env.get('XTREK_S3_POLL_LEDGER', '/var/lib/xtrek/s3-poller/outbox.sqlite3')
        # Never create an accidentally empty production outbox on a wrong path.
        if not Path(self.db_path).is_file():
            raise ValueError('Existing scanner outbox required')

    def receive(self, items):
        if not isinstance(items, list) or not 1 <= len(items) <= 10:
            raise ValueError('Expected 1..10 notifications')
        for item in items:
            if (not isinstance(item, dict) or set(item) != {'bucket', 'key'}
                    or not all(isinstance(item[k], str) and 0 < len(item[k]) <= 2048 for k in item)
                    or any(ord(c) < 32 for c in item['bucket'] + item['key'])):
                raise ValueError('Invalid notification')
        cutoff = read_cutoff(self.env)
        allowlist = frozenset()
        if self.env.get('XTREK_S3_BACKFILL_ALLOWLIST'):
            with open(self.env['XTREK_S3_BACKFILL_ALLOWLIST'], encoding='utf-8') as f:
                allowlist = frozenset((r['bucket'], r['key']) for r in map(json.loads, f) if r.get('bucket') and r.get('key'))
        counts = dict(registered=0, known=0, ignored=0)
        ledger = self.ledger_factory(self.db_path)
        try:
            for item in items:
                bucket, key = item['bucket'], item['key']
                if not any(key.startswith(p) for p in self.routes.get(bucket, [])) or key.endswith('/'):
                    counts['ignored'] += 1
                    continue
                obj = self._metadata(bucket, key)
                if not eligible_object(bucket, key, obj, cutoff, allowlist):
                    counts['ignored'] += 1
                    continue
                eid = event_identity(bucket, key, obj, ())
                if ledger.known(eid):
                    counts['known'] += 1
                elif source_inn_allowed(self.s3, bucket, key, obj, input_bucket=self.input_bucket,
                                        event_prefix=self.prefix, allowed_inns=self.allowed_inns):
                    counts['registered' if ledger.enqueue(eid, bucket, key) else 'known'] += 1
                else:
                    counts['ignored'] += 1
        finally:
            ledger.close()
        return counts

    def _metadata(self, bucket, key):
        token = None
        for _ in range(4):
            args = dict(Bucket=bucket, Prefix=key, MaxKeys=1000)
            if token:
                args['ContinuationToken'] = token
            page = self.s3.list_objects_v2(**args)
            for obj in page.get('Contents', []):
                if obj['Key'] == key:
                    if any(obj.get(k) is None for k in ['ETag', 'Size', 'LastModified']):
                        raise RuntimeError('Incomplete source metadata')
                    return obj
            if not page.get('IsTruncated'):
                break
            next_token = page.get('NextContinuationToken')
            if not next_token or next_token == token:
                raise RuntimeError('Invalid metadata pagination')
            token = next_token
        # Do not acknowledge transiently unavailable objects as successfully
        # registered: the function retries and the scanner remains the fallback.
        raise RuntimeError('Source metadata unavailable')


def create_handler(intake, secret, capacity=None):
    capacity = capacity or threading.BoundedSemaphore(4)

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, fmt, *args):
            pass  # Never log URL/body/signature or credentials.

        def _reply(self, status, value):
            body = json.dumps(value).encode()
            self.send_response(status)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(body)))
            self.send_header('Connection', 'close')
            self.end_headers()
            self.close_connection = True
            self.wfile.write(body)

        def do_POST(self):
            self.connection.settimeout(10)
            if self.path != PATH:
                return self._reply(404, {'error': 'not_found'})
            if self.headers.get('Transfer-Encoding') or self.headers.get_content_type() != 'application/json':
                return self._reply(400, {'error': 'invalid_request'})
            try:
                length = int(self.headers.get('Content-Length', '-1'))
            except ValueError:
                length = -1
            if not 0 < length <= MAX_BODY:
                return self._reply(413, {'error': 'invalid_length'})
            try:
                body = self.rfile.read(length)
            except (OSError, TimeoutError):
                return self._reply(408, {'error': 'read_timeout'})
            if len(body) != length:
                return self._reply(400, {'error': 'incomplete_body'})
            if not authenticate(secret, self.headers.get('X-Xtrek-Timestamp'), self.headers.get('X-Xtrek-Signature'), body):
                return self._reply(401, {'error': 'unauthorized'})
            try:
                data = json.loads(body)
                if not isinstance(data, dict) or set(data) != {'events'}:
                    raise ValueError('Invalid envelope')
            except (ValueError, UnicodeError):
                return self._reply(400, {'error': 'invalid_json'})
            if not capacity.acquire(blocking=False):
                return self._reply(503, {'error': 'busy'})
            try:
                # A successful response means every item was durably admitted,
                # already known, or deliberately excluded by the shared policy.
                result = intake.receive(data['events'])
            except Exception as exc:
                LOG.warning('S3 ingress request failed error_type=%s', type(exc).__name__)
                return self._reply(503, {'error': 'registration_failed'})
            finally:
                capacity.release()
            LOG.info('S3 ingress accepted %s', json.dumps(result, sort_keys=True))
            self._reply(202, result)
    return Handler


def main():
    import boto3
    from botocore.config import Config
    env = dict(os.environ)
    secret = Path(env['XTREK_S3_INGRESS_SECRET_FILE']).read_bytes().strip()
    if len(secret) < 32:
        raise SystemExit('Ingress secret must contain at least 32 bytes')
    s3 = boto3.client('s3', endpoint_url=env.get('XTREK_S3_ENDPOINT_URL', 'https://storage.yandexcloud.net'),
                      region_name=env.get('XTREK_S3_REGION', 'ru-central1'),
                      aws_access_key_id=env['XTREK_S3_ACCESS_KEY'], aws_secret_access_key=env['XTREK_S3_SECRET_KEY'],
                      config=Config(connect_timeout=5, read_timeout=10, retries={'total_max_attempts': 1}))
    intake = Intake(s3, env)
    read_cutoff(env)  # Fail closed before listening on invalid configuration.
    logging.basicConfig(level=logging.INFO)
    server = ThreadingHTTPServer(('127.0.0.1', int(env.get('XTREK_S3_INGRESS_PORT', '18981'))), create_handler(intake, secret))
    server.serve_forever()


if __name__ == '__main__':
    main()
