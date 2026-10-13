"""Shared admission for new document sends, never replay of uncertain sends.

One CAS object per endpoint lives outside event prefixes in operation_state_path.
Only a new guarded operation may be a half-open probe. Its own operation guard
remains authoritative if the HTTP outcome is unknown, regardless of this gate.
"""
import hashlib
import json
import logging
import math
import os
import time
from urllib.parse import urlsplit
from uuid import uuid4, UUID

import requests

from .operation_state import ReconciliationRequired, current_operation

logger = logging.getLogger(__name__)
DOCUMENT_PATH = '/api/v3/true-api/lk/documents/create'
DOCUMENT_OPERATIONS = frozenset({
    'send-introduce', 'send-aggregation', 'send-aggregation-set',
    'send-disaggregation', 'send-reaggregation', 'send-cis-information-change',
})


class DocumentCreateDeferred(RuntimeError):
    """Admission failed before any business HTTP request."""
    def __init__(self, next_retry_at, category='circuit_open', now=None):
        self.next_retry_at = float(next_retry_at)
        self.retry_after = max(1, min(300, math.ceil(self.next_retry_at - (time.time() if now is None else now))))
        self.safety_metadata = {'category': category, 'http_status': None,
                                'business_post_started': False}
        super().__init__('Document create deferred before POST')


class DocumentCreateUncertain(ReconciliationRequired):
    """HTTP may have had an effect: reconciliation is required, not retry."""
    def __init__(self, category, status=None):
        self.next_retry_at = None
        self.safety_metadata = {'category': category, 'http_status': status,
                                'business_post_started': True}
        super().__init__('Document create outcome requires reconciliation')


def endpoint(url):
    p = urlsplit(url)
    if p.scheme != 'https' or not p.hostname or p.path != DOCUMENT_PATH or p.query or p.fragment:
        raise ValueError('Unsupported document create endpoint')
    # Never log userinfo, query strings, tokens, payloads or signatures.
    return 'https://' + p.hostname.lower() + (':' + str(p.port) if p.port else '') + DOCUMENT_PATH


def category(response=None):
    text = response.text.lower() if response is not None else ''
    if 'no healthy upstream' in text:
        return 'no_healthy_upstream'
    if 'upstream connect error' in text or 'disconnect/reset before headers' in text:
        return 'upstream_connection_failure'
    return 'http_5xx' if response is not None else 'transport_failure'


def emit(guard, route, state, **fields):
    data = {'event': 'document_create_http', 'state': state, 'endpoint': route}
    if guard is not None:
        data.update(operation=guard.operation,
                    business_key_sha256=hashlib.sha256(guard.business_key.encode()).hexdigest(),
                    attempt_number=guard.value.get('document_attempt_count', 0))
    data.update(fields)
    logger.info('%s', json.dumps(data, sort_keys=True, separators=(',', ':')))


class DocumentCreateCircuit:
    """Bounded shared 5xx backoff; CAS conflicts/storage errors fail closed.

    The threshold is three failures in 60 seconds. Cooldown starts at 30 seconds
    and doubles only on failed half-open probes, capped at 300 seconds. A probe
    reservation expires after 120 seconds; expiry permits a DIFFERENT operation,
    never a replay of the operation whose guard is external/uncertain.
    """
    THRESHOLD = 3
    WINDOW = 60
    BASE_DELAY = 30
    MAX_DELAY = 300
    PROBE_LEASE = 120
    CAS_ATTEMPTS = 6

    def __init__(self, storage, root, route, clock=time.time):
        self.storage, self.route, self.clock = storage, endpoint(route), clock
        digest = hashlib.sha256(self.route.encode()).hexdigest()
        self.path = root.rstrip('/') + '/_document_create/v1/' + digest + '.json'

    def read(self):
        old = self.storage.read_lock_object(self.path)
        if not old:
            return {'version': 1, 'endpoint': self.route, 'failures': [],
                    'next_retry_at': 0, 'generation': 0, 'probe': None, 'seen': []}, None
        data = json.loads(old[0])
        if (data.get('version') != 1 or data.get('endpoint') != self.route
                or not isinstance(data.get('failures'), list) or len(data['failures']) > 32
                or not isinstance(data.get('seen'), list) or len(data['seen']) > 64
                or not isinstance(data.get('generation'), int) or data['generation'] < 0):
            raise ValueError('Invalid document circuit state')
        for value in [data.get('next_retry_at')] + [f.get('at') for f in data['failures']]:
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
                raise ValueError('Invalid document circuit timestamp')
        if data.get('probe') is not None:
            probe = data['probe']
            if (not isinstance(probe, dict) or not isinstance(probe.get('id'), str)
                    or not isinstance(probe.get('until'), (int, float))
                    or not math.isfinite(probe['until'])):
                raise ValueError('Invalid document circuit probe')
        return data, old[1]

    def write(self, data, etag):
        return self.storage.write_lock_object(self.path, json.dumps(data, sort_keys=True), etag)

    def admit(self):
        permit = {'id': str(uuid4()), 'probe': False}
        for _ in range(self.CAS_ATTEMPTS):
            now = self.clock()
            try:
                data, tag = self.read()
                probe_until = (data.get('probe') or {}).get('until', 0)
                blocked_until = max(data['next_retry_at'], probe_until)
                if blocked_until > now:
                    raise DocumentCreateDeferred(blocked_until, now=now)
                if data['next_retry_at']:
                    permit['probe'] = True
                    data['probe'] = {'id': permit['id'], 'until': now + self.PROBE_LEASE}
                data['last_admitted_at'] = now
                if self.write(data, tag) is not None:
                    return permit
            except DocumentCreateDeferred:
                raise
            except Exception:
                raise DocumentCreateDeferred(now + self.BASE_DELAY, 'control_state_unavailable', now=now) from None
        raise DocumentCreateDeferred(self.clock() + self.BASE_DELAY, 'control_state_contention')

    def finish(self, permit, status, failed=False):
        """Control updates cannot erase or authorize a business operation guard."""
        for _ in range(self.CAS_ATTEMPTS):
            now = self.clock()
            data, tag = self.read()
            if permit['id'] in data['seen']:
                return data['next_retry_at']
            data['seen'] = (data['seen'] + [permit['id']])[-64:]
            is_probe = (data.get('probe') or {}).get('id') == permit['id']
            if failed:
                data['failures'] = [f for f in data['failures'] if now - f['at'] <= self.WINDOW]
                data['failures'] = (data['failures'] + [{'at': now, 'status': status}])[-32:]
                if is_probe or (len(data['failures']) >= self.THRESHOLD and not data['next_retry_at']):
                    data['generation'] = min(16, data['generation'] + 1)
                    delay = min(self.MAX_DELAY, self.BASE_DELAY * 2 ** (data['generation'] - 1))
                    data['next_retry_at'] = now + delay
                    data['probe'] = None
            elif is_probe:
                data.update(failures=[], generation=0, next_retry_at=0, probe=None)
            elif not data['next_retry_at']:
                data['failures'] = []
            # An older in-flight success cannot close an already open circuit.
            if self.write(data, tag) is not None:
                return data['next_retry_at']
        raise RuntimeError('Document circuit update contention')


def bind_endpoint(url):
    guard = current_operation()
    if guard is not None and guard.operation in DOCUMENT_OPERATIONS:
        guard.document_create_endpoint = endpoint(url)


def before_send(guard):
    if guard.operation not in DOCUMENT_OPERATIONS:
        return False
    if guard.value.get('phase') != 'held':
        raise ReconciliationRequired('Document create admission is only valid before POST')
    route = getattr(guard, 'document_create_endpoint', None)
    if route is None:
        route = endpoint(os.getenv('TRUE_API_HOST', 'https://markirovka.crpt.ru').rstrip('/') + DOCUMENT_PATH)
        guard.document_create_endpoint = route
    circuit = DocumentCreateCircuit(guard.storage, guard.root, route)
    permit = circuit.admit()
    number = guard.value.get('document_attempt_count', 0) + 1
    attempt = {'number': number, 'endpoint': route, 'method': 'POST',
               'started_at': time.time(), 'outcome': 'inflight'}
    guard.update('external', document_attempt_count=number, http_attempt=attempt,
                 last_error=None, next_retry_at=None)
    guard.document_create_circuit, guard.document_create_permit = circuit, permit
    return True


def send_document(url, headers, body, pg):
    route = endpoint(url)
    guard = current_operation()
    circuit = permit = None
    if guard is not None:
        if (guard.operation not in DOCUMENT_OPERATIONS or guard.value.get('phase') != 'external'
                or not getattr(guard, 'document_create_permit', None)
                or getattr(guard, 'document_create_endpoint', None) != route
                or getattr(guard, 'document_create_transport_used', False)):
            raise ReconciliationRequired('Document create requires guarded admission before POST')
        circuit, permit = guard.document_create_circuit, guard.document_create_permit
        guard.document_create_transport_used = True
    started = time.monotonic()
    emit(guard, route, 'start', method='POST')
    logger.info('Отправка документа в ЛК...')
    response = None
    try:
        # No automatic POST retries or redirect-based resubmission.
        response = requests.post(url, data=body.encode('utf-8'), params={'pg': pg},
                                 headers=headers, verify=False, timeout=(10, 30),
                                 allow_redirects=False)
    except Exception:
        status, failure = None, 'transport_failure'
    else:
        status = response.status_code
        failure = category(response) if status >= 500 else None
    elapsed = round(time.monotonic() - started, 6)
    if guard is not None:
        attempt = dict(guard.value['http_attempt'], finished_at=time.time(),
                       http_status=status, elapsed_seconds=elapsed,
                       outcome='uncertain' if failure else 'response_received')
        try:
            guard.update(guard.value['phase'], http_attempt=attempt)
        except Exception:
            emit(guard, route, 'attempt_checkpoint_failed', http_status=status)
            # Preserve the HTTP reply for the workflow's critical accepted-result
            # persistence. Failure of observational metadata must not discard it.
    retry_at = None
    if circuit:
        try:
            retry_at = circuit.finish(permit, status, failed=bool(failure))
        except Exception:
            # HTTP already happened. A control-state failure must never become
            # a pre-POST deferral or discard an otherwise successful response.
            emit(guard, route, 'control_update_failed', http_status=status)
    correlation = {}
    if response is not None:
        for name in ('X-Request-ID', 'X-Correlation-ID'):
            value = getattr(response, 'headers', {}).get(name)
            try:
                if (value and str(UUID(value)) == value.lower() and value not in body
                        and value.lower() not in str(headers.get('Authorization', '')).lower()):
                    correlation[name.lower()] = value.lower()
            except (ValueError, AttributeError, TypeError):
                pass
    emit(guard, route, 'uncertain' if failure else 'response', method='POST',
         http_status=status, elapsed_seconds=elapsed, failure_category=failure,
         route_next_retry_at=retry_at, correlation_ids=correlation)
    if failure:
        raise DocumentCreateUncertain(failure, status) from None
    if status == 429:
        response.raise_for_status()
    if status < 200 or status >= 300:
        # Rejected/redirect responses are still fail-closed at the operation
        # layer. Never expose a response body or follow a redirect with POST.
        raise DocumentCreateUncertain('http_rejected_or_redirect', status)
    try:
        return response.json()
    except ValueError:
        return {'document_id': response.text.strip('"')}
