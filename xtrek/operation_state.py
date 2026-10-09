"""Durable per-business-operation CAS state; never steal a lock by its age.

Enabled by operation_state_path, outside all business event prefixes. External
requests reserve their state BEFORE sending. A lost reply requires reconciliation,
not another POST. Completed results survive acknowledgement loss and worker death.
"""
from contextvars import ContextVar
from dataclasses import asdict, is_dataclass
from functools import wraps
import hashlib
import inspect
import json
import logging
import time
from uuid import uuid4

from .storage import get_storage
from .token_master_lock import owner_identity, owner_is_dead

logger = logging.getLogger(__name__)
_current = ContextVar('xtrek_operation', default=None)


class OperationBusy(RuntimeError):
    pass


class OperationConflict(RuntimeError):
    pass


class ReconciliationRequired(RuntimeError):
    pass


def fingerprint(text):
    # Ignore JSON indentation/key order, preserve business values and code bytes.
    value = json.loads(text)
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True,
                                    separators=(',', ':')).encode()).hexdigest()


def encode_result(value):
    if is_dataclass(value):
        return {'model': type(value).__name__, 'value': asdict(value)}
    return {'model': None, 'value': value}


def decode_result(result):
    if result['model']:
        from . import suz_api_models
        model = getattr(suz_api_models, result['model'], None)
        if model is None:
            raise OperationConflict('Unsupported saved operation result model')
        value = result['value']
        if result['model'] == 'ProductionOrder' and isinstance(value.get('PasportData'), dict):
            value = dict(value, PasportData=suz_api_models.PasportData(**value['PasportData']))
        return model(**value)
    return result['value']


class OperationState:
    def __init__(self, storage, root, operation, business_key, input_hash=None):
        self.storage = storage
        digest = hashlib.sha256(str(business_key).encode()).hexdigest()
        self.path = '{}/{}/{}.json'.format(root.rstrip('/'), operation, digest)
        self.operation, self.business_key = operation, str(business_key)
        self.input_hash = input_hash
        self.owner = owner_identity()
        self.owner_id = str(uuid4())
        self.value = self.etag = None
        self.cached = False

    def _persist(self, value, etag):
        text = json.dumps(value)
        try:
            return self.storage.write_lock_object(self.path, text, etag)
        except Exception:
            # PUT may have succeeded even if its response was lost. Read back
            # this exact owner/phase/result before adopting it; never guess.
            try:
                stored = self.storage.read_lock_object(self.path)
                if stored and json.loads(stored[0]) == value:
                    return stored[1]
            except Exception:
                pass
            raise

    def acquire(self):
        stored = self.storage.read_lock_object(self.path)
        previous, etag = (json.loads(stored[0]), stored[1]) if stored else (None, None)
        if previous:
            if (previous.get('version') != 1 or previous.get('operation') != self.operation
                    or previous.get('business_key') != self.business_key
                    or previous.get('input_hash') != self.input_hash):
                raise OperationConflict('Operation identity/input conflict: ' + self.path)
            phase = previous.get('phase')
            if phase == 'complete':
                self.value, self.etag, self.cached = previous, etag, True
                return decode_result(previous['result'])
            if phase in {'external', 'uncertain', 'accepted'}:
                # Accepted results may be persisted safely without repeating the API.
                if phase == 'accepted':
                    if (not previous.get('replay_allowed')
                            and not owner_is_dead(previous.get('owner'), self.owner)
                            and time.time() - previous.get('updated_at', 0) < 120):
                        raise OperationBusy('Accepted result is being persisted: ' + self.path)
                    self.value, self.etag, self.cached = previous, etag, True
                    return decode_result(previous['result'])
                if (phase == 'external' and not owner_is_dead(previous.get('owner'), self.owner)
                        and time.time() - previous.get('updated_at', 0) < 120):
                    # A duplicate can arrive while the first request is still
                    # in flight. Retry later; this does not grant ownership.
                    raise OperationBusy('External request still in progress: ' + self.path)
                raise ReconciliationRequired('External outcome requires reconciliation: ' + self.path)
            if phase not in {'ready', 'held'}:
                raise OperationConflict('Unknown operation state: ' + self.path)
            if phase == 'held' and not owner_is_dead(previous.get('owner'), self.owner):
                raise OperationBusy('Operation owned by a live/unknown worker: ' + self.path)
        self.value = {'version': 1, 'operation': self.operation,
                      'business_key': self.business_key, 'input_hash': self.input_hash,
                      'owner_id': self.owner_id, 'owner': self.owner,
                      'phase': 'held', 'updated_at': time.time()}
        self.etag = self._persist(self.value, etag)
        if self.etag is None:
            raise OperationBusy('Another worker won operation CAS: ' + self.path)
        logger.info('[OPERATION] claimed operation=%s key=%s owner=%s',
                    self.operation, self.business_key, self.owner_id)

    def update(self, phase, result=None):
        if not self.value or self.value.get('owner_id') != self.owner_id or self.cached:
            raise OperationBusy('Operation ownership changed: ' + self.path)
        value = dict(self.value, phase=phase, updated_at=time.time())
        if result is not None or phase == 'complete':
            value['result'] = encode_result(result)
        # The prior ETag proves ownership. IfMatch rejects any newer owner;
        # another GET before this PUT adds cost without improving that proof.
        tag = self._persist(value, self.etag)
        if tag is None:
            raise OperationBusy('Operation state CAS failed: ' + self.path)
        self.value, self.etag = value, tag

    def external(self):
        self.update('external')

    def accepted(self, result):
        self.update('accepted', result)

    def finish(self, result):
        self.update('complete', result)
        logger.info('[OPERATION] complete operation=%s key=%s owner=%s',
                    self.operation, self.business_key, self.owner_id)

    def release_safe(self):
        if self.value and self.value['phase'] == 'held':
            self.update('ready')

    def failed(self, error):
        if self.value and self.value['phase'] == 'accepted':
            self.value['replay_allowed'] = True
            self.update('accepted')
        if self.value and self.value['phase'] == 'external':
            response = getattr(error, 'response', None)
            if response is not None and getattr(response, 'status_code', None) == 429:
                # Explicit rate rejection accepted no document. Network/5xx
                # failures remain uncertain and are never blindly repeated.
                self.update('held')
            else:
                self.update('uncertain')
        self.release_safe()


def before_external_request():
    guard = _current.get()
    if guard:
        guard.external()


def remember_external_result(result):
    guard = _current.get()
    if guard:
        guard.accepted(result)


def guarded(operation, *, input_path=None, resume=None, cache_none=False, config_loader=None,
            success=None):
    """Guard a function by its first business argument; optional safe result replay.

    resume receives a confirmed saved result, never sends it to the external API.
    Settings must be identical on every worker; rollout never mixes old/new code.
    """
    def decorate(function):
        @wraps(function)
        def call(*args, **kwargs):
            from .config_loader import load_config
            config = (config_loader or load_config)('suz_worker_config')
            root = config.get('operation_state_path')
            if not root:
                return function(*args, **kwargs)
            bound = inspect.signature(function).bind(*args, **kwargs)
            bound.apply_defaults()
            business_key = bound.arguments[next(iter(inspect.signature(function).parameters))]
            source = input_path(config, business_key) if input_path else None
            input_hash = None
            if source:
                input_hash = fingerprint(get_storage(source, config.get('s3_config')).read_text(source))
            parameters = {name: bound.arguments[name] for name in
                          ('qty', 'group', 'contact', 'production_date', 'expiration_date',
                           'inn_override', 'participant_inn', 'ignore_duplicate', 'vdf_template_name',
                           'oms_id', 'client_token')
                          if name in bound.arguments}
            if parameters:
                input_hash = fingerprint(json.dumps({'source': input_hash, 'parameters': parameters}))
            guard = OperationState(get_storage(root, config.get('s3_config')), root,
                                   operation, business_key, input_hash)
            result = guard.acquire()
            if guard.cached:
                if resume and guard.value['phase'] == 'accepted':
                    result = resume(config, business_key, result)
                return result
            token = _current.set(guard)
            try:
                result = function(*args, **kwargs)
                if (result is not None or cache_none) and (success is None or success(result)):
                    guard.finish(result)
                else:
                    guard.release_safe()
                return result
            except BaseException as error:
                guard.failed(error)
                raise
            finally:
                _current.reset(token)
        return call
    return decorate


def publish_once(storage, path, text):
    """Create an immutable result; a conflicting existing object is an error.

    Use plain PUT with IfNoneMatch, not upload_file's multipart/background transfer.
    Coordination objects and immutable business results use different prefixes.
    """
    if storage.write_lock_object(path, text, None) is not None:
        return
    stored = storage.read_text(path)
    if fingerprint(stored) != fingerprint(text):
        raise OperationConflict('Existing output conflicts with this operation: ' + path)
