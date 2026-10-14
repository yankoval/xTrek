"""Managed PRNSRV polling. Retry reads; never replay admitted business effects."""
from __future__ import annotations

import copy
import fcntl
import hashlib
import json
import logging
import math
import os
from pathlib import Path
import random
import signal
import sqlite3
import sys
import time
from uuid import UUID, uuid4

from botocore.exceptions import (ClientError, EndpointConnectionError,
                                ConnectionClosedError, ConnectTimeoutError,
                                ReadTimeoutError)

BUCKET = '1bf11148-3595-4a07-a089-d460153b7c7a'
PROTECTED = {'623c20fa-f90c-4478-aeae-9c6635cedc9f'}
ALLOWED_INNS = {'7733154124', '9718180660', '9723161905'}
LOG = logging.getLogger(__name__)


class FatalSupervisorError(RuntimeError):
    """Identity/storage invariant failure: exit 78 prevents automatic restart."""


class ReadUnavailable(RuntimeError):
    pass


def transient_read(error):
    if isinstance(error, (EndpointConnectionError, ConnectionClosedError,
                          ConnectTimeoutError, ReadTimeoutError)):
        return True
    if isinstance(error, ClientError):
        return error.response.get('ResponseMetadata', {}).get('HTTPStatusCode') in {
            408, 429, 500, 502, 503, 504}
    return False


def safe_error(error):
    """Structured traceback only; exception text/source lines/locals are excluded."""
    known = {'FatalSupervisorError', 'ReadUnavailable', 'RuntimeError', 'ValueError',
             'TypeError', 'OSError', 'PermissionError', 'FileNotFoundError',
             'ClientError', 'EndpointConnectionError', 'ConnectionClosedError',
             'ConnectTimeoutError', 'ReadTimeoutError', 'InputValidationError',
             'ImmutableObjectConflict', 'ReconciliationRequired', 'AssertionError'}
    name = type(error).__name__
    frames, tb = [], error.__traceback__
    while tb is not None and len(frames) < 24:
        code = tb.tb_frame.f_code
        module = str(tb.tb_frame.f_globals.get('__name__', ''))
        if module.startswith(('xtrek.', 'botocore.', 'urllib3.', 'requests.', 'prnsrv.')):
            frames.append({'module': module, 'function': code.co_name, 'line': tb.tb_lineno})
        else:
            frames.append({'module': 'external', 'line': tb.tb_lineno})
        tb = tb.tb_next
    status = None
    if isinstance(error, ClientError):
        candidate = error.response.get('ResponseMetadata', {}).get('HTTPStatusCode')
        if type(candidate) is int and 100 <= candidate <= 599:
            status = candidate
    return {'error_type': name if name in known else 'UnclassifiedError',
            'http_status': status, 'traceback': frames}


def atomic_json(path, value):
    """0600, fsync body and directory; a failed checkpoint closes admission."""
    path = Path(path)
    tmp = path.with_name(path.name + '.' + uuid4().hex + '.tmp')
    try:
        fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, 'w') as stream:
            json.dump(value, stream, ensure_ascii=False, sort_keys=True)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(tmp, path)
        directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    except OSError:
        raise FatalSupervisorError('Durable checkpoint unavailable') from None
    finally:
        try:
            tmp.unlink(missing_ok=True)
        except OSError:
            pass


class Evidence:
    def __init__(self, root, main, *, clock=time.time, rotate_bytes=10*1024*1024):
        self.root, self.clock, self.rotate_bytes = Path(root), clock, rotate_bytes
        self.path = self.root / 'health.json'
        # Health is observational; do not copy a previous process's live status.
        previous = {}
        if self.path.exists():
            try:
                old = json.loads(self.path.read_text())
                for name in ('last_full_scan_success', 'last_progress', 'last_cycle_success'):
                    value = old.get(name)
                    if type(value) in (int,float) and math.isfinite(value):
                        previous[name] = value
            except (OSError,ValueError,TypeError):
                pass
        self.value = {'version': 1, 'main': main, 'pid': os.getpid(),
                      'owner_id': str(uuid4()), 'started_at': clock(),
                      'phase': 'STARTING', 'last_full_scan_success': None,
                      'last_progress': None, 'last_error': None, 'failures': 0,
                      'next_retry_at': None, 'current_uuid': None}
        boot = Path('/proc/sys/kernel/random/boot_id')
        self.value['boot_id'] = boot.read_text().strip() if boot.exists() else None
        self.value.update(previous)
        self.update('STARTING')

    def update(self, phase, **fields):
        self.value.update(fields, phase=phase, heartbeat_at=self.clock())
        atomic_json(self.path, self.value)

    def event(self, kind, **fields):
        row = {'at': self.clock(), 'kind': kind, 'main': self.value['main'],
               'pid': self.value['pid'], **fields}
        path = self.root / 'events.jsonl'
        try:
            if path.exists() and path.stat().st_size >= self.rotate_bytes:
                for i in range(4, 0, -1):
                    source = path.with_name(path.name + '.' + str(i))
                    if source.exists():
                        os.replace(source, path.with_name(path.name + '.' + str(i+1)))
                os.replace(path, path.with_name(path.name + '.1'))
            fd = os.open(path, os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o600)
            with os.fdopen(fd, 'a') as stream:
                stream.write(json.dumps(row, sort_keys=True, separators=(',', ':')) + '\n')
                stream.flush()
                os.fsync(stream.fileno())
            directory = os.open(self.root, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
        except OSError:
            raise FatalSupervisorError('Durable evidence unavailable') from None
        LOG.info('%s', json.dumps(row, sort_keys=True, separators=(',', ':')))

    def error(self, error, phase, uid=None):
        value = {'at': self.clock(), 'operation': phase, 'uuid': uid, **safe_error(error)}
        self.update(phase, last_error=value)
        self.event('ERROR', **value)


class Supervisor:
    """Single owner required externally. Adapter business methods are called once."""
    def __init__(self, root, main, adapter, *, check_release=lambda: None,
                 clock=time.time, monotonic=time.monotonic, sleep=time.sleep,
                 jitter=random.random):
        self.root, self.main, self.adapter = Path(root), main, adapter
        self.clock, self.monotonic, self.sleep, self.jitter = clock, monotonic, sleep, jitter
        self.check_release = check_release
        self.path = self.root / 'state.json'
        try:
            self.state = json.loads(self.path.read_text())
            if (self.state.get('main') != main or not isinstance(self.state.get('jobs'), dict)
                    or not isinstance(self.state.get('baseline_keys'), list)):
                raise ValueError
            from datetime import datetime
            self.cutoff = datetime.fromisoformat(self.state['cutoff'])
            if self.cutoff.tzinfo is None:
                raise ValueError
            for uid, job in self.state['jobs'].items():
                if str(UUID(uid)) != uid or job.get('state') not in {'STAGED','SKIPPED','BLOCKED','ADMITTED'}:
                    raise ValueError
        except (OSError, ValueError, KeyError, TypeError):
            raise FatalSupervisorError('Invalid existing supervisor state') from None
        self.baseline = set(self.state['baseline_keys'])
        self.evidence = Evidence(self.root, main, clock=clock)
        self.stop_requested = False
        self.failures = 0
        self.quarantined = [u for u,j in self.state['jobs'].items() if j['state']=='ADMITTED']
        self.evidence.update('READY', quarantined_admitted=len(self.quarantined))
        self.evidence.event('READY', quarantined_admitted=len(self.quarantined))

    def stop(self, *_):
        # Signal handler does not write files or interrupt a business operation.
        self.stop_requested = True

    def wait(self, seconds):
        deadline = self.monotonic() + seconds
        while not self.stop_requested and self.monotonic() < deadline:
            self.sleep(min(1, deadline-self.monotonic()))

    def read(self, function, phase, uid=None):
        # One application retry layer, shared 120-second budget per read sequence.
        deadline = self.monotonic() + 120
        for attempt in range(5):
            try:
                if self.monotonic() >= deadline:
                    raise ReadUnavailable('Read deadline exceeded')
                value = function(deadline)
                if self.monotonic() > deadline:
                    raise ReadUnavailable('Read deadline exceeded')
                return value
            except Exception as error:
                if not transient_read(error):
                    raise
                self.evidence.error(error, phase, uid)
                if attempt == 4 or self.monotonic() >= deadline or self.stop_requested:
                    raise ReadUnavailable('Read retry budget exhausted') from None
                delay = min(16, 2**(attempt+1)) * (0.8 + 0.4*self.jitter())
                self.evidence.update('RETRY_READ', next_retry_at=self.clock()+delay,
                                     current_uuid=uid)
                self.wait(min(delay, max(0, deadline-self.monotonic())))
        raise ReadUnavailable('Read retry budget exhausted')

    def save(self):
        atomic_json(self.path, self.state)

    def process_new(self, key, uid):
        source_hash = None
        try:
            source = self.read(lambda deadline: self.adapter.read_source(key, deadline), 'GET_SOURCE', uid)
            source_hash, inn, count = self.adapter.describe(source)
            if count is None or count <= 0:
                self.state['jobs'][uid] = {'state':'SKIPPED','reason':'quantity_not_positive',
                                           'source_hash':source_hash}
                self.save()
                self.evidence.event('SKIPPED', uuid=uid)
                return
            self.read(lambda deadline: self.adapter.check_history(uid, source_hash, inn, deadline),
                      'GET_HISTORY', uid)
        except ReadUnavailable:
            # Nothing admitted/allocated: leave the source untouched for a later scan.
            self.evidence.event('DEFERRED_SOURCE', uuid=uid)
            raise
        except FatalSupervisorError:
            raise
        except Exception as error:
            if isinstance(error, ClientError) and error.response.get('ResponseMetadata',{}).get('HTTPStatusCode') in {401,403}:
                raise FatalSupervisorError('S3 authorization rejected') from None
            self.evidence.error(error, 'VALIDATING', uid)
            self.state['jobs'][uid] = {'state':'BLOCKED','source_hash':source_hash,
                                      'error_type':safe_error(error)['error_type']}
            self.save()
            return
        if self.stop_requested:
            return
        # Persist BEFORE invoking allocator/generator. A crash leaves ADMITTED;
        # any restarted process skips it without touching the business route.
        self.state['jobs'][uid] = {'state':'ADMITTED','source_hash':source_hash,
                                   'quantity':count,'inn':inn,'key':key}
        self.save()
        self.evidence.update('PROCESSING', current_uuid=uid, last_progress=self.clock())
        try:
            self.adapter.generate(key, uid, source_hash)
            self.evidence.update('STAGING', last_progress=self.clock())
            receipt = self.adapter.stage(uid)
        except FatalSupervisorError:
            raise
        except Exception as error:
            # Includes reads AFTER admission: do not retry an unknown external result.
            self.evidence.error(error, 'BLOCKED_OPERATION', uid)
            self.state['jobs'][uid].update(state='BLOCKED', error_type=safe_error(error)['error_type'])
            self.save()
            return
        self.state['jobs'][uid].update(state='STAGED', receipt_key=receipt)
        self.save()
        self.evidence.event('STAGED', uuid=uid, quantity=count)
        self.evidence.update('PROCESSING', last_progress=self.clock(), current_uuid=None)

    def degraded(self):
        self.failures += 1
        delay = min(300, 30*2**min(self.failures-1,4)) * (0.8+0.2*self.jitter())
        delay = min(300, delay)
        self.evidence.update('DEGRADED', failures=self.failures,
                             next_retry_at=self.clock()+delay)
        self.evidence.event('DEGRADED', failures=self.failures, retry_seconds=delay)
        return delay

    def cycle(self):
        self.check_release()
        self.evidence.update('SCANNING', last_loop_start=self.clock(), current_uuid=None)
        try:
            objects = self.read(self.adapter.list_sources, 'LIST')
        except ReadUnavailable:
            return self.degraded()
        except Exception as error:
            self.evidence.error(error, 'FATAL')
            raise FatalSupervisorError('Non-retryable scan failure') from None
        self.evidence.update('PROCESSING', last_full_scan_success=self.clock(),
                             next_retry_at=None, discovered_objects=len(objects))
        for obj in objects:
            if self.stop_requested:
                break
            key = obj['Key']
            if key in self.baseline or obj['LastModified'] <= self.cutoff:
                continue
            uid = key.removeprefix('Задания/').removesuffix('.json')
            try:
                if str(UUID(uid)) != uid:
                    continue
            except ValueError:
                continue
            if uid in PROTECTED or uid in self.state['jobs']:
                continue
            try:
                self.process_new(key, uid)
            except ReadUnavailable:
                return self.degraded()
        self.failures = 0
        counts = {}
        for job in self.state['jobs'].values():
            counts[job['state']] = counts.get(job['state'],0)+1
        self.evidence.update('IDLE', last_cycle_success=self.clock(), current_uuid=None,
                             next_retry_at=None, failures=0, job_states=counts)
        return 30

    def run(self, once=False):
        while not self.stop_requested:
            delay = self.cycle()
            if once:
                break
            self.wait(delay)
        self.evidence.update('STOPPED_SAFE', current_uuid=None)


class ProductionAdapter:
    def __init__(self, runtime):
        import boto3
        from botocore.config import Config
        from .prnsrv_generator_runtime.storage import S3Storage
        self.runtime = runtime
        if not all(runtime.get('s3_config',{}).get(n)for n in ('aws_access_key_id','aws_secret_access_key')):
            raise FatalSupervisorError('Explicit S3 credentials required')
        self.client = boto3.client('s3', **runtime['s3_config'], config=Config(
            connect_timeout=5, read_timeout=15, retries={'total_max_attempts':1}))
        self.storage = S3Storage(self.client)
        self.db = sqlite3.connect('file:/var/lib/xtrek-sscc/ledger.sqlite3?mode=ro', uri=True)
        self.route = {'url':'https://127.0.0.1:9443','prefix':runtime['sscc_prefix'],
                      'client_cert':os.environ['XTREK_SSCC_CLIENT_CERT'],
                      'client_key':os.environ['XTREK_SSCC_CLIENT_KEY'],
                      'ca_bundle':os.environ['XTREK_SSCC_CA']}

    def list_sources(self, deadline):
        result = []
        for page in self.client.get_paginator('list_objects_v2').paginate(Bucket=BUCKET,Prefix='Задания/'):
            if time.monotonic() > deadline:
                raise ReadTimeoutError(endpoint_url='object_storage')
            result.extend(o for o in page.get('Contents',[]) if o['Key'].endswith('.json'))
        return result

    def read_source(self, key, deadline):
        if time.monotonic() >= deadline:
            raise ReadTimeoutError(endpoint_url='object_storage')
        return self.storage.get_json(BUCKET,key)

    def describe(self, source):
        from prnsrv import canonical_source_hash, classify_count
        inn = str((source.get('PasportData')or{}).get('Manufacturer_inn')or'')
        if inn not in ALLOWED_INNS:
            # Preserve the legacy allowance for notification-only sources.
            count = classify_count(source).count
            if count and count > 0:
                raise ValueError('Unconfigured manufacturer')
        return canonical_source_hash(source), inn, classify_count(source).count

    def check_history(self, uid, source_hash, inn, deadline):
        from .prnsrv_local_allocator import LocalPrnsrvAllocator
        candidates = ['_prnsrv/done/'+uid+'.done','printer-tasks/'+uid+'.csv',
                      'printer-task-stage-receipts/'+uid+'.json']
        candidates += ['printer-tasks/'+i+'_'+uid+'.vdf' for i in ALLOWED_INNS]
        for key in candidates:
            if time.monotonic() >= deadline:
                raise ReadTimeoutError(endpoint_url='object_storage')
            if self.storage.head_optional(BUCKET,key) is not None:
                raise ValueError('Existing artifacts require reconciliation')
        request = LocalPrnsrvAllocator.request_id(uid, source_hash)
        if self.db.execute('SELECT 1 FROM allocations WHERE request_id=?',(request,)).fetchone():
            raise ValueError('Existing allocation requires reconciliation')

    def generate(self, key, uid, source_hash):
        from .prnsrv_local_runtime import process_local_prnsrv_s3_event
        cfg = copy.deepcopy(self.runtime)
        cfg['prnsrv_local_generator_enabled'] = True
        cfg['prnsrv_local_allocator_by_inn'] = {i:dict(self.route)for i in ALLOWED_INNS}
        cfg['prnsrv_local_history_eligible'] = {uid:source_hash}
        result = process_local_prnsrv_s3_event(bucket=BUCKET,key=key,runtime_config=cfg,
                                              event_id='temporary-prnsrv-'+uid)
        if result.get('result') != 'GENERATED':
            raise ValueError('Generator result requires reconciliation')

    def stage(self, uid):
        from .prnsrv_staging import PrnsrvStagingReceiver
        receiver = PrnsrvStagingReceiver.from_xtrek_config(bucket=BUCKET,
            s3_config=self.runtime['s3_config'], staging_root='/var/lib/xtrek/prnsrv/staging',
            allowlisted_uuids=[uid])
        result = receiver.stage(uid)
        if result.get('state') != 'STAGED':
            raise ValueError('Stage result requires reconciliation')
        return result['receipt_key']


def quiet_sdk():
    # Third-party request headers/bodies never belong in production health logs.
    names = ('boto3','botocore','urllib3','s3transfer')
    for name in list(logging.Logger.manager.loggerDict) + list(names):
        if any(name == n or name.startswith(n+'.')for n in names):
            logging.getLogger(name).setLevel(logging.WARNING)


def main():
    root = Path('/var/lib/xtrek/prnsrv/temporary-auto')
    supervisor = None
    try:
        manifest = json.loads(Path('/etc/xtrek/release-manifest.json').read_text())
        source = Path('/srv/xtrek/current').resolve()
        if (manifest.get('branch') != 'main' or str(source) != manifest.get('source')
                or Path(__file__).resolve() != source/'xtrek/prnsrv_supervisor.py'):
            raise FatalSupervisorError('Full-main source identity differs')
        for name,digest in manifest['files'].items():
            if hashlib.sha256((source/name).read_bytes()).hexdigest()!=digest:
                raise FatalSupervisorError('Full-main source checksum differs')
        from .config_loader import load_config
        runtime = load_config('suz_worker_config')
        if (runtime.get('sscc_prefix') != '460705179' or str(runtime.get('sscc_extension'))!='3'
                or runtime.get('sscc_auth_mode')!='mtls'
                or not ALLOWED_INNS <= set(runtime['signing']['local_by_inn'])):
            raise FatalSupervisorError('Runtime identity differs')
        import pwd
        account = pwd.getpwnam('xtrek')
        if os.geteuid() == 0:
            os.initgroups('xtrek',account.pw_gid)
            os.setgid(account.pw_gid)
            os.setuid(account.pw_uid)
        os.umask(0o077)
        owner = (root/'owner.lock').open('a')
        try:
            fcntl.flock(owner,fcntl.LOCK_EX|fcntl.LOCK_NB)
        except BlockingIOError:
            raise FatalSupervisorError('Owner already active') from None
        os.environ['XTREK_LOCAL_ALLOWED_INNS'] = ','.join(sorted(ALLOWED_INNS))
        quiet_sdk()
        adapter = ProductionAdapter(runtime)
        def check_release():
            if Path('/srv/xtrek/current').resolve() != source:
                raise FatalSupervisorError('Current release changed')
        supervisor = Supervisor(root,manifest['commit'],adapter,check_release=check_release)
        signal.signal(signal.SIGTERM,supervisor.stop)
        signal.signal(signal.SIGINT,supervisor.stop)
        supervisor.run(once='--once'in sys.argv)
        return 0
    except Exception as error:
        fatal = isinstance(error, (FatalSupervisorError, KeyError, ImportError,
                                    json.JSONDecodeError)) or supervisor is None
        phase = 'FATAL' if fatal else 'CRASHED'
        try:
            if supervisor is not None:
                supervisor.evidence.error(error,phase)
            else:
                # Separate file: never overwrite another live owner's heartbeat.
                atomic_json(root/'startup-error.json', {'at':time.time(),
                            'kind':phase, **safe_error(error)})
        except FatalSupervisorError:
            fatal = True
        LOG.error('%s',json.dumps({'kind':phase,**safe_error(error)}))
        return 78 if fatal else 1


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    raise SystemExit(main())
