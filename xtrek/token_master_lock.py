"""Owner-checked S3 lock for token issuers; no age-only takeover or DELETE."""
from contextlib import contextmanager
import hashlib
import json
import logging
import os
from pathlib import Path
import socket
import sys
import time
from uuid import uuid4

from .token_registry import TokenValidationError
from .token_runtime import cleanup_budget, current_runtime, DeadlineExpired

logger = logging.getLogger(__name__)


def _read(path):
    try:
        return Path(path).read_text().strip()
    except OSError:
        return None


def _process_start(pid):
    stat = _read('/proc/{}/stat'.format(pid))
    # comm may contain spaces and parentheses. starttime is field 22.
    return stat.rsplit(')', 1)[1].split()[19] if stat else None


def owner_identity():
    try:
        pid_namespace = os.readlink('/proc/self/ns/pid')
    except OSError:
        pid_namespace = None
    return {
        'host': socket.gethostname(),
        'machine': _read('/etc/machine-id'),
        'boot': _read('/proc/sys/kernel/random/boot_id'),
        'pid': os.getpid(),
        'process_start': _process_start(os.getpid()),
        'pid_namespace': pid_namespace,
        'invocation': os.getenv('INVOCATION_ID'),
    }


def owner_is_dead(owner, local):
    """Unknown/foreign/live owners stay locked, regardless of expires_at."""
    if not isinstance(owner, dict) or owner.get('host') != local['host']:
        return False
    if owner.get('machine') != local['machine']:
        return False
    pid = owner.get('pid')
    if not isinstance(pid, int) or isinstance(pid, bool) or pid <= 0:
        return False
    if owner.get('boot') and local['boot'] and owner['boot'] != local['boot']:
        return True
    if owner.get('pid_namespace') != local.get('pid_namespace'):
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return True
    except (PermissionError, OSError):
        return False
    start = _process_start(pid)
    return bool(start and owner.get('process_start') and start != owner['process_start'])


class TokenMasterLock:
    def __init__(self, storage, path, config):
        self.storage, self.path = storage, path
        self.identity = owner_identity()
        self.owner_id = str(uuid4())
        self.etag = None
        digest = hashlib.sha256(path.encode()).hexdigest()
        directory = Path(config.get('tokens_master_local_lock_dir') or Path.home() / '.cache' / 'xtrek')
        self.local_path = directory / ('issuer-' + digest + '.lock')

    def read(self):
        result = self.storage.read_lock_object(self.path)
        if result is None:
            return None, None
        text, etag = result
        if not isinstance(etag, str) or not etag:
            raise TokenValidationError('Token master lock has no ETag')
        try:
            value = json.loads(text)
        except (ValueError, TypeError):
            raise TokenValidationError('Legacy token master lock requires coordinated migration') from None
        if (not isinstance(value, dict) or value.get('version') != 1
                or value.get('state') not in {'held', 'released'}
                or not isinstance(value.get('owner_id'), str)
                or not isinstance(value.get('owner'), dict)):
            raise TokenValidationError('Unknown token master lock requires coordinated migration')
        return value, etag

    def acquire(self):
        previous, etag = self.read()
        if previous is not None and previous['state'] != 'released':
            if not owner_is_dead(previous['owner'], self.identity):
                raise TokenValidationError('Token master is already locked; no issuance attempted')
            logger.warning('Recovering token master lock after owner termination')
        value = dict(version=1, owner_id=self.owner_id, owner=self.identity,
                     state='held', acquired_at=time.time(), expires_at=time.time() + 720,
                     revision=str(uuid4()))
        self.etag = self.storage.write_lock_object(self.path, json.dumps(value), etag)
        if self.etag is None:
            raise TokenValidationError('Token master is already locked; no issuance attempted')
        self.assert_owned()

    def assert_owned(self):
        value, etag = self.read()
        if (value is None or value['owner_id'] != self.owner_id or value['state'] != 'held'
                or etag != self.etag):
            raise TokenValidationError('Token master ownership could not be confirmed')

    def release(self):
        # Also resolves a PUT accepted remotely whose response was interrupted.
        value, etag = self.read()
        if value is None or value['owner_id'] != self.owner_id or value['state'] != 'held':
            return
        value.update(state='released', released_at=time.time(), revision=str(uuid4()))
        if self.storage.write_lock_object(self.path, json.dumps(value), etag) is None:
            raise TokenValidationError('Token master lock changed during release')

    @contextmanager
    def hold(self):
        # Local flock is shared by master and crpt-auth, independent of systemd's
        # outer flock. Never unlink its inode, including after SIGKILL.
        import fcntl
        self.local_path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(self.local_path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        try:
            os.fchmod(fd, 0o600)
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise TokenValidationError('Token master is already locked; no issuance attempted') from None
            runtime = current_runtime()
            old_guard = runtime.guard if runtime else None
            try:
                # try begins BEFORE the remote PUT: even interrupted acquisition
                # may have created a lock belonging to this process.
                self.acquire()
                if runtime:
                    runtime.guard = self
                yield
            finally:
                primary_error = sys.exc_info()[0] is not None
                if runtime:
                    runtime.guard = old_guard
                try:
                    with cleanup_budget(15):
                        self.release()
                except (Exception, DeadlineExpired):
                    logger.error('Token master lock release not confirmed')
                    if not primary_error:
                        raise
        finally:
            os.close(fd)
