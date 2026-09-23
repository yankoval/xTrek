"""Bounded lifetime for the POSIX master/issuer, including synchronous I/O.

Cancellation derives from BaseException so legacy per-token/network exception
handlers cannot swallow it. No signal handlers are installed in consumers.
"""
from contextlib import contextmanager
from contextvars import ContextVar
import math
import signal
import threading
import time


class MasterStopped(BaseException):
    pass


class DeadlineExpired(MasterStopped):
    pass


_current = ContextVar('token_master_runtime', default=None)


def current_runtime():
    return _current.get()


class MasterRuntime:
    def __init__(self):
        self.started = time.monotonic()
        self.deadline = self.started + 420
        self.limit = self.deadline
        self.stopping = False
        self.cleaning = False
        self.guard = None

    def configure(self, config):
        seconds = float(config.get('tokens_master_cycle_seconds', 420))
        if not math.isfinite(seconds) or not 0 < seconds <= 480:
            raise ValueError('tokens_master_cycle_seconds must be in (0, 480]')
        self.deadline = self.started + seconds
        self.limit = self.deadline
        self.check()
        self.arm()

    def remaining(self):
        return max(0, self.limit - time.monotonic())

    def check(self):
        if self.stopping and not self.cleaning:
            raise MasterStopped('Master stop requested')
        if self.remaining() <= 0:
            raise DeadlineExpired('Master time budget exhausted')

    def arm(self):
        # A positive timer is essential: zero disarms ITIMER_REAL.
        signal.setitimer(signal.ITIMER_REAL, max(0.001, self.remaining()))

    def on_alarm(self, signum, frame):
        raise DeadlineExpired('Master time budget exhausted')

    def on_stop(self, signum, frame):
        self.stopping = True
        if not self.cleaning:
            raise MasterStopped('Master stop requested')

    @contextmanager
    def budget(self, seconds):
        self.check()
        previous = self.limit
        self.limit = min(previous, time.monotonic() + seconds)
        self.arm()
        try:
            self.check()
            yield
        finally:
            self.limit = previous
            self.arm()

    @contextmanager
    def cleanup(self, seconds):
        """A small independent budget after cancellation; never an infinite finally."""
        previous, cleaning = self.limit, self.cleaning
        self.limit = time.monotonic() + seconds
        self.cleaning = True
        self.arm()
        try:
            yield
        finally:
            self.limit, self.cleaning = previous, cleaning
            # Do not re-raise a deadline between nested finally blocks.
            if self.remaining() > 0 and not self.stopping:
                self.arm()
            else:
                signal.setitimer(signal.ITIMER_REAL, 0)

    def assert_owner(self):
        self.check()
        if self.guard is not None:
            self.guard.assert_owned()


@contextmanager
def master_runtime(config=None):
    active = current_runtime()
    if active is not None:
        yield active
        return
    if threading.current_thread() is not threading.main_thread() or not hasattr(signal, 'setitimer'):
        raise RuntimeError('Token master requires a POSIX main process')
    if signal.getitimer(signal.ITIMER_REAL) != (0.0, 0.0):
        raise RuntimeError('Token master cannot replace an existing process timer')
    runtime = MasterRuntime()
    previous = {s: signal.getsignal(s) for s in (signal.SIGTERM, signal.SIGINT, signal.SIGALRM)}
    token = _current.set(runtime)
    try:
        signal.signal(signal.SIGTERM, runtime.on_stop)
        signal.signal(signal.SIGINT, runtime.on_stop)
        signal.signal(signal.SIGALRM, runtime.on_alarm)
        runtime.configure(config or {})
        yield runtime
        runtime.check()
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        for sig, handler in previous.items():
            signal.signal(sig, handler)
        _current.reset(token)


@contextmanager
def cleanup_budget(seconds):
    runtime = current_runtime()
    if runtime is None:
        yield
    else:
        with runtime.cleanup(seconds):
            yield


def checkpoint():
    runtime = current_runtime()
    if runtime is not None:
        runtime.check()


def request_timeout():
    runtime = current_runtime()
    if runtime is None:
        return (10, 30)
    runtime.check()
    remaining = runtime.remaining()
    return (min(10, remaining), min(30, remaining))
