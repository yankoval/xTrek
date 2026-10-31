"""Durable MAX retry deadlines. No HTTP retries or message POSTs happen here."""
from __future__ import annotations

import hashlib
import random
import sqlite3
import time
from email.utils import parsedate_to_datetime


class MaxRetryLater(RuntimeError):
    def __init__(self, retry_after, message='MAX check deferred; retry GET later'):
        self.retry_after = max(1.0, float(retry_after))
        super().__init__(message)

    def __reduce__(self):
        return type(self), (self.retry_after, str(self))


class RetrySchedule:
    def __init__(self, path, *, clock=time.time, jitter=random.random, base=5, cap=300):
        self.path, self.clock, self.jitter = str(path), clock, jitter
        self.base, self.cap = base, cap
        with sqlite3.connect(self.path) as db:
            db.execute('''CREATE TABLE IF NOT EXISTS max_retry_schedule (
                scope TEXT PRIMARY KEY, failures INTEGER NOT NULL, next_at REAL NOT NULL)''')

    def remaining(self, scope):
        with sqlite3.connect(self.path) as db:
            row = db.execute('SELECT next_at FROM max_retry_schedule WHERE scope=?', (scope,)).fetchone()
        return max(0, row[0] - self.clock()) if row else 0

    def check(self, scope):
        delay = self.remaining(scope)
        if delay:
            raise MaxRetryLater(delay)

    def fail(self, scope, retry_after=None):
        now = self.clock()
        minimum = 0
        if retry_after is not None:
            try:
                minimum = max(0, float(retry_after))
            except (TypeError, ValueError):
                try:
                    minimum = max(0, parsedate_to_datetime(retry_after).timestamp() - now)
                except (TypeError, ValueError, OverflowError):
                    pass
        with sqlite3.connect(self.path) as db:
            db.execute('BEGIN IMMEDIATE')
            row = db.execute('SELECT failures,next_at FROM max_retry_schedule WHERE scope=?', (scope,)).fetchone()
            # Other tasks seeing the same cooldown must not keep increasing it.
            if row and row[1] > now:
                deadline = max(row[1], now + minimum)
                db.execute('UPDATE max_retry_schedule SET next_at=? WHERE scope=?', (deadline, scope))
                return deadline - now
            failures = min((row[0] if row else 0) + 1, 32)
            delay = max(minimum, min(self.cap, self.base * 2 ** (failures - 1) * (1 + .2 * self.jitter())))
            db.execute('INSERT OR REPLACE INTO max_retry_schedule VALUES (?,?,?)', (scope, failures, now + delay))
            return delay

    def success(self, scope):
        with sqlite3.connect(self.path) as db:
            # A response from an older in-flight request cannot cancel a new 429.
            db.execute('DELETE FROM max_retry_schedule WHERE scope=? AND next_at<=?', (scope, self.clock()))

    def clear(self, scope):
        with sqlite3.connect(self.path) as db:
            db.execute('DELETE FROM max_retry_schedule WHERE scope=?', (scope,))


def api_scope(token, chat_id):
    return 'max-api:' + hashlib.sha256(token.encode()).hexdigest() + ':' + str(chat_id)


def observe_response(schedule, scope, response):
    if response.status_code == 429 or response.status_code >= 500:
        headers = getattr(response, 'headers', {})
        delay = schedule.fail(scope, headers.get('Retry-After'))
        raise MaxRetryLater(delay, f'MAX HTTP {response.status_code}; retry GET later')
