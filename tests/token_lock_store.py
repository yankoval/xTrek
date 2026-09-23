"""Synthetic conditional-object stores; never connect to production S3."""
import json
import sqlite3
from uuid import uuid4


class MemoryLockStore:
    def __init__(self):
        self.lock_object = None

    @property
    def locked(self):
        return self.lock_object is not None and json.loads(self.lock_object[0])['state'] == 'held'

    def read_lock_object(self, path):
        return self.lock_object

    def write_lock_object(self, path, content, etag):
        current = self.lock_object[1] if self.lock_object else None
        if current != etag:
            return None
        tag = str(uuid4())
        self.lock_object = (content, tag)
        return tag


class ProcessLockStore:
    """SQLite serializes synthetic CAS across independent test processes."""
    def __init__(self, path):
        self.path = path
        with sqlite3.connect(path) as conn:
            conn.execute('CREATE TABLE IF NOT EXISTS locks (path TEXT PRIMARY KEY, body TEXT, etag TEXT)')

    def read_lock_object(self, path):
        with sqlite3.connect(self.path) as conn:
            return conn.execute('SELECT body, etag FROM locks WHERE path=?', (path,)).fetchone()

    def write_lock_object(self, path, content, etag):
        with sqlite3.connect(self.path) as conn:
            conn.execute('BEGIN IMMEDIATE')
            old = conn.execute('SELECT etag FROM locks WHERE path=?', (path,)).fetchone()
            if (old[0] if old else None) != etag:
                return None
            tag = str(uuid4())
            conn.execute('INSERT OR REPLACE INTO locks VALUES (?, ?, ?)', (path, content, tag))
            return tag
