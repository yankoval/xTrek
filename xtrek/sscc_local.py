"""Local, durable SSCC allocator for a single configured prefix/extension."""
import hashlib
import json
import os
import sqlite3
import sys
import threading


class AllocationConflict(ValueError):
    pass


def _check_digit(body):
    total = sum(int(d) * (3 if i % 2 == 0 else 1)
                for i, d in enumerate(reversed(body)))
    return str((-total) % 10)


class Ledger:
    """SQLite rollback-journal ledger; each range and response commits atomically."""
    def __init__(self, path, prefix, extension=3):
        if not str(prefix).isdigit() or not 1 <= len(str(prefix)) <= 14:
            raise ValueError('prefix must contain 1-14 digits')
        if str(extension) != '3':
            raise ValueError('local allocator is restricted to extension 3')
        self.path, self.prefix, self.extension = path, str(prefix), 3
        self.serial_digits = 16 - len(self.prefix)
        self.limit = 10 ** self.serial_digits
        self._lock = threading.RLock()
        if not os.path.isfile(path):
            raise RuntimeError('SSCC ledger database is missing; explicit initialization required')
        try:
            with self._connect() as db:
                integrity = db.execute('PRAGMA integrity_check').fetchone()
                if not integrity or integrity[0] != 'ok':
                    raise RuntimeError('SSCC ledger integrity check failed')
                metadata = dict(db.execute('SELECT key, value FROM ledger_meta').fetchall())
                counter = db.execute(
                    'SELECT next_serial FROM counters WHERE prefix=? AND extension=3',
                    (self.prefix,)).fetchone()
                journal_highwater = db.execute(
                    'SELECT COALESCE(MAX(start_serial + quantity), 0) FROM allocations'
                ).fetchone()[0]
                if (metadata.get('prefix') != self.prefix or metadata.get('extension') != '3'
                        or counter is None or counter[0] < 0 or counter[0] > self.limit
                        or counter[0] != journal_highwater):
                    raise RuntimeError('SSCC ledger namespace/counter verification failed')
                db.execute('SELECT request_id FROM allocations LIMIT 1').fetchall()
        except sqlite3.DatabaseError as exc:
            raise RuntimeError('SSCC ledger schema verification failed') from exc
        os.chmod(path, 0o600)

    @classmethod
    def initialize(cls, path, prefix, extension=3):
        """Explicitly create a brand-new ledger; refuses every existing file."""
        if not str(prefix).isdigit() or not 1 <= len(str(prefix)) <= 14:
            raise ValueError('prefix must contain 1-14 digits')
        if str(extension) != '3':
            raise ValueError('local allocator is restricted to extension 3')
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        os.close(fd)
        db = sqlite3.connect(path, isolation_level=None)
        try:
            db.execute('PRAGMA journal_mode=DELETE')
            db.execute('PRAGMA synchronous=EXTRA')
            db.execute('PRAGMA fullfsync=ON')
            db.execute('BEGIN IMMEDIATE')
            db.execute('''CREATE TABLE ledger_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)''')
            db.execute('''CREATE TABLE counters (
                prefix TEXT NOT NULL, extension INTEGER NOT NULL, next_serial INTEGER NOT NULL,
                PRIMARY KEY(prefix, extension))''')
            db.execute('''CREATE TABLE allocations (
                request_id TEXT PRIMARY KEY, params_hash TEXT NOT NULL,
                response_json TEXT NOT NULL, start_serial INTEGER NOT NULL,
                quantity INTEGER NOT NULL)''')
            db.executemany('INSERT INTO ledger_meta VALUES (?, ?)',
                           [('prefix', str(prefix)), ('extension', '3')])
            db.execute('INSERT INTO counters VALUES (?, 3, 0)', (str(prefix),))
            db.execute('COMMIT')
        except BaseException:
            if db.in_transaction:
                db.execute('ROLLBACK')
            db.close()
            os.unlink(path)
            raise
        db.close()
        os.chmod(path, 0o600)
        return cls(path, prefix, extension)

    def _connect(self):
        db = sqlite3.connect(self.path, timeout=30, isolation_level=None)
        db.execute('PRAGMA journal_mode=DELETE')
        db.execute('PRAGMA synchronous=EXTRA')
        db.execute('PRAGMA fullfsync=ON')
        db.execute('PRAGMA busy_timeout=30000')
        return db

    def allocate(self, request_id, prefix, count, extension=3):
        request_id = str(request_id or '')
        if not request_id or len(request_id) > 200:
            raise ValueError('request_id is required')
        if str(prefix) != self.prefix or str(extension) != '3':
            raise AllocationConflict('namespace is not enabled on this allocator')
        if isinstance(count, bool) or not isinstance(count, int) or count <= 0 or count > 10000:
            raise ValueError('count must be an integer from 1 to 10000')
        params_hash = hashlib.sha256(json.dumps(
            {'prefix': self.prefix, 'extension': 3, 'count': count},
            sort_keys=True, separators=(',', ':')).encode()).hexdigest()
        with self._lock:
            db = self._connect()
            try:
                db.execute('BEGIN IMMEDIATE')
                prior = db.execute(
                    'SELECT params_hash, response_json FROM allocations WHERE request_id=?',
                    (request_id,)).fetchone()
                if prior:
                    if prior[0] != params_hash:
                        raise AllocationConflict('request_id was already used with different parameters')
                    result = json.loads(prior[1])
                    db.execute('COMMIT')
                    return result
                row = db.execute('SELECT next_serial FROM counters WHERE prefix=? AND extension=3',
                                 (self.prefix,)).fetchone()
                start = row[0]
                if start + count > self.limit:
                    raise OverflowError('SSCC namespace exhausted')
                codes = []
                for serial in range(start, start + count):
                    body = f'3{self.prefix}{serial:0{self.serial_digits}d}'
                    codes.append(body + _check_digit(body))
                result = {'ssccs': codes}
                db.execute('UPDATE counters SET next_serial=? WHERE prefix=? AND extension=3',
                           (start + count, self.prefix))
                db.execute('INSERT INTO allocations VALUES (?, ?, ?, ?, ?)',
                           (request_id, params_hash, json.dumps(result), start, count))
                db.execute('COMMIT')
                return result
            except BaseException:
                if db.in_transaction:
                    db.execute('ROLLBACK')
                raise
            finally:
                db.close()


def create_handler(ledger, allowed_subjects):
    from http.server import BaseHTTPRequestHandler

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            cert = self.connection.getpeercert()
            subject = '/'.join(f'{k}={v}' for group in cert.get('subject', ())
                               for k, v in group)
            if subject not in allowed_subjects:
                return self._reply(403, {'error': 'client certificate is not authorized'})
            if self.path != '/v1/allocate':
                return self._reply(404, {'error': 'not found'})
            try:
                length = int(self.headers.get('Content-Length', '0'))
                if not 0 < length <= 65536:
                    raise ValueError('invalid request size')
                payload = json.loads(self.rfile.read(length))
                result = ledger.allocate(payload.get('request_id'), payload.get('prefix'),
                                         payload.get('count'), payload.get('extension', 3))
                self._reply(200, result)
            except AllocationConflict as exc:
                self._reply(409, {'error': str(exc)})
            except OverflowError as exc:
                self._reply(409, {'error': str(exc)})
            except (TypeError, ValueError, json.JSONDecodeError) as exc:
                self._reply(400, {'error': str(exc)})
            except Exception:
                self._reply(500, {'error': 'allocation failed'})

        def _reply(self, code, value):
            body = json.dumps(value).encode()
            self.send_response(code)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *_args):
            return

    return Handler


if __name__ == '__main__' and sys.argv[1:2] == ['--init-ledger']:
    if len(sys.argv) != 4:
        raise SystemExit('usage: python -m xtrek.sscc_local --init-ledger DATABASE PREFIX')
    Ledger.initialize(sys.argv[2], sys.argv[3], 3)
