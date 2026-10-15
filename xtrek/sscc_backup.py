"""Bounded SSCC snapshots: three changed states and two daily checkpoints.

Only the five named slots and one candidate in a dedicated bucket are touched.
A single producer must hold the local lock; never restore into a live allocator.
"""
from __future__ import annotations

import argparse
from contextlib import closing
from datetime import datetime, timezone
import gzip
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import tempfile
from urllib.parse import quote, urlparse

from botocore.exceptions import ClientError

SLOTS = tuple(f'rolling-{i}.sqlite3.gz' for i in range(3)) + tuple(
    f'daily-{i}.sqlite3.gz' for i in range(2))
CANDIDATE = 'candidate.sqlite3.gz'
ROOT = 'sscc-backup/v1/'
MAX_BYTES = 64 * 1024 * 1024


def inspect_database(path, prefix):
    """Read-only application invariants, including the allocation watermark."""
    with closing(sqlite3.connect('file:' + quote(str(Path(path).resolve())) + '?mode=ro',
                                uri=True)) as db:
        if db.execute('PRAGMA integrity_check').fetchall() != [('ok',)]:
            raise RuntimeError('Snapshot integrity check failed')
        meta = dict(db.execute('SELECT key,value FROM ledger_meta'))
        counters = db.execute('SELECT prefix,extension,next_serial FROM counters').fetchall()
        if meta.get('prefix') != prefix or meta.get('extension') != '3':
            raise RuntimeError('Snapshot namespace mismatch')
        highwater = 0
        count = 0
        for start, quantity in db.execute(
                'SELECT start_serial,quantity FROM allocations ORDER BY start_serial'):
            if start != highwater or quantity <= 0:
                raise RuntimeError('Snapshot allocation ranges are inconsistent')
            highwater += quantity
            count += 1
        if (counters != [(prefix, 3, highwater)] or
                highwater > 10 ** (16 - len(prefix))):
            raise RuntimeError('Snapshot counter mismatch')
        return {'next_serial': highwater, 'allocations': count}


def snapshot(database, prefix):
    if not Path(database).is_file():
        raise RuntimeError('Source ledger is missing')
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / 'snapshot.sqlite3'
        with closing(sqlite3.connect('file:' + quote(str(Path(database).resolve())) +
                                    '?mode=ro', uri=True)) as source:
            with closing(sqlite3.connect(path)) as target:
                source.backup(target)
        info = inspect_database(path, prefix)
        content = path.read_bytes()
        if len(content) > MAX_BYTES:
            raise RuntimeError('Snapshot exceeds the configured safety bound')
        return content, info


def verify(content, metadata, prefix):
    if len(content) > MAX_BYTES:
        raise RuntimeError('Compressed snapshot is too large')
    # Bounded decompression also protects the isolated restore command.
    import io
    with gzip.GzipFile(fileobj=io.BytesIO(content)) as stream:
        raw = stream.read(MAX_BYTES + 1)
    if len(raw) > MAX_BYTES or hashlib.sha256(raw).hexdigest() != metadata.get('sha256'):
        raise RuntimeError('Snapshot checksum mismatch')
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / 'verify.sqlite3'
        path.write_bytes(raw)
        info = inspect_database(path, prefix)
    if (str(info['next_serial']) != metadata.get('next-serial') or
            str(info['allocations']) != metadata.get('allocations') or
            metadata.get('extension') != '3' or metadata.get('prefix') != prefix):
        raise RuntimeError('Snapshot metadata mismatch')
    captured = datetime.fromisoformat(metadata['captured-at'])
    if captured.utcoffset() is None or metadata.get('day') != captured.date().isoformat():
        raise RuntimeError('Snapshot timestamp metadata mismatch')
    return raw, info


class BackupStore:
    def __init__(self, client, bucket, prefix, operation_bucket):
        if (not bucket or not operation_bucket or bucket == operation_bucket or
                not prefix.isdigit() or not 1 <= len(prefix) <= 14):
            raise ValueError('A separate backup bucket and valid SSCC namespace are required')
        self.client, self.bucket, self.prefix = client, bucket, prefix

    def check_bucket(self):
        versioning = self.client.get_bucket_versioning(Bucket=self.bucket)
        if versioning.get('Status') in ('Enabled', 'Suspended'):
            raise RuntimeError('Versioning must never have been enabled: slot overwrites would accumulate')

    def get(self, slot):
        if slot not in SLOTS + (CANDIDATE,):
            raise ValueError('Unknown backup slot')
        try:
            obj = self.client.get_object(Bucket=self.bucket, Key=ROOT + slot)
        except ClientError as exc:
            if exc.response['Error']['Code'] in ('NoSuchKey', '404'):
                return None
            raise
        try:
            content = obj['Body'].read(MAX_BYTES + 1)
        finally:
            obj['Body'].close()
        # S3-compatible providers can return user metadata names in title case.
        metadata = {k.lower(): v for k, v in obj['Metadata'].items()}
        verify(content, metadata, self.prefix)
        return {'content': content, 'metadata': metadata}

    def put_verified(self, slot, content, metadata):
        if slot not in SLOTS + (CANDIDATE,):
            raise ValueError('Unknown backup slot')
        self.client.put_object(Bucket=self.bucket, Key=ROOT + slot, Body=content,
                               Metadata=metadata, ContentType='application/gzip')
        restored = self.get(slot)
        if not restored or restored['metadata'] != metadata or restored['content'] != content:
            raise RuntimeError('Uploaded snapshot verification failed')

    def run(self, database, now=None):
        self.check_bucket()
        now = now or datetime.now(timezone.utc)
        if now.utcoffset() is None:
            raise ValueError('A timezone-aware clock is required')
        now = now.astimezone(timezone.utc)
        raw, info = snapshot(database, self.prefix)
        digest = hashlib.sha256(raw).hexdigest()
        slots = {slot: self.get(slot) for slot in SLOTS}
        rolling = {k: v for k, v in slots.items() if k.startswith('rolling-') and v}
        newest = max(rolling.values(), key=lambda v: v['metadata']['captured-at'], default=None)
        candidate = self.get(CANDIDATE)
        retained = [v for v in slots.values() if v] + ([candidate] if candidate else [])
        if any(info['next_serial'] < int(v['metadata']['next-serial']) for v in retained):
            raise RuntimeError('Source watermark moved backwards; refusing to rotate')
        if any(now < datetime.fromisoformat(v['metadata']['captured-at']) for v in retained):
            raise RuntimeError('Clock moved backwards; refusing to rotate')
        changed = not newest or digest != newest['metadata']['sha256']
        today = now.date().isoformat()
        daily = {k: v for k, v in slots.items() if k.startswith('daily-')}
        previous_day = newest['metadata']['day'] if newest else None
        need_daily = (previous_day is not None and previous_day < today and not any(
            v and v['metadata']['day'] == previous_day for v in daily.values()))
        if not changed and not need_daily:
            # Recover the one temporary object left by an interrupted successful run.
            self.client.delete_object(Bucket=self.bucket, Key=ROOT + CANDIDATE)
            return {'state': 'unchanged', **info, 'slots': sum(v is not None for v in slots.values())}
        metadata = {'sha256': digest, 'next-serial': str(info['next_serial']),
                    'allocations': str(info['allocations']), 'extension': '3',
                    'prefix': self.prefix, 'captured-at': now.isoformat(), 'day': today}
        compressed = gzip.compress(raw, mtime=0)
        self.put_verified(CANDIDATE, compressed, metadata)
        targets = []
        # Preserve the previous day's last observed state before replacing its slot.
        if need_daily:
            slot = min(daily, key=lambda k: daily[k]['metadata']['day'] if daily[k] else '')
            self.put_verified(slot, newest['content'], newest['metadata'])
            targets.append(slot)
        if changed:
            slot = min(SLOTS[:3], key=lambda k: slots[k]['metadata']['captured-at']
                       if slots[k] else '')
            self.put_verified(slot, compressed, metadata)
            targets.append(slot)
        self.client.delete_object(Bucket=self.bucket, Key=ROOT + CANDIDATE)
        return {'state': 'verified', **info, 'slots': sum(v is not None for v in slots.values()) +
                sum(slots[k] is None for k in targets), 'written_slots': targets,
                'raw_bytes': len(raw), 'compressed_bytes': len(compressed)}

    def restore(self, slot, destination):
        if slot not in SLOTS:
            raise ValueError('Restore requires a confirmed slot')
        self.check_bucket()
        obj = self.get(slot)
        if obj is None:
            raise RuntimeError('Backup slot is empty')
        raw, info = verify(obj['content'], obj['metadata'], self.prefix)
        # Caller supplies a new directory, never a live database path.
        destination = Path(destination)
        destination.mkdir(mode=0o700, parents=False, exist_ok=False)
        path = destination / 'ledger.sqlite3'
        fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        with os.fdopen(fd, 'wb') as output:
            output.write(raw)
            output.flush()
            os.fsync(output.fileno())
        inspect_database(path, self.prefix)
        return {'state': 'restored-isolated', **info, 'sha256': obj['metadata']['sha256']}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', default='suz_worker_config')
    parser.add_argument('--database', default='/var/lib/xtrek-sscc/ledger.sqlite3')
    parser.add_argument('--lock', default='/var/lib/xtrek-sscc/backup.lock')
    parser.add_argument('--restore-slot', choices=SLOTS)
    parser.add_argument('--restore-directory')
    args = parser.parse_args()
    if bool(args.restore_slot) != bool(args.restore_directory):
        parser.error('Restore requires both slot and a new isolated directory')
    import fcntl
    from .config_loader import load_config
    from .storage import S3Storage
    config = load_config(args.config)
    operation = urlparse(config['operation_state_path'])
    if operation.scheme != 's3' or not operation.netloc:
        raise ValueError('A configured S3 operation state path is required')
    store = BackupStore(S3Storage(config['s3_config']).s3, config['sscc_backup_bucket'],
                        str(config['sscc_prefix']), operation.netloc)
    with open(args.lock, 'a') as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        result = (store.restore(args.restore_slot, args.restore_directory) if args.restore_slot
                  else store.run(args.database))
    print(json.dumps(result))


if __name__ == '__main__':
    main()
