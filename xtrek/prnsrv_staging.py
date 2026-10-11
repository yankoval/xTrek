"""Non-hot local staging receiver for immutable PRNSRV S3 outputs."""
from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from .operation_state import publish_once


class StagingConflict(RuntimeError):
    pass


class PrnsrvStagingReceiver:
    """Stage explicitly allowlisted PRNSRV artifacts; never print or route onward."""

    FORBIDDEN_ROOT_MARKERS = ('c:/tmp', 'c:\\tmp', 'vdfpr', 'printserver', 'hotfolder', 'hot-folder')

    @classmethod
    def from_xtrek_config(cls, *, bucket: str, s3_config: dict,
                          staging_root: str, allowlisted_uuids: Iterable[str],
                          output_prefix: str = 'printer-tasks/',
                          done_prefix: str = '_prnsrv/done/',
                          receipt_prefix: str = 'printer-task-stage-receipts/'):
        """Build a bucket-scoped receiver over the already configured S3 client."""
        from .storage import get_storage
        from .prnsrv_generator_runtime.storage import S3Storage
        existing = get_storage(f's3://{bucket}/', s3_config)
        client = getattr(existing, 's3', None)
        if client is None:
            raise RuntimeError('Configured S3 storage does not expose its boto3 client')
        return cls(
            storage=S3Storage(client), bucket=bucket, output_root='', receipt_root='',
            staging_root=staging_root, allowlisted_uuids=allowlisted_uuids,
            output_prefix=output_prefix, done_prefix=done_prefix,
            receipt_prefix=receipt_prefix,
        )

    def __init__(self, *, storage: Any, output_root: str, receipt_root: str,
                 staging_root: str, allowlisted_uuids: Iterable[str],
                 bucket: str | None = None,
                 output_prefix: str = 'printer-tasks/',
                 done_prefix: str = '_prnsrv/done/',
                 receipt_prefix: str = 'printer-task-stage-receipts/'):
        root = Path(staging_root)
        normalized = str(root).replace('\\', '/').lower()
        if any(marker in normalized for marker in self.FORBIDDEN_ROOT_MARKERS):
            raise ValueError('Staging root points to a watched or printer path')
        self.storage = storage
        self.bucket = bucket
        self.output_root = output_root.rstrip('/')
        self.receipt_root = receipt_root.rstrip('/')
        self.output_prefix = output_prefix.strip('/') + '/'
        self.done_prefix = done_prefix.strip('/') + '/'
        self.receipt_prefix = receipt_prefix.strip('/') + '/'
        self.staging_root = root
        self.allowlisted = {str(uuid.UUID(value)) for value in allowlisted_uuids}

    @staticmethod
    def _hash(data: bytes) -> str:
        return hashlib.sha256(data).hexdigest()

    def _read_bytes(self, key: str) -> bytes:
        if self.bucket:
            return self.storage.get_bytes(self.bucket, key)
        reader = getattr(self.storage, 'read_bytes', None)
        if reader:
            return reader(key)
        with tempfile.TemporaryDirectory(prefix='xtrek-prnsrv-receiver-') as folder:
            path = Path(folder) / 'object'
            self.storage.download(key, str(path))
            return path.read_bytes()

    def _exists(self, key: str) -> bool:
        if self.bucket:
            return self.storage.head_optional(self.bucket, key) is not None
        return self.storage.exists(key)

    def _read_text(self, key: str) -> str:
        if self.bucket:
            return self._read_bytes(key).decode('utf-8-sig')
        return self.storage.read_text(key)

    def _publish_receipt(self, key: str, receipt: dict) -> None:
        text = json.dumps(receipt, ensure_ascii=False, sort_keys=True,
                          separators=(',', ':'))
        if not self.bucket:
            publish_once(self.storage, key, text)
            if json.loads(self.storage.read_text(key)) != receipt:
                raise StagingConflict('S3 staging receipt failed read-back verification')
            return
        from .prnsrv_generator_runtime.storage import ObjectAlreadyExists
        try:
            self.storage.put_create_only(
                self.bucket, key, text.encode('utf-8'),
                content_type='application/json',
            )
        except ObjectAlreadyExists:
            if self.storage.get_json(self.bucket, key) != receipt:
                raise StagingConflict('Existing S3 staging receipt has different content')
        if self.storage.get_json(self.bucket, key) != receipt:
            raise StagingConflict('S3 staging receipt failed read-back verification')

    def _atomic_file(self, target: Path, data: bytes) -> None:
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.exists():
            if target.is_symlink() or target.read_bytes() != data:
                raise StagingConflict(f'Existing staged file differs: {target}')
            return
        fd, temporary = tempfile.mkstemp(prefix=f'.{target.name}.', suffix='.tmp', dir=target.parent)
        try:
            with os.fdopen(fd, 'wb') as output:
                output.write(data)
                output.flush()
                os.fsync(output.fileno())
            try:
                os.link(temporary, target)
            except FileExistsError:
                if target.is_symlink() or target.read_bytes() != data:
                    raise StagingConflict(f'Concurrent staged file differs: {target}')
            directory_fd = os.open(target.parent, os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        finally:
            try:
                os.unlink(temporary)
            except FileNotFoundError:
                pass

    def stage(self, job_uuid: str) -> dict:
        job_uuid = str(uuid.UUID(job_uuid))
        if job_uuid not in self.allowlisted:
            raise StagingConflict('UUID is not on the receiver allowlist')
        if self.bucket:
            done_key = f'{self.done_prefix}{job_uuid}.done'
            done_key_label = f's3://{self.bucket}/{done_key}'
        else:
            done_key = f'{self.output_root}/_prnsrv/done/{job_uuid}.done'
            done_key_label = done_key
        if not self._exists(done_key):
            raise StagingConflict('PRNSRV done marker is missing')
        marker = json.loads(self._read_text(done_key))
        if marker.get('uuid') != job_uuid or marker.get('result') != 'GENERATED':
            raise StagingConflict('Done marker has a wrong UUID or non-generated result')
        source_key = marker.get('source_key')
        if not isinstance(source_key, str) or source_key.rsplit('/', 1)[-1] != f'{job_uuid}.json':
            raise StagingConflict('Done marker source key does not match the allowlisted UUID')
        allocation = marker.get('allocation')
        if not isinstance(allocation, dict) or not str(allocation.get('namespace', '')).endswith('/extension-3'):
            raise StagingConflict('Done marker is outside the local extension-3 namespace')
        source_hash = marker.get('source_hash')
        outputs = marker.get('outputs')
        if not re.fullmatch(r'[0-9a-f]{64}', str(source_hash or '')) or not isinstance(outputs, dict):
            raise StagingConflict('Done marker lacks source identity or output hashes')

        verified: dict[str, tuple[str, bytes, str]] = {}
        expected_names = {'csv': f'{job_uuid}.csv'}
        for name in ('csv', 'vdf'):
            value = outputs.get(name)
            if not isinstance(value, dict) or not isinstance(value.get('key'), str):
                raise StagingConflict(f'Done marker lacks {name} key')
            key = value['key']
            expected_prefix = self.output_prefix if self.bucket else f'{self.output_root}/printer-tasks/'
            if not key.startswith(expected_prefix):
                raise StagingConflict(f'{name} key is outside printer-tasks')
            file_name = key.rsplit('/', 1)[-1]
            if name == 'csv' and file_name != expected_names['csv']:
                raise StagingConflict('CSV filename does not match the allowlisted UUID')
            if name == 'vdf' and not re.fullmatch(r'[A-Za-z0-9_-]+_' + re.escape(job_uuid) + r'\.vdf', file_name):
                raise StagingConflict('VDF filename does not match the allowlisted UUID')
            expected_hash = value.get('sha256')
            if not re.fullmatch(r'[0-9a-f]{64}', str(expected_hash or '')):
                raise StagingConflict(f'Done marker has invalid {name} hash')
            data = self._read_bytes(key)
            actual_hash = self._hash(data)
            if actual_hash != expected_hash:
                raise StagingConflict(f'S3 {name} hash does not match done marker')
            verified[name] = (key, data, actual_hash)

        destination = self.staging_root / job_uuid
        destination.mkdir(parents=True, exist_ok=True)
        lock = destination / '.stage.lock'
        try:
            lock_fd = os.open(lock, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        except FileExistsError as exc:
            raise StagingConflict('Another staging attempt or stale lock exists; reconcile before retry') from exc
        try:
            try:
                os.write(lock_fd, str(os.getpid()).encode())
                os.fsync(lock_fd)
            finally:
                os.close(lock_fd)
            files = {}
            for name, (key, data, digest) in verified.items():
                file_name = key.rsplit('/', 1)[-1]
                self._atomic_file(destination / file_name, data)
                if self._hash((destination / file_name).read_bytes()) != digest:
                    raise StagingConflict(f'Local {name} readback hash does not match')
                files[name] = {'name': file_name, 's3_key': key, 'sha256': digest}
            manifest = {
                'schema_version': 1, 'uuid': job_uuid, 'source_hash': source_hash,
                'source_key': source_key, 'done_key': done_key_label, 'files': files,
            }
            manifest_path = destination / 'manifest.json'
            if manifest_path.exists():
                previous_manifest = json.loads(manifest_path.read_text(encoding='utf-8'))
                if any(previous_manifest.get(key) != value for key, value in manifest.items()):
                    raise StagingConflict('Existing staging manifest has different source/output identity')
                arrived_at = previous_manifest.get('arrived_at')
                if not isinstance(arrived_at, str):
                    raise StagingConflict('Existing staging manifest has no arrival timestamp')
                manifest['arrived_at'] = arrived_at
            else:
                manifest['arrived_at'] = datetime.now(timezone.utc).isoformat()
            manifest_bytes = json.dumps(manifest, ensure_ascii=False, sort_keys=True,
                                        separators=(',', ':')).encode('utf-8')
            self._atomic_file(manifest_path, manifest_bytes)
            receipt = dict(manifest, staging_path=str(destination),
                           staged_manifest_sha256=self._hash(manifest_bytes))
            if self.bucket:
                receipt_key = f'{self.receipt_prefix}{job_uuid}.json'
                receipt_key_label = f's3://{self.bucket}/{receipt_key}'
            else:
                receipt_key = f'{self.receipt_root}/{job_uuid}.json'
                receipt_key_label = receipt_key
            self._publish_receipt(receipt_key, receipt)
            return {'state': 'STAGED', 'uuid': job_uuid,
                    'path': str(destination), 'receipt_key': receipt_key_label,
                    'files': files}
        finally:
            try:
                lock.unlink()
            except FileNotFoundError:
                pass
