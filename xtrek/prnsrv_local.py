"""Crash-safe local adapter for prnsrv CSV/VDF staging.

The pinned ``prnsrv`` engine supplies count/routing/rendering. This module owns
the xTrek operation guard, extension-3 request identity, immutable S3 outputs,
and completion marker. It never calls a printer or a hot-folder receiver.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
import uuid
from pathlib import Path
from typing import Any, Callable, Mapping

from .operation_state import (
    OperationConflict,
    OperationState,
    ReconciliationRequired,
    publish_once,
)


class PrnsrvLocalAdapter:
    """Adapt the reusable prnsrv engine to the worker's durable S3 workflows.

    ``engine`` must expose ``classify_count(source)``, ``resolve_route(source)``,
    and ``build_artifacts(source, ssccs, source_key, route)`` returning the exact
    CSV and VDF bytes. ``allocator`` accepts keyword arguments
    ``request_id``, ``prefix``, ``count``, and ``extension`` and returns SSCCs.
    """

    OPERATION = 'allocate-pallet-sscc'
    RETRY_MODE = 'local-sscc-v1'
    PROTECTED_UNCERTAIN_UUIDS = {'623c20fa-f90c-4478-aeae-9c6635cedc9f'}

    def __init__(
        self,
        *,
        storage: Any,
        state_storage: Any,
        state_root: str,
        output_root: str,
        engine: Any,
        allocator: Callable[..., list[str]],
        history_reconciled: Callable[[str, str], bool],
        prefix: str,
        extension: int = 3,
    ) -> None:
        if str(extension) != '3':
            raise ValueError('local prnsrv adapter is restricted to extension 3')
        if not str(prefix).isdigit() or not 1 <= len(str(prefix)) <= 14:
            raise ValueError('SSCC prefix must contain 1-14 digits')
        self.storage = storage
        self.state_storage = state_storage
        self.state_root = state_root.rstrip('/')
        self.output_root = output_root.rstrip('/')
        self.engine = engine
        self.allocator = allocator
        self.history_reconciled = history_reconciled
        self.prefix = str(prefix)
        self.extension = 3

    @staticmethod
    def source_hash(source: Mapping[str, Any]) -> str:
        canonical = json.dumps(
            source, ensure_ascii=False, sort_keys=True, separators=(',', ':')
        ).encode('utf-8')
        return hashlib.sha256(canonical).hexdigest()

    @staticmethod
    def _uuid(source_key: str) -> str:
        name = source_key.rsplit('/', 1)[-1]
        if not name.endswith('.json'):
            raise ValueError('source object must be a JSON file')
        try:
            return str(uuid.UUID(name[:-5]))
        except ValueError as exc:
            raise ValueError('source filename must contain a UUID') from exc

    @staticmethod
    def _sha256(data: bytes) -> str:
        return hashlib.sha256(data).hexdigest()

    def _output_keys(self, job_uuid: str, route: str) -> tuple[str, str]:
        route = str(route)
        if not route or route in {'.', '..'} or not re.fullmatch(r'[A-Za-z0-9_-]+', route):
            raise ValueError('route must be a safe filename component')
        return (
            f'{self.output_root}/printer-tasks/{job_uuid}.csv',
            f'{self.output_root}/printer-tasks/{route}_{job_uuid}.vdf',
        )

    def _done_key(self, job_uuid: str) -> str:
        return f'{self.output_root}/_prnsrv/done/{job_uuid}.done'

    def _read_bytes(self, path: str) -> bytes:
        read_bytes = getattr(self.storage, 'read_bytes', None)
        if read_bytes:
            return read_bytes(path)
        with tempfile.TemporaryDirectory(prefix='xtrek-prnsrv-verify-') as temp:
            local = Path(temp) / 'object.bin'
            self.storage.download(path, str(local))
            return local.read_bytes()

    def _read_done(self, job_uuid: str, source_hash: str) -> dict[str, Any] | None:
        path = self._done_key(job_uuid)
        if not self.storage.exists(path):
            return None
        marker = json.loads(self.storage.read_text(path))
        if marker.get('uuid') != job_uuid or marker.get('source_hash') != source_hash:
            raise OperationConflict('Existing prnsrv done marker has a different identity/source hash')
        result = marker.get('result')
        if result == 'NO_PRINT':
            return marker
        if result != 'GENERATED' or not isinstance(marker.get('outputs'), dict):
            raise ReconciliationRequired('Existing prnsrv done marker is incomplete or unknown')
        for name in ('csv', 'vdf'):
            output = marker['outputs'].get(name)
            if not isinstance(output, dict) or not output.get('key') or not output.get('sha256'):
                raise ReconciliationRequired(f'Existing prnsrv marker has no valid {name} metadata')
            if self._sha256(self._read_bytes(output['key'])) != output['sha256']:
                raise OperationConflict(f'Existing prnsrv {name} differs from its marker')
        return marker

    def _publish_done(self, marker: dict[str, Any]) -> None:
        publish_once(
            self.state_storage,
            self._done_key(marker['uuid']),
            json.dumps(marker, ensure_ascii=False, sort_keys=True, separators=(',', ':')),
        )

    def process(self, *, source_key: str, source: Mapping[str, Any]) -> dict[str, Any]:
        """Generate immutable S3 artifacts; downstream printer delivery is separate."""
        job_uuid = self._uuid(source_key)
        source_hash = self.source_hash(source)
        if job_uuid in self.PROTECTED_UNCERTAIN_UUIDS:
            raise ReconciliationRequired('Protected uncertain UUID is not eligible for local allocation')
        if not self.history_reconciled(job_uuid, source_hash):
            raise ReconciliationRequired('Prior allocator/output history has not been reconciled for this UUID/source hash')

        existing = self._read_done(job_uuid, source_hash)
        if existing is not None:
            return {'uuid': job_uuid, 'result': existing['result'], 'duplicate': True}

        guard = OperationState(
            self.state_storage,
            self.state_root,
            self.OPERATION,
            f'prnsrv:{job_uuid}',
            source_hash,
            retry_mode=self.RETRY_MODE,
        )
        allocated = guard.acquire(retry_idempotent_external=True)
        if guard.cached and guard.value.get('phase') == 'complete':
            # A complete operation without its immutable done marker is evidence
            # loss. Do not silently recreate/reallocate its artifacts.
            raise ReconciliationRequired('Completed local operation has no matching done marker')

        try:
            decision = self.engine.classify_count(source)
            if not decision.should_print:
                marker = {
                    'schema_version': 1,
                    'uuid': job_uuid,
                    'source_key': source_key,
                    'source_hash': source_hash,
                    'result': 'NO_PRINT',
                    'reason_code': decision.reason_code,
                }
                self._publish_done(marker)
                result = {'uuid': job_uuid, 'result': 'NO_PRINT', 'duplicate': False}
                if not guard.cached:
                    guard.finish(result)
                return result

            count = decision.count
            route = self.engine.resolve_route(source)
            csv_key, vdf_key = self._output_keys(job_uuid, route)

            # A legacy attempt may have uploaded an artifact before the local
            # UUID/hash state existed. It is not safe to claim or replace it.
            if not guard.cached and (self.storage.exists(csv_key) or self.storage.exists(vdf_key)):
                raise ReconciliationRequired('Untracked printer artifact exists without local UUID/hash state')

            if guard.cached:
                if guard.value.get('phase') != 'accepted' or not isinstance(allocated, list):
                    raise ReconciliationRequired('Local allocation result is not in a replayable accepted phase')
                ssccs = allocated
            else:
                request_id = hashlib.sha256(
                    f'{self.OPERATION}\0{job_uuid}\0{source_hash}'.encode('utf-8')
                ).hexdigest()
                guard.external()
                ssccs = self.allocator(
                    request_id=request_id,
                    prefix=self.prefix,
                    count=count,
                    extension=self.extension,
                )
                if (not isinstance(ssccs, list) or len(ssccs) != count
                        or len(set(ssccs)) != count
                        or any(not re.fullmatch(r'\d{18}', str(value)) for value in ssccs)):
                    raise ReconciliationRequired('Extension-3 allocator returned an invalid SSCC response')
                guard.accepted(ssccs)

            csv_bytes, vdf_bytes = self.engine.build_artifacts(source, ssccs, source_key, route)
            if not isinstance(csv_bytes, bytes) or not isinstance(vdf_bytes, bytes):
                raise TypeError('prnsrv engine must return CSV and VDF as bytes')
            with tempfile.TemporaryDirectory(prefix='xtrek-prnsrv-stage-') as temp:
                csv_path = Path(temp) / f'{job_uuid}.csv'
                vdf_path = Path(temp) / f'{route}_{job_uuid}.vdf'
                csv_path.write_bytes(csv_bytes)
                vdf_path.write_bytes(vdf_bytes)
                # CSV first, then VDF. Both uploads are conditional/idempotent.
                self.storage.upload_once(str(csv_path), csv_key)
                self.storage.upload_once(str(vdf_path), vdf_key)

            actual_csv_hash = self._sha256(self._read_bytes(csv_key))
            actual_vdf_hash = self._sha256(self._read_bytes(vdf_key))
            if actual_csv_hash != self._sha256(csv_bytes) or actual_vdf_hash != self._sha256(vdf_bytes):
                raise OperationConflict('Staged printer artifacts failed read-back SHA-256 verification')

            marker = {
                'schema_version': 1,
                'uuid': job_uuid,
                'source_key': source_key,
                'source_hash': source_hash,
                'result': 'GENERATED',
                'allocation': {'namespace': f'{self.prefix}/extension-3', 'request_id': request_id if not guard.cached else None},
                'outputs': {
                    'csv': {'key': csv_key, 'sha256': actual_csv_hash},
                    'vdf': {'key': vdf_key, 'sha256': actual_vdf_hash},
                },
            }
            self._publish_done(marker)
            result = {'uuid': job_uuid, 'result': 'GENERATED', 'duplicate': False,
                      'csv_key': csv_key, 'vdf_key': vdf_key,
                      'csv_sha256': actual_csv_hash, 'vdf_sha256': actual_vdf_hash}
            if not guard.cached:
                guard.finish(result)
            return result
        except BaseException as error:
            if not guard.cached:
                guard.failed(error)
            raise
