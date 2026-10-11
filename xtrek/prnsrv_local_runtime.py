"""Explicit Celery/S3-poller entrypoint reusing the released PrnsrvFunction."""
from __future__ import annotations

import hashlib
import json
import os
from typing import Any, Callable, Mapping

from .operation_state import ReconciliationRequired
from .prnsrv_local_allocator import LocalPrnsrvAllocator
from .prnsrv_generator_runtime.app import PrnsrvFunction
from .prnsrv_generator_runtime.config import Settings
from .prnsrv_generator_runtime.storage import S3Storage


PROTECTED_UNCERTAIN_UUIDS = {'623c20fa-f90c-4478-aeae-9c6635cedc9f'}


class LocalPrnsrvFunction(PrnsrvFunction):
    """Reuse count/route/template/mapping/CSV/VDF/S3 logic with local SSCC-3."""

    def __init__(self, *, history_reconciled: Callable[[str, str], bool],
                 local_prefix: str, **kwargs: Any):
        super().__init__(**kwargs)
        self.history_reconciled = history_reconciled
        self.local_prefix = str(local_prefix)

    def process_object(self, *, bucket: str, key: str, event_id: str | None = None,
                       sscc_auth_token: str | None = None) -> dict[str, Any]:
        if sscc_auth_token:
            raise ReconciliationRequired('PRNSRV generator is not a SUZ/business signing route')
        job_uuid = self._job_uuid_from_key(key)
        if job_uuid in PROTECTED_UNCERTAIN_UUIDS:
            raise ReconciliationRequired('Protected uncertain UUID is not eligible for allocation')
        source = self._read_source(bucket, key)
        from prnsrv import canonical_source_hash
        source_hash = canonical_source_hash(source)
        existing = self.storage.get_json_optional(bucket, self._done_key(job_uuid))
        if existing is not None:
            if existing.get('result') == 'GENERATED':
                allocation = existing.get('allocation')
                expected = f'{self.local_prefix}/extension-3'
                expected_request = LocalPrnsrvAllocator.request_id(job_uuid, source_hash)
                if (not isinstance(allocation, Mapping)
                        or allocation.get('namespace') != expected
                        or allocation.get('request_id') != expected_request):
                    raise ReconciliationRequired('Existing generated marker is not proven local extension-3')
            return super().process_object(bucket=bucket, key=key, event_id=event_id)
        if not self.history_reconciled(job_uuid, source_hash):
            raise ReconciliationRequired(
                'S3 output/allocator history is not reconciled for this UUID/source hash'
            )
        return super().process_object(bucket=bucket, key=key, event_id=event_id)

    def _create_done_marker(self, *, bucket: str, job_uuid: str,
                            marker: dict[str, Any]) -> None:
        if marker.get('result') == 'GENERATED':
            marker['allocation'] = {
                'namespace': f'{self.local_prefix}/extension-3',
                'request_id': LocalPrnsrvAllocator.request_id(job_uuid, marker['source_hash']),
            }
        super()._create_done_marker(bucket=bucket, job_uuid=job_uuid, marker=marker)


def _load_source(s3_config: Mapping[str, Any], bucket: str, key: str) -> dict[str, Any]:
    from .storage import get_storage
    body = get_storage(f's3://{bucket}/{key}', s3_config).read_text(f's3://{bucket}/{key}')
    source = json.loads(body)
    if not isinstance(source, dict):
        raise ReconciliationRequired('PRNSRV source task is not a JSON object')
    return source


def process_local_prnsrv_s3_event(*, bucket: str, key: str,
                                  runtime_config: Mapping[str, Any],
                                  event_id: str | None = None,
                                  session_factory: Callable[[], Any] | None = None) -> dict[str, Any]:
    """Ready event handler called from tasks.process_s3_event for Задания/ keys.

    Required runtime fields are deliberately explicit: allocator mTLS settings,
    per-INN prefix routing, and a reconciled UUID->source-hash eligibility map.
    """
    if runtime_config.get('prnsrv_local_generator_enabled') is not True:
        return {'state': 'DISABLED'}
    input_prefix = str(runtime_config.get('prnsrv_input_prefix', 'Задания/')).strip('/') + '/'
    if not key.startswith(input_prefix):
        raise ValueError('S3 poller key is outside the PRNSRV input prefix')
    s3_config = runtime_config.get('s3_config')
    if not isinstance(s3_config, Mapping):
        raise RuntimeError('s3_config is required for local PRNSRV generation')
    raw_storage = _load_source(s3_config, bucket, key)
    job_uuid = key[len(input_prefix):].removesuffix('.json')
    manufacturer_inn = str((raw_storage.get('PasportData') or {}).get('Manufacturer_inn') or '')
    allowed_inns = {value.strip() for value in os.environ.get('XTREK_LOCAL_ALLOWED_INNS', '').split(',') if value.strip()}
    if not allowed_inns or manufacturer_inn not in allowed_inns:
        raise ReconciliationRequired('Manufacturer INN is absent from XTREK_LOCAL_ALLOWED_INNS')
    routes = runtime_config.get('prnsrv_local_allocator_by_inn') or {}
    route = routes.get(manufacturer_inn) if isinstance(routes, Mapping) else None
    if not isinstance(route, Mapping):
        raise ReconciliationRequired(f'No local extension-3 allocator route for INN {manufacturer_inn}')
    url = str(route.get('url') or '')
    prefix = str(route.get('prefix') or '')
    shared_prefix = str(runtime_config.get('sscc_prefix') or '')
    if shared_prefix and prefix != shared_prefix:
        raise ReconciliationRequired('PRNSRV route must use the shared SSCC prefix for every INN')
    cert = str(route.get('client_cert') or '')
    private_key = str(route.get('client_key') or '')
    if not all((url, prefix, cert, private_key)):
        raise RuntimeError(f'Incomplete local mTLS allocator route for INN {manufacturer_inn}')
    if session_factory is None:
        import requests
        session = requests.Session()
    else:
        session = session_factory()
    session.cert = (cert, private_key)
    session.verify = route.get('ca_bundle') or True
    allocator = LocalPrnsrvAllocator(
        url=url, prefix=prefix, session=session,
        timeout_seconds=float(route.get('timeout_seconds') or 15),
    )
    from .storage import get_storage
    input_storage = get_storage(f's3://{bucket}/{key}', s3_config)
    client = getattr(input_storage, 's3', None)
    if client is None:
        raise RuntimeError('Configured S3 storage does not expose its existing boto3 client')
    settings = Settings(
        bucket_id=bucket,
        input_prefix=input_prefix,
        output_prefix=str(runtime_config.get('prnsrv_output_prefix', 'printer-tasks/')).strip('/') + '/',
        done_prefix=str(runtime_config.get('prnsrv_done_prefix', '_prnsrv/done/')).strip('/') + '/',
        templates_bucket=runtime_config.get('prnsrv_templates_bucket') or bucket,
        templates_prefix=str(runtime_config.get('prnsrv_templates_prefix', 'config/templates/')).strip('/') + '/',
        mapping_key=runtime_config.get('prnsrv_mapping_key') or None,
        column_name=str(runtime_config.get('prnsrv_column_name', 'C1')),
        windows_csv_dir=str(runtime_config.get('prnsrv_vdf_source_dir', r'C:\tmp')),
        sscc_prefix=prefix,
        sscc_extension='3',
    )
    eligible = runtime_config.get('prnsrv_local_history_eligible') or {}
    history_reconciled = lambda target_uuid, source_hash: (
        isinstance(eligible, Mapping) and eligible.get(target_uuid) == source_hash
    )
    function = LocalPrnsrvFunction(
        storage=S3Storage(client), settings=settings, allocator=allocator,
        history_reconciled=history_reconciled, local_prefix=prefix,
    )
    return function.process_object(bucket=bucket, key=key, event_id=event_id)
