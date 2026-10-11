import hashlib
import importlib
import json
import sys
import types
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from xtrek.prnsrv_generator_runtime.storage import ObjectAlreadyExists, ObjectInfo, S3Storage
from xtrek.prnsrv_local_allocator import LocalAllocatorError, LocalPrnsrvAllocator
from xtrek.prnsrv_staging import PrnsrvStagingReceiver, StagingConflict


UUID = '8d7a2808-a64f-4a23-84eb-0404f31a03f4'
BUCKET = 'fixture-bucket'
PREFIX = '460705179'


def check_digit(payload):
    return str((-sum(int(d) * (3 if i % 2 == 0 else 1)
                     for i, d in enumerate(reversed(payload)))) % 10)


class FakePrnsrvS3:
    def __init__(self):
        self.objects = {}

    def get_bytes(self, bucket, key):
        return self.objects[(bucket, key)]

    def get_json(self, bucket, key):
        return json.loads(self.get_bytes(bucket, key))

    def get_json_optional(self, bucket, key):
        if (bucket, key) not in self.objects:
            return None
        return self.get_json(bucket, key)

    def head_optional(self, bucket, key):
        value = self.objects.get((bucket, key))
        if value is None:
            return None
        return ObjectInfo(key, None, hashlib.sha256(value).hexdigest(), len(value), datetime.now(timezone.utc))

    def actual_sha256(self, bucket, key):
        info = self.head_optional(bucket, key)
        return info.sha256 if info else None

    def put_immutable(self, bucket, key, body, *, content_type, extra_metadata=None):
        old = self.objects.get((bucket, key))
        if old is not None and old != body:
            raise RuntimeError('immutable conflict')
        self.objects[(bucket, key)] = bytes(body)
        return ObjectInfo(key, 'fixture-etag', hashlib.sha256(body).hexdigest(), len(body), None)

    def put_create_only(self, bucket, key, body, *, content_type, metadata=None):
        if (bucket, key) in self.objects:
            raise ObjectAlreadyExists(key)
        self.objects[(bucket, key)] = bytes(body)
        return ObjectInfo(key, 'fixture-etag', hashlib.sha256(body).hexdigest(), len(body), None)


class FakeAllocator:
    def __init__(self):
        self.calls = []

    def allocate(self, **kwargs):
        self.calls.append(kwargs)
        codes = []
        for serial in range(kwargs['count']):
            payload = '3' + PREFIX + f'{serial:07d}'
            codes.append(payload + check_digit(payload))
        return SimpleNamespace(ssccs=tuple(codes), allocation_id='fixture-allocation')


def install_fake_prnsrv(monkeypatch):
    fake = types.ModuleType('prnsrv')
    fake.__path__ = []
    fake.InputValidationError = type('InputValidationError', (ValueError,), {})
    fake.SSCCClient = type('SSCCClient', (), {})

    def canonical_source_hash(source):
        payload = json.dumps(source, ensure_ascii=False, sort_keys=True, separators=(',', ':')).encode()
        return hashlib.sha256(payload).hexdigest()

    def classify_count(source):
        raw = source.get('count', source.get('Quantity', 0))
        try:
            count = int(raw)
        except (ValueError, TypeError, OverflowError):
            count = 0
        return SimpleNamespace(should_print=count > 0, count=count,
                               reason_code='COUNT_EMPTY', source_field='count')

    fake.canonical_source_hash = canonical_source_hash
    fake.classify_count = classify_count
    fake.resolve_route = lambda source: '7733154124'
    fake.resolve_template_name = lambda source: source['PasportData']['Format']
    fake.build_csv = lambda ssccs, count, column_name: (column_name + '\n' + '\n'.join(ssccs)).encode()
    fake.build_vdf = lambda template, csv, source, mapping, source_path: (
        b'<Root><DataSource><SourcePath>' + source_path.encode() + b'</SourcePath></DataSource></Root>')
    monkeypatch.setitem(sys.modules, 'prnsrv', fake)
    sys.modules.pop('xtrek.prnsrv_local_runtime', None)
    return importlib.import_module('xtrek.prnsrv_local_runtime')


class FakeHttpResponse:
    status_code = 200

    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self):
        pass

    def json(self):
        return self.payload


class FakeSession:
    def __init__(self, payload):
        self.payload = payload
        self.calls = []

    def post(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return FakeHttpResponse(self.payload)


def test_local_allocator_uses_stable_mtls_request_and_extension_three():
    payloads = []
    for serial in range(2):
        body = '3' + PREFIX + f'{serial:07d}'
        payloads.append(body + check_digit(body))
    session = FakeSession({'ssccs': payloads})
    allocator = LocalPrnsrvAllocator(url='https://worker94.local', prefix=PREFIX, session=session)
    first = allocator.allocate(job_uuid=UUID, source_hash='a' * 64, count=2)
    second = allocator.allocate(job_uuid=UUID, source_hash='a' * 64, count=2)
    assert first.ssccs == tuple(payloads)
    assert first.allocation_id == second.allocation_id
    assert session.calls[0][0] == 'https://worker94.local/v1/allocate'
    assert session.calls[0][1]['json']['extension'] == 3
    assert session.calls[0][1]['json']['request_id'] == session.calls[1][1]['json']['request_id']


def test_local_allocator_rejects_cloud_signer_token_and_invalid_response():
    session = FakeSession({'ssccs': ['346070517900000057']})
    allocator = LocalPrnsrvAllocator(url='https://worker94.local', prefix=PREFIX, session=session)
    with pytest.raises(LocalAllocatorError, match='SUZ/business signer'):
        allocator.allocate(job_uuid=UUID, source_hash='b' * 64, count=1, auth_token='unused')
    session.payload = {'ssccs': ['000000000000000001']}
    with pytest.raises(LocalAllocatorError, match='wrong-prefix/invalid'):
        allocator.allocate(job_uuid=UUID, source_hash='b' * 64, count=1)


def test_local_allocator_rejects_duplicate_range():
    code = '346070517900000057'
    allocator = LocalPrnsrvAllocator(url='https://worker94.local', prefix=PREFIX,
                                    session=FakeSession({'ssccs': [code, code]}))
    with pytest.raises(LocalAllocatorError, match='duplicate SSCCs'):
        allocator.allocate(job_uuid=UUID, source_hash='b' * 64, count=2)


def test_s3_checksum_reads_body_instead_of_trusting_metadata():
    from io import BytesIO
    client = SimpleNamespace(
        head_object=lambda **kwargs: {'Metadata': {'sha256': hashlib.sha256(b'expected').hexdigest()}},
        get_object=lambda **kwargs: {'Body': BytesIO(b'corrupt')},
    )
    assert S3Storage(client).actual_sha256(BUCKET, 'printer-tasks/job.csv') == hashlib.sha256(b'corrupt').hexdigest()


def make_local_function(monkeypatch, *, history=True, count=2):
    runtime = install_fake_prnsrv(monkeypatch)
    from xtrek.prnsrv_generator_runtime.config import Settings
    storage = FakePrnsrvS3()
    allocator = FakeAllocator()
    source_key = f'Задания/{UUID}.json'
    source = {'count': count, 'PasportData': {'Format': 'fixture'},
              'Manufacturer_inn': '7733154124'}
    storage.objects[(BUCKET, source_key)] = json.dumps(source).encode()
    storage.objects[(BUCKET, 'templates/fixture.vdf')] = b'<Root/>'
    storage.objects[(BUCKET, 'config/mapping.json')] = b'[]'
    function = runtime.LocalPrnsrvFunction(
        storage=storage, settings=Settings(
            bucket_id=BUCKET, templates_bucket=BUCKET, templates_prefix='templates/',
            mapping_key='config/mapping.json', sscc_prefix=PREFIX, sscc_extension='3',
        ),
        allocator=allocator, history_reconciled=lambda *_: history,
        local_prefix=PREFIX,
    )
    return function, storage, allocator, source_key, source


def test_existing_prnsrv_function_generates_immutable_outputs_and_local_marker(monkeypatch):
    function, storage, allocator, key, _ = make_local_function(monkeypatch)
    result = function.process_object(bucket=BUCKET, key=key, event_id='fixture-event')
    marker = storage.get_json(BUCKET, f'_prnsrv/done/{UUID}.done')
    assert result['result'] == 'GENERATED'
    assert marker['allocation']['namespace'] == f'{PREFIX}/extension-3'
    assert marker['outputs']['csv']['key'] == f'printer-tasks/{UUID}.csv'
    assert marker['outputs']['vdf']['key'] == f'printer-tasks/7733154124_{UUID}.vdf'
    assert len(allocator.calls) == 1
    duplicate = function.process_object(bucket=BUCKET, key=key)
    assert duplicate['duplicate'] is True
    assert len(allocator.calls) == 1


def test_generated_marker_requires_matching_local_allocation_request(monkeypatch):
    function, storage, allocator, key, _ = make_local_function(monkeypatch)
    function.process_object(bucket=BUCKET, key=key)
    marker = storage.get_json(BUCKET, f'_prnsrv/done/{UUID}.done')
    marker['allocation']['request_id'] = 'unrelated-request'
    storage.objects[(BUCKET, f'_prnsrv/done/{UUID}.done')] = json.dumps(marker).encode()
    with pytest.raises(Exception, match='not proven local extension-3'):
        function.process_object(bucket=BUCKET, key=key)
    assert len(allocator.calls) == 1


@pytest.mark.parametrize('inn', ['7733154124', '9718180660', '9723161905'])
def test_runtime_requires_shared_prefix_for_every_inn(monkeypatch, inn):
    runtime = install_fake_prnsrv(monkeypatch)
    monkeypatch.setenv('XTREK_LOCAL_ALLOWED_INNS', inn)
    monkeypatch.setattr(runtime, '_load_source', lambda *_: {'PasportData': {'Manufacturer_inn': inn}})
    config = {'prnsrv_local_generator_enabled': True, 's3_config': {}, 'sscc_prefix': PREFIX,
              'prnsrv_local_allocator_by_inn': {inn: {'prefix': '123456789'}}}
    with pytest.raises(Exception, match='shared SSCC prefix'):
        runtime.process_local_prnsrv_s3_event(bucket=BUCKET, key=f'Задания/{UUID}.json', runtime_config=config)


def test_local_function_blocks_unreconciled_and_protected_uuid(monkeypatch):
    function, _, allocator, key, _ = make_local_function(monkeypatch, history=False)
    with pytest.raises(Exception, match='history is not reconciled'):
        function.process_object(bucket=BUCKET, key=key)
    assert allocator.calls == []

    protected = '623c20fa-f90c-4478-aeae-9c6635cedc9f'
    function, storage, allocator, _, source = make_local_function(monkeypatch)
    protected_key = f'Задания/{protected}.json'
    storage.objects[(BUCKET, protected_key)] = json.dumps(source).encode()
    with pytest.raises(Exception, match='Protected uncertain UUID'):
        function.process_object(bucket=BUCKET, key=protected_key)
    assert allocator.calls == []


def test_old_generated_marker_without_extension_three_proof_is_held(monkeypatch):
    function, storage, allocator, key, source = make_local_function(monkeypatch)
    from prnsrv import canonical_source_hash
    storage.objects[(BUCKET, f'_prnsrv/done/{UUID}.done')] = json.dumps({
        'uuid': UUID, 'source_hash': canonical_source_hash(source), 'result': 'GENERATED',
        'outputs': {'csv': {'key': 'printer-tasks/a.csv', 'sha256': 'a'},
                    'vdf': {'key': 'printer-tasks/a.vdf', 'sha256': 'b'}},
    }).encode()
    with pytest.raises(Exception, match='not proven local extension-3'):
        function.process_object(bucket=BUCKET, key=key)
    assert allocator.calls == []


def test_empty_count_reuses_existing_no_print_contract_without_allocation(monkeypatch):
    function, storage, allocator, key, _ = make_local_function(monkeypatch, count='')
    result = function.process_object(bucket=BUCKET, key=key)
    marker = storage.get_json(BUCKET, f'_prnsrv/done/{UUID}.done')
    assert result['result'] == 'NO_PRINT'
    assert marker['reason_code'] == 'COUNT_EMPTY'
    assert 'allocation' not in marker
    assert allocator.calls == []


def stage_fixture(storage):
    from xtrek.prnsrv_generator_runtime.storage import ObjectInfo
    job_uuid = UUID
    prefix = 'printer-tasks/'
    csv_key = f'{prefix}{job_uuid}.csv'
    vdf_key = f'{prefix}7733154124_{job_uuid}.vdf'
    csv, vdf = b'C1\nSSCC', b'<Root/>'
    storage.objects[(BUCKET, csv_key)] = csv
    storage.objects[(BUCKET, vdf_key)] = vdf
    marker = {
        'schema_version': 1, 'uuid': job_uuid,
        'source_key': f'Задания/{job_uuid}.json', 'source_hash': 'a' * 64,
        'result': 'GENERATED', 'allocation': {'namespace': f'{PREFIX}/extension-3'},
        'outputs': {
            'csv': {'key': csv_key, 'sha256': hashlib.sha256(csv).hexdigest()},
            'vdf': {'key': vdf_key, 'sha256': hashlib.sha256(vdf).hexdigest()},
        },
    }
    storage.objects[(BUCKET, f'_prnsrv/done/{job_uuid}.done')] = json.dumps(marker).encode()


def test_bucket_scoped_receiver_matches_prnsrvfunction_marker_and_receipt_contract(tmp_path):
    storage = FakePrnsrvS3()
    stage_fixture(storage)
    receiver = PrnsrvStagingReceiver(
        storage=storage, bucket=BUCKET, output_root='', receipt_root='',
        staging_root=str(tmp_path / 'review-stage'), allowlisted_uuids=[UUID],
    )
    result = receiver.stage(UUID)
    receipt = storage.get_json(BUCKET, f'printer-task-stage-receipts/{UUID}.json')
    assert result['state'] == 'STAGED'
    assert receipt['done_key'] == f's3://{BUCKET}/_prnsrv/done/{UUID}.done'
    assert receipt['source_key'] == f'Задания/{UUID}.json'
    assert set(receipt['files']) == {'csv', 'vdf'}
    assert (tmp_path / 'review-stage' / UUID / 'manifest.json').is_file()
    assert receiver.stage(UUID)['state'] == 'STAGED'


def test_bucket_scoped_receiver_requires_allowlist_hashes_and_safe_root(tmp_path):
    storage = FakePrnsrvS3()
    stage_fixture(storage)
    receiver = PrnsrvStagingReceiver(
        storage=storage, bucket=BUCKET, output_root='', receipt_root='',
        staging_root=str(tmp_path / 'review-stage'), allowlisted_uuids=[],
    )
    with pytest.raises(StagingConflict, match='allowlist'):
        receiver.stage(UUID)
    receiver.allowlisted.add(UUID)
    storage.objects[(BUCKET, f'printer-tasks/{UUID}.csv')] = b'corrupt'
    with pytest.raises(StagingConflict, match='hash does not match'):
        receiver.stage(UUID)
    assert (BUCKET, f'printer-task-stage-receipts/{UUID}.json') not in storage.objects
    with pytest.raises(ValueError, match='watched or printer path'):
        PrnsrvStagingReceiver(
            storage=storage, bucket=BUCKET, output_root='', receipt_root='',
            staging_root='C:/tmp', allowlisted_uuids=[UUID],
        )
