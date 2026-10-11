import json
from dataclasses import dataclass

import pytest

from xtrek.operation_state import OperationConflict, ReconciliationRequired
from xtrek.prnsrv_local import PrnsrvLocalAdapter


UUID = '8d7a2808-a64f-4a23-84eb-0404f31a03f4'
SOURCE_KEY = f'Задания/{UUID}.json'
ROOT = 's3://bucket'
STATE_ROOT = 's3://bucket/_operation_state'
SOURCE = {'Quantity': '2', 'Article': 'fixture', 'PasportData': {'Format': '32x32'}}


class FakeStorage:
    def __init__(self):
        self.objects = {}
        self.states = {}

    def exists(self, path):
        return path in self.objects or path in self.states

    def read_text(self, path):
        if path in self.objects:
            return self.objects[path].decode('utf-8')
        return self.states[path][0]

    def read_bytes(self, path):
        return self.objects[path]

    def upload_once(self, local_path, remote_path):
        from pathlib import Path
        body = Path(local_path).read_bytes()
        prior = self.objects.get(remote_path)
        if prior is not None and prior != body:
            raise OperationConflict(f'Existing immutable file differs: {remote_path}')
        self.objects[remote_path] = body

    def read_lock_object(self, path):
        return self.states.get(path)

    def write_lock_object(self, path, content, etag, *, tags=None):
        old = self.states.get(path)
        old_etag = old[1] if old else None
        if old_etag != etag:
            return None
        new_etag = str(int(old_etag or '0') + 1)
        self.states[path] = (content, new_etag)
        self.objects[path] = content.encode('utf-8')
        return new_etag


@dataclass
class CountDecision:
    should_print: bool
    count: int = 0
    reason_code: str = 'COUNT_EMPTY'


class FakeEngine:
    def __init__(self, should_print=True):
        self.should_print = should_print
        self.build_calls = 0
        self.fail_build_once = False

    def classify_count(self, source):
        count = int(source.get('count', source.get('Quantity', 0)) or 0)
        return CountDecision(self.should_print and count > 0, count, 'COUNT_EMPTY')

    def resolve_route(self, source):
        return '7733154124'

    def build_artifacts(self, source, ssccs, source_key, route):
        self.build_calls += 1
        if self.fail_build_once:
            self.fail_build_once = False
            raise RuntimeError('simulated worker crash after allocation receipt')
        csv_body = ('C1\n' + ''.join(f'00{sscc}\n' for sscc in ssccs)).encode()
        return csv_body, f'<VDF>{source_key}:{route}</VDF>'.encode()


class IdempotentLocalAllocator:
    def __init__(self):
        self.requests = {}
        self.calls = []
        self.drop_first_response = False

    def __call__(self, *, request_id, prefix, count, extension):
        self.calls.append((request_id, prefix, count, extension))
        assert extension == 3
        assert prefix == '460705179'
        if request_id not in self.requests:
            self.requests[request_id] = [f'3460705179{i:07d}{(i * 3) % 10}' for i in range(count)]
        if self.drop_first_response:
            self.drop_first_response = False
            raise TimeoutError('allocator committed but response was lost')
        return self.requests[request_id]


def make_adapter(*, storage=None, engine=None, allocator=None, prefix='460705179', extension=3,
                 history_reconciled=lambda job_uuid, source_hash: True):
    storage = storage or FakeStorage()
    engine = engine or FakeEngine()
    allocator = allocator or IdempotentLocalAllocator()
    adapter = PrnsrvLocalAdapter(
        storage=storage,
        state_storage=storage,
        state_root=STATE_ROOT,
        output_root=ROOT,
        engine=engine,
        allocator=allocator,
        history_reconciled=history_reconciled,
        prefix=prefix,
        extension=extension,
    )
    return adapter, storage, engine, allocator


def test_creates_immutable_files_in_separate_printer_tasks_contract():
    adapter, storage, _, allocator = make_adapter()
    result = adapter.process(source_key=SOURCE_KEY, source=SOURCE)
    csv_key = f'{ROOT}/printer-tasks/{UUID}.csv'
    vdf_key = f'{ROOT}/printer-tasks/7733154124_{UUID}.vdf'
    done_key = f'{ROOT}/_prnsrv/done/{UUID}.done'

    assert result['result'] == 'GENERATED'
    assert set((csv_key, vdf_key, done_key)).issubset(storage.objects)
    marker = json.loads(storage.objects[done_key])
    assert marker['source_hash'] == adapter.source_hash(SOURCE)
    assert marker['allocation']['namespace'] == '460705179/extension-3'
    assert allocator.calls[0][3] == 3
    assert not any('C:\\tmp' in key for key in storage.objects)


def test_repeated_event_reuses_done_marker_and_does_not_allocate_twice():
    adapter, _, _, allocator = make_adapter()
    first = adapter.process(source_key=SOURCE_KEY, source=SOURCE)
    second = adapter.process(source_key=SOURCE_KEY, source=SOURCE)
    assert first['result'] == 'GENERATED'
    assert second == {'uuid': UUID, 'result': 'GENERATED', 'duplicate': True}
    assert len(allocator.calls) == 1


def test_lost_allocator_response_retries_same_local_request_id():
    allocator = IdempotentLocalAllocator()
    allocator.drop_first_response = True
    adapter, _, _, _ = make_adapter(allocator=allocator)
    with pytest.raises(TimeoutError):
        adapter.process(source_key=SOURCE_KEY, source=SOURCE)
    result = adapter.process(source_key=SOURCE_KEY, source=SOURCE)
    assert result['result'] == 'GENERATED'
    assert len(allocator.calls) == 2
    assert allocator.calls[0][0] == allocator.calls[1][0]
    assert len(allocator.requests) == 1


def test_crash_after_allocator_receipt_resumes_without_new_allocation():
    engine = FakeEngine()
    engine.fail_build_once = True
    allocator = IdempotentLocalAllocator()
    adapter, _, _, _ = make_adapter(engine=engine, allocator=allocator)
    with pytest.raises(RuntimeError, match='simulated worker crash'):
        adapter.process(source_key=SOURCE_KEY, source=SOURCE)
    result = adapter.process(source_key=SOURCE_KEY, source=SOURCE)
    assert result['result'] == 'GENERATED'
    assert len(allocator.calls) == 1


def test_changed_source_hash_conflicts_and_preserves_prior_assignment():
    adapter, _, _, allocator = make_adapter()
    adapter.process(source_key=SOURCE_KEY, source=SOURCE)
    changed = dict(SOURCE, Quantity='3')
    with pytest.raises(OperationConflict):
        adapter.process(source_key=SOURCE_KEY, source=changed)
    assert len(allocator.calls) == 1


def test_existing_untracked_partial_output_requires_reconciliation_before_allocation():
    storage = FakeStorage()
    storage.objects[f'{ROOT}/printer-tasks/{UUID}.csv'] = b'legacy partial'
    adapter, _, _, allocator = make_adapter(storage=storage)
    with pytest.raises(ReconciliationRequired, match='Untracked printer artifact'):
        adapter.process(source_key=SOURCE_KEY, source=SOURCE)
    assert allocator.calls == []


def test_empty_quantity_writes_no_print_marker_without_allocator():
    adapter, storage, _, allocator = make_adapter(engine=FakeEngine(should_print=False))
    result = adapter.process(source_key=SOURCE_KEY, source={'Quantity': ''})
    marker = json.loads(storage.objects[f'{ROOT}/_prnsrv/done/{UUID}.done'])
    assert result['result'] == 'NO_PRINT'
    assert marker['reason_code'] == 'COUNT_EMPTY'
    assert allocator.calls == []


def test_local_allocator_extension_cannot_be_changed_to_cloud_extension_zero():
    with pytest.raises(ValueError, match='restricted to extension 3'):
        make_adapter(extension=0)


def test_history_not_reconciled_blocks_allocator_before_any_request():
    adapter, _, _, allocator = make_adapter(history_reconciled=lambda *_: False)
    with pytest.raises(ReconciliationRequired, match='history has not been reconciled'):
        adapter.process(source_key=SOURCE_KEY, source=SOURCE)
    assert allocator.calls == []


def test_protected_uncertain_uuid_cannot_reach_allocator():
    adapter, _, _, allocator = make_adapter()
    protected = '623c20fa-f90c-4478-aeae-9c6635cedc9f'
    with pytest.raises(ReconciliationRequired, match='Protected uncertain UUID'):
        adapter.process(source_key=f'Задания/{protected}.json', source=SOURCE)
    assert allocator.calls == []
