import concurrent.futures
import os
import sqlite3
import subprocess
import sys
import tempfile

import pytest

from xtrek.sscc_local import AllocationConflict, Ledger


def test_idempotency_survives_restart_and_parameter_conflicts():
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, 'ledger.sqlite3')
        first = Ledger.initialize(path, '460705179')
        result = first.allocate('stable-operation-hash', '460705179', 3, 3)
        restarted = Ledger(path, '460705179')
        assert restarted.allocate('stable-operation-hash', '460705179', 3, 3) == result
        assert restarted.allocate('next', '460705179', 1, 3)['ssccs'][0] != result['ssccs'][0]
        with pytest.raises(AllocationConflict):
            restarted.allocate('stable-operation-hash', '460705179', 2, 3)


def test_parallel_requests_reserve_disjoint_ranges():
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, 'ledger.sqlite3')
        ledger = Ledger.initialize(path, '460705179')
        def allocate(index):
            return ledger.allocate(f'request-{index}', '460705179', 5, 3)['ssccs']
        with concurrent.futures.ThreadPoolExecutor(max_workers=12) as pool:
            batches = list(pool.map(allocate, range(24)))
        flattened = [code for batch in batches for code in batch]
        assert len(flattened) == len(set(flattened)) == 120


def test_commit_is_atomic_and_database_is_private():
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, 'ledger.sqlite3')
        ledger = Ledger.initialize(path, '460705179')
        ledger.allocate('one', '460705179', 2, 3)
        db = ledger._connect()
        assert db.execute('SELECT next_serial FROM counters').fetchone()[0] == 2
        assert db.execute('SELECT COUNT(*) FROM allocations').fetchone()[0] == 1
        db.close()
        assert os.stat(path).st_mode & 0o777 == 0o600


def test_namespace_exhaustion_does_not_advance_counter():
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, 'ledger.sqlite3')
        ledger = Ledger.initialize(path, '12345678901234')
        with pytest.raises(OverflowError):
            ledger.allocate('too-many', '12345678901234', 101, 3)
        db = ledger._connect()
        assert db.execute('SELECT next_serial FROM counters').fetchone()[0] == 0
        assert db.execute('SELECT COUNT(*) FROM allocations').fetchone()[0] == 0
        db.close()


def test_sscc_check_digit_and_result_shape():
    with tempfile.TemporaryDirectory() as tmp:
        result = Ledger.initialize(os.path.join(tmp, 'ledger.sqlite3'), '460705179').allocate(
            'one', '460705179', 1, 3)
        code = result['ssccs'][0]
        assert len(code) == 18 and code.isdigit() and code.startswith('3460705179')
        total = sum(int(d) * (3 if i % 2 == 0 else 1)
                    for i, d in enumerate(reversed(code[:-1])))
        assert int(code[-1]) == (-total) % 10


def test_missing_database_and_missing_counter_fail_closed():
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, 'ledger.sqlite3')
        with pytest.raises(RuntimeError, match='missing'):
            Ledger(path, '460705179')
        Ledger.initialize(path, '460705179').allocate('one', '460705179', 1, 3)
        os.unlink(path)
        with pytest.raises(RuntimeError, match='missing'):
            Ledger(path, '460705179')

        Ledger.initialize(path, '460705179')
        db = sqlite3.connect(path)
        db.execute('DELETE FROM counters')
        db.commit()
        db.close()
        with pytest.raises(RuntimeError, match='namespace/counter'):
            Ledger(path, '460705179')

        mismatch_path = os.path.join(tmp, 'counter-mismatch.sqlite3')
        Ledger.initialize(mismatch_path, '460705179').allocate('logged', '460705179', 2, 3)
        db = sqlite3.connect(mismatch_path)
        db.execute('UPDATE counters SET next_serial=1')
        db.commit()
        db.close()
        with pytest.raises(RuntimeError, match='namespace/counter'):
            Ledger(mismatch_path, '460705179')


def test_database_cannot_be_reinitialized_or_reused_for_another_namespace():
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, 'ledger.sqlite3')
        Ledger.initialize(path, '460705179')
        with pytest.raises(FileExistsError):
            Ledger.initialize(path, '460705179')
        with pytest.raises(RuntimeError, match='namespace/counter'):
            Ledger(path, '460705178')


def test_failure_before_commit_rolls_back_counter_and_response(tmp_path):
    path = os.path.join(tmp_path, 'ledger.sqlite3')
    ledger = Ledger.initialize(path, '460705179')
    connect = ledger._connect

    class FailBeforeCommit:
        def __init__(self, db):
            self.db = db
        def __getattr__(self, name):
            return getattr(self.db, name)
        def execute(self, sql, *args):
            if sql == 'COMMIT':
                raise OSError('simulated disk failure before commit')
            return self.db.execute(sql, *args)

    ledger._connect = lambda: FailBeforeCommit(connect())
    with pytest.raises(OSError, match='disk failure'):
        ledger.allocate('not-committed', '460705179', 2, 3)
    ledger._connect = connect
    db = connect()
    assert db.execute('SELECT next_serial FROM counters').fetchone()[0] == 0
    assert db.execute('SELECT COUNT(*) FROM allocations').fetchone()[0] == 0
    db.close()


def test_committed_request_replays_and_backup_preserves_counter(tmp_path):
    path = os.path.join(tmp_path, 'ledger.sqlite3')
    ledger = Ledger.initialize(path, '460705179')
    expected = ledger.allocate('response-lost', '460705179', 2, 3)
    assert ledger.allocate('response-lost', '460705179', 2, 3) == expected
    backup_path = os.path.join(tmp_path, 'backup.sqlite3')
    source = sqlite3.connect(path)
    backup = sqlite3.connect(backup_path)
    source.backup(backup)
    backup.close()
    source.close()
    restored = Ledger(backup_path, '460705179')
    assert restored.allocate('response-lost', '460705179', 2, 3) == expected
    assert restored.allocate('after-backup', '460705179', 1, 3)['ssccs'][0] not in expected['ssccs']


@pytest.mark.parametrize('stage,expected_next', [('before', 2), ('after', 2)])
def test_process_death_around_commit_recovers_one_allocation(tmp_path, stage, expected_next):
    path = os.path.join(tmp_path, 'ledger.sqlite3')
    Ledger.initialize(path, '460705179')
    child_code = r'''import os, sys
from xtrek.sscc_local import Ledger
path, stage = sys.argv[1:]
ledger = Ledger(path, '460705179')
connect = ledger._connect
class AbruptExit:
    def __init__(self, db): self.db = db
    def __getattr__(self, name): return getattr(self.db, name)
    def execute(self, sql, *args):
        if sql == 'COMMIT' and stage == 'before': os._exit(91)
        result = self.db.execute(sql, *args)
        if sql == 'COMMIT' and stage == 'after': os._exit(92)
        return result
ledger._connect = lambda: AbruptExit(connect())
ledger.allocate('crash-boundary', '460705179', 2, 3)
'''
    child = subprocess.run([sys.executable, '-c', child_code, path, stage],
                           capture_output=True, timeout=15)
    assert child.returncode == (91 if stage == 'before' else 92)
    recovered = Ledger(path, '460705179')
    replay = recovered.allocate('crash-boundary', '460705179', 2, 3)
    assert len(replay['ssccs']) == 2
    db = recovered._connect()
    assert db.execute('SELECT next_serial FROM counters').fetchone()[0] == expected_next
    assert db.execute('SELECT COUNT(*) FROM allocations').fetchone()[0] == 1
    db.close()
