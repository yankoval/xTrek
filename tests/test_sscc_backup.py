from datetime import datetime, timedelta, timezone
import io
import sqlite3

from botocore.exceptions import ClientError
import pytest

from xtrek.sscc_backup import BackupStore, CANDIDATE, ROOT, SLOTS
from xtrek.sscc_local import Ledger

PREFIX = '460705179'
NOW = datetime(2026, 10, 15, 12, tzinfo=timezone.utc)


class S3:
    def __init__(self):
        self.objects = {}
        self.history = []
        self.fail_put = None
        self.corrupt = None
        self.versioning = {}

    def get_bucket_versioning(self, **kwargs):
        return self.versioning

    def get_object(self, Bucket, Key):
        if Key not in self.objects:
            raise ClientError({'Error': {'Code': 'NoSuchKey'}}, 'GetObject')
        body, metadata = self.objects[Key]
        if self.corrupt == Key:
            body = body[:-4]
        return {'Body': io.BytesIO(body), 'Metadata': metadata.copy()}

    def put_object(self, Bucket, Key, Body, Metadata, **kwargs):
        if self.fail_put == Key:
            raise TimeoutError('write failed')
        self.objects[Key] = (Body, Metadata.copy())
        self.history.append(('put', Key, len(self.objects)))

    def delete_object(self, Bucket, Key):
        self.objects.pop(Key, None)
        self.history.append(('delete', Key, len(self.objects)))


@pytest.fixture
def setup(tmp_path):
    path = tmp_path / 'ledger.sqlite3'
    ledger = Ledger.initialize(str(path), PREFIX)
    client = S3()
    store = BackupStore(client, 'dedicated-backups', PREFIX, 'operation-state')
    return path, ledger, client, store


def rows(path, table):
    with sqlite3.connect(path) as db:
        return db.execute('SELECT * FROM ' + table + ' ORDER BY 1').fetchall()


def test_five_slots_bounded_and_restore_matches_every_table(setup, tmp_path):
    path, ledger, client, store = setup
    for i in range(15):
        ledger.allocate(str(i), PREFIX, i + 1)
        result = store.run(path, NOW + timedelta(days=i // 4, minutes=i))
        assert result['slots'] <= 5
    assert len(client.objects) == 5
    assert max(count for _, _, count in client.history) <= 6
    assert set(client.objects) == {ROOT + slot for slot in SLOTS}
    newest = max(SLOTS[:3], key=lambda slot: client.objects[ROOT + slot][1]['captured-at'])
    destination = tmp_path / 'isolated'
    restored = store.restore(newest, destination)
    assert restored['next_serial'] == sum(range(1, 16))
    for table in ('ledger_meta', 'counters', 'allocations'):
        assert rows(path, table) == rows(destination / 'ledger.sqlite3', table)
    with pytest.raises(FileExistsError):
        store.restore(newest, destination)
    assert sorted({v[1]['day'] for k, v in client.objects.items() if '/daily-' in k}) == [
        '2026-10-16', '2026-10-17']


def test_unchanged_content_skips_upload_but_creates_next_daily_checkpoint(setup):
    path, ledger, client, store = setup
    store.run(path, NOW)
    writes = sum(op == 'put' for op, _, _ in client.history)
    assert store.run(path, NOW + timedelta(minutes=5))['state'] == 'unchanged'
    assert sum(op == 'put' for op, _, _ in client.history) == writes
    result = store.run(path, NOW + timedelta(days=1))
    assert result['written_slots'] == ['daily-0.sqlite3.gz']
    assert len(client.objects) == 2


def test_upload_failure_keeps_all_old_snapshots_and_retries_after_restart(setup):
    path, ledger, client, store = setup
    for i in range(3):
        ledger.allocate(str(i), PREFIX, 1)
        store.run(path, NOW + timedelta(minutes=i))
    old = dict(client.objects)
    ledger.allocate('new', PREFIX, 1)
    client.fail_put = ROOT + SLOTS[0]
    with pytest.raises(TimeoutError):
        store.run(path, NOW + timedelta(minutes=4))
    assert all(client.objects[k] == v for k, v in old.items())
    assert ROOT + CANDIDATE in client.objects
    client.fail_put = None
    restarted = BackupStore(client, 'dedicated-backups', PREFIX, 'operation-state')
    assert restarted.run(path, NOW + timedelta(minutes=5))['state'] == 'verified'
    assert ROOT + CANDIDATE not in client.objects


def test_bad_candidate_download_never_overwrites_confirmed_slots(setup):
    path, ledger, client, store = setup
    store.run(path, NOW)
    old = dict(client.objects)
    ledger.allocate('new', PREFIX, 1)
    client.corrupt = ROOT + CANDIDATE
    with pytest.raises((EOFError, RuntimeError)):
        store.run(path, NOW + timedelta(minutes=1))
    assert all(client.objects[k] == v for k, v in old.items())


def test_failed_destination_verification_retains_candidate_and_other_slots(setup):
    path, ledger, client, store = setup
    for i in range(3):
        ledger.allocate(str(i), PREFIX, 1)
        store.run(path, NOW + timedelta(minutes=i))
    old = dict(client.objects)
    ledger.allocate('new', PREFIX, 1)
    # Corrupt only reads after the PUT, rather than the preflight read.
    original = client.put_object
    def put(**kw):
        original(**kw)
        if kw['Key'] == ROOT + SLOTS[0]:
            client.corrupt = kw['Key']
    client.put_object = put
    with pytest.raises((EOFError, RuntimeError)):
        store.run(path, NOW + timedelta(minutes=5))
    assert ROOT + CANDIDATE in client.objects
    assert all(client.objects[k] == v for k, v in old.items() if k != ROOT + SLOTS[0])


def test_source_rollback_and_clock_rollback_are_rejected(setup, tmp_path):
    path, ledger, client, store = setup
    previous = tmp_path / 'old.sqlite3'
    previous.write_bytes(path.read_bytes())
    ledger.allocate('committed', PREFIX, 1)
    store.run(path, NOW)
    before = list(client.history)
    with pytest.raises(RuntimeError, match='watermark'):
        store.run(previous, NOW + timedelta(minutes=1))
    with pytest.raises(RuntimeError, match='Clock'):
        store.run(path, NOW - timedelta(minutes=1))
    assert client.history == before


def test_missing_corrupt_or_wrong_namespace_source_is_rejected(setup, tmp_path):
    path, ledger, client, store = setup
    with pytest.raises(RuntimeError, match='missing'):
        store.run(tmp_path / 'missing.sqlite3', NOW)
    with sqlite3.connect(path) as db:
        db.execute('UPDATE counters SET next_serial=100')
    with pytest.raises(RuntimeError, match='counter'):
        store.run(path, NOW)
    assert not client.objects


@pytest.mark.parametrize('versioning', ['Enabled', 'Suspended'])
def test_versioning_refused_to_prevent_unbounded_old_versions(setup, versioning):
    path, _, client, store = setup
    client.versioning = {'Status': versioning}
    with pytest.raises(RuntimeError, match='Versioning'):
        store.run(path, NOW)
    assert not client.objects


def test_legacy_bucket_and_unrecognized_keys_are_untouchable(setup):
    path, _, client, store = setup
    with pytest.raises(ValueError):
        BackupStore(client, 'operation-state', PREFIX, 'operation-state')
    with pytest.raises(ValueError):
        store.get('../../operations/v1/protected')
    with pytest.raises(ValueError):
        store.put_verified('legacy.sqlite3', b'', {})
    with pytest.raises(ValueError):
        store.restore(CANDIDATE, path.parent / 'restore')
    assert not client.history


def test_restart_refuses_source_older_than_uncommitted_candidate(setup, tmp_path):
    path, ledger, client, store = setup
    store.run(path, NOW)
    old = tmp_path / 'old.sqlite3'
    old.write_bytes(path.read_bytes())
    ledger.allocate('issued', PREFIX, 1)
    client.fail_put = ROOT + SLOTS[1]
    with pytest.raises(TimeoutError):
        store.run(path, NOW + timedelta(minutes=1))
    candidate = client.objects[ROOT + CANDIDATE]
    client.fail_put = None
    with pytest.raises(RuntimeError, match='watermark'):
        store.run(old, NOW + timedelta(minutes=2))
    assert client.objects[ROOT + CANDIDATE] == candidate


def test_lost_put_response_can_recover_without_extra_rolling_slots(setup):
    path, ledger, client, store = setup
    store.run(path, NOW)
    ledger.allocate('new', PREFIX, 1)
    put = client.put_object
    def lost_response(**kw):
        put(**kw)
        if kw['Key'] == ROOT + SLOTS[1]:
            raise TimeoutError('response lost after commit')
    client.put_object = lost_response
    with pytest.raises(TimeoutError):
        store.run(path, NOW + timedelta(minutes=1))
    assert ROOT + CANDIDATE in client.objects
    client.put_object = put
    assert store.run(path, NOW + timedelta(minutes=2))['state'] == 'unchanged'
    assert len(client.objects) == 2


def test_daily_checkpoint_preserves_last_prior_day_not_first_new_day(setup):
    path, ledger, client, store = setup
    store.run(path, NOW)
    ledger.allocate('yesterday', PREFIX, 3)
    store.run(path, NOW + timedelta(hours=2))
    ledger.allocate('today', PREFIX, 4)
    store.run(path, NOW + timedelta(days=1))
    daily = store.get('daily-0.sqlite3.gz')
    assert daily['metadata']['next-serial'] == '3'
    assert daily['metadata']['day'] == NOW.date().isoformat()
    assert max(int(store.get(s)['metadata']['next-serial']) for s in SLOTS[:3]) == 7


def test_yandex_metadata_header_casing_does_not_break_checksum_checks(setup):
    path, _, client, store = setup
    get = client.get_object
    def title_case(**kw):
        result = get(**kw)
        result['Metadata'] = {k.title(): v for k, v in result['Metadata'].items()}
        return result
    client.get_object = title_case
    assert store.run(path, NOW)['state'] == 'verified'
    assert store.run(path, NOW + timedelta(minutes=5))['state'] == 'unchanged'
