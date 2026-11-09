"""Offline cohort, legacy-message compatibility and S3 read-only tests."""
import io
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import Mock

import pytest
from xtrek import max_queue_pilot as pilot

KEY = 'equipment-tasks/T-00000000-0000-4000-8000-000000000001.json'


@pytest.fixture
def event(tmp_path, monkeypatch):
    now = datetime.now(timezone.utc)
    path = tmp_path / 'cohort.json'
    path.write_text(json.dumps({'not_before': (now-timedelta(minutes=2)).isoformat(), 'keys': [KEY]}))
    monkeypatch.setenv('XTREK_MAX_QUEUE_PILOT_COHORT', str(path))
    url = (f'https://{pilot.HOST}/{pilot.BUCKET}/{KEY}?X-Amz-Date={now:%Y%m%dT%H%M%SZ}'
           '&X-Amz-Expires=172800&X-Amz-Signature=' + 'a'*64)
    return dict(event_id=pilot.identity(KEY), key=KEY, created_at=(now-timedelta(minutes=1)).isoformat(),
                ready_at=(now-timedelta(seconds=2)).isoformat(), url=url)


def test_post_body_and_visible_format_identical_to_legacy(event, monkeypatch):
    # Exercise the actual old notifier, not a copied expected template.
    from test_task_notifier import LocalNotifierTests
    legacy = LocalNotifierTests(); legacy.setUp()
    try:
        legacy.s3.generate_presigned_url = lambda *a, **kw: event['url']
        monkeypatch.setattr(legacy.notifier, '_verify_link', lambda url: True)
        monkeypatch.setattr('xtrek.task_notifier.requests.get', legacy.get)
        old_post = Mock(side_effect=legacy.post)
        monkeypatch.setattr('xtrek.task_notifier.requests.post', old_post)
        assert legacy.invoke().state == 'delivered'
        new_post = Mock(return_value=Mock(status_code=200, json=lambda: {'message': {'body': {'mid': 'mid.test'}}}))
        assert pilot.send(event, new_post, 'synthetic-token', 'synthetic-ca', 1) == 'mid.test'
        assert new_post.call_args.kwargs['json'] == old_post.call_args.kwargs['json']
        assert new_post.call_args.kwargs['params'] == old_post.call_args.kwargs['params']
        assert new_post.call_args.kwargs['json']['format'] == 'html'
        assert new_post.call_args.kwargs['params']['disable_link_preview'] == 'true'
        assert 'attachments' not in new_post.call_args.kwargs['json']
    finally:
        legacy.doCleanups()


@pytest.mark.parametrize('field,value', [
    ('created_at', '2000-01-01T00:00:00Z'), ('key', 'equipment-tasks/T-old.json'),
    ('event_id', 'untrusted'), ('url', 'https://other.invalid/task.json'),
])
def test_old_or_invalid_event_never_posts(event, field, value):
    event[field] = value
    post = Mock()
    with pytest.raises(pilot.PilotInvalid):
        pilot.send(event, post, 'synthetic', 'synthetic-ca', 1)
    post.assert_not_called()


def test_prepare_reads_existing_version_without_writes(event):
    content = json.dumps({'id': Path(KEY).stem, 'date': event['created_at']}).encode()
    storage = Mock(spec=['head_object', 'get_object', 'generate_presigned_url'])
    storage.head_object.return_value = {'ContentLength': len(content), 'ETag': 'etag', 'VersionId': 'v1',
                                        'LastModified': pilot.utc(event['ready_at'])}
    storage.get_object.return_value = {'Body': io.BytesIO(content)}
    storage.generate_presigned_url.return_value = event['url']
    assert pilot.prepare(storage, KEY) == event
    storage.get_object.assert_called_once_with(Bucket=pilot.BUCKET, Key=KEY, IfMatch='etag', VersionId='v1')
    assert storage.generate_presigned_url.call_args.kwargs['Params']['VersionId'] == 'v1'


def test_legacy_scanner_excludes_only_exact_pilot_key(event):
    from xtrek.task_notifier import scan_and_deliver, DeliveryResult
    other = 'equipment-tasks/T-other.json'
    s3 = Mock(); s3.list_objects_v2.return_value = {'Contents': [
        {'Key': key, 'LastModified': datetime.now(timezone.utc)} for key in (KEY, other)]}
    notifier = Mock(); notifier.process_object.return_value = DeliveryResult('uncertain', other)
    scan_and_deliver(notifier, s3=s3, cutoff_utc=datetime(2000,1,1,tzinfo=timezone.utc))
    assert notifier.process_object.call_count == 1
    assert notifier.process_object.call_args.kwargs['object_key'] == other


def test_poller_adds_only_exact_cohort_key(event):
    from xtrek.s3_poller import configured_routes
    routes = configured_routes(pilot.BUCKET, 'isolated-internal')
    keys = [p for p in routes[pilot.BUCKET] if p.startswith(pilot.PREFIX)]
    assert keys == [KEY]


def automatic(event, tmp_path, monkeypatch, excluded=()):
    cutoff = pilot.utc(event['created_at']) - timedelta(seconds=1)
    p = tmp_path / 'automatic.json'
    p.write_text(json.dumps({'mode': 'new_after_cutoff', 'not_before': cutoff.isoformat(), 'excluded_keys': list(excluded)}))
    monkeypatch.setenv('XTREK_MAX_QUEUE_PILOT_COHORT', str(p))
    return pilot.cohort()


def test_automatic_admits_future_key_without_registration(event, tmp_path, monkeypatch):
    policy = automatic(event, tmp_path, monkeypatch)
    assert policy.enabled and not policy.keys
    assert policy.admits_object(KEY, pilot.utc(event['ready_at']))
    assert not policy.admits_object(KEY, policy.not_before - timedelta(seconds=1))
    assert pilot.validate(event, datetime.now(timezone.utc))[0] == KEY


def test_historical_uncertain_key_stays_excluded_even_if_recreated(event, tmp_path, monkeypatch):
    policy = automatic(event, tmp_path, monkeypatch, excluded=[KEY])
    assert not policy.admits_key(KEY)
    assert not policy.admits_object(KEY, datetime.now(timezone.utc))
    post = Mock()
    with pytest.raises(pilot.PilotInvalid, match='outside_cohort'):
        pilot.send(event, post, 'token', 'ca', 1)
    post.assert_not_called()


def test_automatic_checks_original_json_date_not_only_lastmodified(event, tmp_path, monkeypatch):
    automatic(event, tmp_path, monkeypatch)
    event['created_at'] = '2000-01-01T00:00:00Z'
    post = Mock()
    with pytest.raises(pilot.PilotInvalid, match='outside_cohort'):
        pilot.send(event, post, 'token', 'ca', 1)
    post.assert_not_called()


def test_automatic_poller_enqueues_only_new_nonexcluded_objects(event, tmp_path, monkeypatch):
    from xtrek.s3_poller import EventLedger, S3EventPoller, configured_routes
    from test_s3_poller import FakeS3
    old = 'equipment-tasks/T-old-uncertain.json'
    policy = automatic(event, tmp_path, monkeypatch, [old])
    assert configured_routes(pilot.BUCKET, 'isolated')[pilot.BUCKET] == ('Задания/', pilot.PREFIX)
    objects = [{'Key': k, 'LastModified': modified, 'ETag': 'fixture', 'Size': 1} for k,modified in [
        (KEY, pilot.utc(event['ready_at'])), (old, pilot.utc(event['ready_at'])),
        ('equipment-tasks/T-before-cutoff.json', policy.not_before-timedelta(seconds=1))]]
    ledger = EventLedger(str(tmp_path / 'outbox.sqlite3'))
    try:
        poller = S3EventPoller(FakeS3([{'Contents': objects}]), ledger,
            datetime(2000,1,1,tzinfo=timezone.utc), routed_prefixes={pilot.BUCKET: (pilot.PREFIX,)})
        assert poller.scan(max_pages=1) == 1
        assert ledger.db.execute('select object_key from s3_outbox').fetchall() == [(KEY,)]
        assert poller.scan(max_pages=1) == 0
    finally:
        ledger.close()


def test_automatic_old_notifier_hands_off_only_new_nonexcluded_objects(event, tmp_path, monkeypatch):
    from xtrek.task_notifier import scan_and_deliver, DeliveryResult
    old = 'equipment-tasks/T-old-uncertain.json'
    policy = automatic(event, tmp_path, monkeypatch, [old])
    older = 'equipment-tasks/T-before-cutoff.json'
    s3 = Mock(); s3.list_objects_v2.return_value = {'Contents': [
        {'Key': KEY, 'LastModified': pilot.utc(event['ready_at'])},
        {'Key': old, 'LastModified': pilot.utc(event['ready_at'])},
        {'Key': older, 'LastModified': policy.not_before-timedelta(seconds=1)}]}
    notifier = Mock(); notifier.process_object.return_value = DeliveryResult('uncertain', old)
    scan_and_deliver(notifier,s3=s3,cutoff_utc=datetime(2000,1,1,tzinfo=timezone.utc))
    assert [x.kwargs['object_key'] for x in notifier.process_object.call_args_list] == [old,older]


@pytest.mark.parametrize('bad', [
    {'mode':'new_after_cutoff', 'not_before':'2026-11-09T00:00:00Z'},
    {'mode':'all', 'not_before':'2026-11-09T00:00:00Z','excluded_keys':[]},
    {'mode':'new_after_cutoff', 'not_before':'2026-11-09','excluded_keys':[]},
])
def test_automatic_requires_explicit_exclusion_snapshot_and_utc(bad,tmp_path,monkeypatch):
    p=tmp_path/'bad.json';p.write_text(json.dumps(bad))
    monkeypatch.setenv('XTREK_MAX_QUEUE_PILOT_COHORT',str(p))
    with pytest.raises(pilot.PilotInvalid):pilot.cohort()
