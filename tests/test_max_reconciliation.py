"""Offline regression tests: positive history evidence is shared; POST is never replayed."""
from dataclasses import replace
from datetime import datetime, timezone
from email.utils import format_datetime

import pytest

from xtrek.max_retry import MaxRetryLater, RetrySchedule, api_scope, observe_response
from xtrek.pallet_label_max_notifier import DeliveryConflict, DeliveryLedger, DeliveryUncertain, MaxPalletLabelNotifier
from test_pallet_label_max_notifier import (
    BOT, CHAT, NOW_MS, FakeSession, Response, make_item, make_notifier,
)


def item(number):
    original = make_item()
    return replace(original, report_id=f'T-report-{number}', sscc=f'{number:018}',
                   file_name=f'pallet_1_{number:018}.png',
                   text=f'T-report-{number}\nSSCC: {number:018}')


def message(label, mid):
    return {'timestamp': NOW_MS, 'sender': {'user_id': BOT, 'is_bot': True},
            'recipient': {'chat_id': CHAT},
            'body': {'mid': mid, 'text': label.text, 'attachments': [{'type': 'image', 'payload': {}}]}}


def remember(notifier, label, state='POST_UNCERTAIN', mid=None):
    notifier.ledger.remember(label, CHAT, BOT)
    notifier.ledger.set_state(label, CHAT, state, mid)


def test_one_page_confirms_all_pending_including_backed_off_rows(tmp_path):
    labels = [item(i) for i in range(1, 4)]
    session = FakeSession(history=[message(label, f'mid-{i}') for i, label in enumerate(labels)])
    notifier, ledger = make_notifier(tmp_path, session)
    for label in labels:
        remember(notifier, label)
        notifier.retries.fail(notifier._check_scope(ledger.get(label.report_id, label.sscc, CHAT)))
    notifier._history()
    assert all(ledger.get(x.report_id, x.sscc, CHAT)['state'] == 'CONFIRMED' for x in labels)
    assert not session.posts


def test_mid_batch_includes_all_due_rows_and_partial_reply_backs_off_missing(tmp_path):
    labels = [item(i) for i in range(1, 6)]
    calls = []
    class Session(FakeSession):
        def get(self, url, params=None, **kwargs):
            calls.append(params)
            assert params == {'message_ids': 'mid-1,mid-2,mid-3,mid-4,mid-5'}
            return Response(value={'messages': [message(labels[0], 'mid-1'), message(labels[3], 'mid-4')]})
    session = Session()
    notifier, ledger = make_notifier(tmp_path, session)
    for i, label in enumerate(labels, 1):
        remember(notifier, label, 'POSTED_UNCONFIRMED', f'mid-{i}')
    assert len(notifier.reconcile_pending_mids()) == 2
    notifier.reconcile_pending_mids()
    assert len(calls) == 1
    for i, label in enumerate(labels, 1):
        row = ledger.get(label.report_id, label.sscc, CHAT)
        assert row['state'] == ('CONFIRMED' if i in (1, 4) else 'POSTED_UNCONFIRMED')
        assert row['message_id'] == f'mid-{i}'


def test_429_on_second_page_keeps_first_page_confirmations(tmp_path):
    labels = [item(1), item(2)]
    first = [message(labels[0], 'found')] + [{'timestamp': NOW_MS - i, 'body': {}} for i in range(1, 100)]
    calls = []
    class Session(FakeSession):
        def get(self, url, params=None, **kwargs):
            calls.append(params)
            return Response(value={'messages': first}) if len(calls) == 1 else Response(429)
    session = Session()
    notifier, ledger = make_notifier(tmp_path, session)
    for label in labels:
        remember(notifier, label)
    with pytest.raises(MaxRetryLater):
        notifier._history()
    assert ledger.get(labels[0].report_id, labels[0].sscc, CHAT)['state'] == 'CONFIRMED'
    assert ledger.get(labels[1].report_id, labels[1].sscc, CHAT)['state'] == 'POST_UNCERTAIN'
    with pytest.raises(MaxRetryLater):
        notifier._history()
    assert len(calls) == 2
    assert not session.posts


def test_429_after_post_persists_mid_then_restart_only_gets(tmp_path):
    clock = [1000.]
    class Session(FakeSession):
        fail = True
        gets = []
        def get(self, url, params=None, **kwargs):
            self.gets.append((url, params))
            if params and 'message_ids' in params and self.fail:
                return Response(429)
            return super().get(url, params=params, **kwargs)
    session = Session()
    ledger = DeliveryLedger.initialize(str(tmp_path / 'ledger'))
    def build():
        return MaxPalletLabelNotifier(token='fake', chat_id=CHAT, expected_bot_user_id=BOT,
            session=session, ledger=ledger, history_start_ms=0, clock=lambda: clock[0])
    notifier = build()
    label = make_item()
    with pytest.raises(MaxRetryLater):
        notifier.deliver(label)
    assert ledger.get(label.report_id, label.sscc, CHAT)['message_id'] == 'mid-1'
    count = len(session.posts)
    notifier = build()
    with pytest.raises(MaxRetryLater):
        notifier.deliver(label)
    clock[0] += 400
    session.fail = False
    assert notifier.deliver(label)['message_id'] == 'mid-1'
    assert len(session.posts) == count
    assert sum(bool(params and 'chat_id' in params) for _, params in session.gets) == 1


def test_prepost_429_has_durable_row_and_can_resume(tmp_path):
    class Session(FakeSession):
        fail = True
        def get(self, url, **kw):
            return Response(429) if self.fail else super().get(url, **kw)
    session = Session()
    notifier, ledger = make_notifier(tmp_path, session)
    label = make_item()
    with pytest.raises(MaxRetryLater):
        notifier.deliver(label)
    assert ledger.get(label.report_id, label.sscc, CHAT)['state'] == 'UPLOAD_PENDING'
    assert not session.posts
    notifier.api_retries.clear(notifier.api_scope)
    session.fail = False
    assert notifier.deliver(label)['state'] == 'CONFIRMED'


def test_confirmed_fast_path_uses_no_get_and_rejects_changed_source(tmp_path):
    session = FakeSession()
    notifier, _ = make_notifier(tmp_path, session)
    label = make_item()
    notifier.deliver(label)
    def no_get(*args, **kwargs):
        raise AssertionError('Confirmed delivery must not call MAX')
    session.get = no_get
    assert notifier.deliver(label)['state'] == 'ALREADY_CONFIRMED_BY_LEDGER'
    with pytest.raises(DeliveryConflict):
        notifier.deliver(replace(label, source_hash='changed'))


@pytest.mark.parametrize('change', ['bot', 'chat', 'caption', 'mid', 'file'])
def test_batch_confirmation_requires_matching_identity_and_evidence(tmp_path, change):
    label = item(1)
    msg = message(label, 'expected-mid')
    if change == 'bot': msg['sender']['user_id'] = 'another'
    if change == 'chat': msg['recipient']['chat_id'] = 'another'
    if change == 'caption': msg['body']['text'] += ' changed'
    if change == 'mid': msg['body']['mid'] = 'different'
    if change == 'file': msg['body']['attachments'][0]['type'] = 'file'
    notifier, ledger = make_notifier(tmp_path, FakeSession())
    remember(notifier, label, 'POSTED_UNCONFIRMED', 'expected-mid')
    assert notifier.reconcile_history([msg]) == []
    row = ledger.get(label.report_id, label.sscc, CHAT)
    assert row['state'] == ('POST_UNCERTAIN' if change == 'mid' else 'POSTED_UNCONFIRMED')
    assert row['message_id'] == 'expected-mid'


def test_multiple_matches_are_held_and_not_resent(tmp_path):
    label = make_item()
    session = FakeSession(history=[message(label, 'a'), message(label, 'b')])
    notifier, ledger = make_notifier(tmp_path, session)
    with pytest.raises(DeliveryUncertain):
        notifier.deliver(label)
    assert ledger.get(label.report_id, label.sscc, CHAT)['state'] == 'POST_UNCERTAIN'
    assert not session.posts


def test_legacy_known_mid_can_be_reconciled_without_new_caption(tmp_path):
    notifier, ledger = make_notifier(tmp_path, FakeSession())
    label = item(1)
    ledger.reserve(label, CHAT)  # Pre-migration schema has no saved caption.
    ledger.set_state(label, CHAT, 'POSTED_UNCONFIRMED', 'old-mid')
    assert notifier.reconcile_history([message(label, 'old-mid')]) == ['old-mid']


def test_history_is_reused_for_several_labels(tmp_path):
    session = FakeSession()
    notifier, _ = make_notifier(tmp_path, session)
    original_get = session.get
    calls = []
    def get(url, params=None, **kw):
        calls.append(params)
        return original_get(url, params=params, **kw)
    session.get = get
    notifier.deliver(item(1))
    notifier.deliver(item(2))
    assert sum(bool(p and 'chat_id' in p) for p in calls) == 1
    assert sum(bool(p and 'message_ids' in p) for p in calls) == 2


def test_backoff_survives_restart_grows_and_respects_retry_after(tmp_path):
    clock = [1000.]
    def build(): return RetrySchedule(tmp_path / 'retry.db', clock=lambda: clock[0], jitter=lambda: 0)
    retry = build()
    for expected in (5, 10, 20, 40, 80, 160, 300, 300):
        assert retry.fail('scope') == expected
        retry = build()
        with pytest.raises(MaxRetryLater): retry.check('scope')
        assert retry.fail('scope') == expected  # Checking early cannot inflate attempts.
        clock[0] += expected
    retry.success('scope')
    assert retry.fail('scope', '600') == 600
    clock[0] += 600
    date = format_datetime(datetime.fromtimestamp(clock[0] + 700, timezone.utc), usegmt=True)
    assert retry.fail('scope', date) == 700


def test_shared_api_cooldown_blocks_other_sender_without_reset_by_inflight_success(tmp_path):
    clock = [1000.]
    a = RetrySchedule(tmp_path / 'shared.db', clock=lambda: clock[0], jitter=lambda: 0)
    b = RetrySchedule(tmp_path / 'shared.db', clock=lambda: clock[0], jitter=lambda: 0)
    scope = api_scope('same-token', CHAT)
    with pytest.raises(MaxRetryLater): observe_response(a, scope, Response(429))
    b.success(scope)
    with pytest.raises(MaxRetryLater): b.check(scope)
    b.check(api_scope('different-token', CHAT))
    assert 'same-token' not in scope


def make_task_notifier(tmp_path):
    from xtrek.task_notifier import NotifierSettings, TaskNotifier
    from test_task_notifier import FakeS3
    return TaskNotifier(s3=FakeS3(), settings=NotifierSettings('test-token', CHAT, 'fake-ca', tmp_path / 'tasks.db'))


def task_message(number):
    from xtrek.task_notifier import TASK_BUCKET
    task_id = f'T-task-{number}'
    key = f'equipment-tasks/{task_id}.json'
    msg = {'timestamp': NOW_MS, 'sender': {'user_id': 'bot-1', 'is_bot': True},
           'recipient': {'chat_id': CHAT},
           'body': {'mid': f'task-mid-{number}',
                    'text': f'{task_id} https://storage.yandexcloud.net/{TASK_BUCKET}/{key}?signature=fake'}}
    return task_id, key, msg


def task_rows(notifier, known_mids=False):
    from xtrek.task_notifier import TASK_BUCKET
    for i in range(1, 4):
        task_id, key, _ = task_message(i)
        notifier._ensure_row(TASK_BUCKET, key, 'etag', None, 1)
        notifier._transition(TASK_BUCKET, task_id, ('discovered',), 'uncertain',
                             message_id=f'task-mid-{i}' if known_mids else None)


def test_task_history_confirms_all_rows_in_one_pass(tmp_path, monkeypatch):
    from xtrek.task_notifier import TASK_BUCKET
    from test_task_notifier import Response as TaskResponse
    notifier = make_task_notifier(tmp_path)
    task_rows(notifier)
    calls = []
    def get(url, **kw):
        calls.append(kw.get('params'))
        return TaskResponse(data={'user_id': 'bot-1'} if url.endswith('/me') else
                            {'messages': [task_message(i)[2] for i in range(1, 4)]})
    monkeypatch.setattr('xtrek.task_notifier.requests.get', get)
    task_id, key, _ = task_message(1)
    assert notifier._reconcile(task_id=task_id, bucket=TASK_BUCKET, key=key, created_ms=1) == (True, 'task-mid-1', False)
    assert all(notifier._row(TASK_BUCKET, task_message(i)[0])['state'] == 'delivered' for i in range(1, 4))
    assert len(calls) == 2  # /me plus one shared history read.


def test_task_known_mids_use_one_batch_and_missing_row_waits(tmp_path, monkeypatch):
    from xtrek.task_notifier import TASK_BUCKET
    from test_task_notifier import Response as TaskResponse
    notifier = make_task_notifier(tmp_path)
    task_rows(notifier, known_mids=True)
    calls = []
    def get(url, **kw):
        if url.endswith('/me'): return TaskResponse(data={'user_id': 'bot-1'})
        calls.append(kw['params'])
        return TaskResponse(data={'messages': [task_message(i)[2] for i in (1, 3)]})
    monkeypatch.setattr('xtrek.task_notifier.requests.get', get)
    task_id, key, _ = task_message(2)
    for _ in range(2):
        assert notifier._reconcile(task_id=task_id, bucket=TASK_BUCKET, key=key, created_ms=1) == (False, None, False)
    assert calls == [{'message_ids': 'task-mid-1,task-mid-2,task-mid-3'}]
    assert notifier._row(TASK_BUCKET, task_id)['state'] == 'uncertain'
    assert notifier._row(TASK_BUCKET, task_id)['message_id'] == 'task-mid-2'


def test_task_second_page_429_preserves_other_confirmations(tmp_path, monkeypatch):
    from xtrek.task_notifier import TASK_BUCKET
    from test_task_notifier import Response as TaskResponse
    notifier = make_task_notifier(tmp_path)
    task_rows(notifier)
    calls = []
    def get(url, **kw):
        if url.endswith('/me'): return TaskResponse(data={'user_id': 'bot-1'})
        calls.append(kw['params'])
        if len(calls) > 1: return TaskResponse(429)
        return TaskResponse(data={'messages': [task_message(1)[2]] +
                             [{'timestamp': NOW_MS - i, 'body': {}} for i in range(1, 100)]})
    monkeypatch.setattr('xtrek.task_notifier.requests.get', get)
    task_id, key, _ = task_message(2)
    assert notifier._reconcile(task_id=task_id, bucket=TASK_BUCKET, key=key, created_ms=1) == (False, None, False)
    assert notifier._row(TASK_BUCKET, task_message(1)[0])['state'] == 'delivered'
    assert notifier._row(TASK_BUCKET, task_id)['state'] == 'uncertain'
    notifier._reconcile(task_id=task_id, bucket=TASK_BUCKET, key=key, created_ms=1)
    assert len(calls) == 2


def test_task_mid_is_saved_before_failing_confirmation_get(tmp_path, monkeypatch):
    from xtrek.task_notifier import TASK_BUCKET
    from test_task_notifier import Response as TaskResponse, KEY, TASK_ID
    notifier = make_task_notifier(tmp_path)
    def get(url, **kw):
        if url.endswith('/me'): return TaskResponse(data={'user_id': 'bot-1'})
        if not url.endswith('/messages'): return TaskResponse(206)
        if 'message_ids' in kw['params']:
            row = notifier._row(TASK_BUCKET, TASK_ID)
            assert row['message_id'] == 'posted-mid'
            assert row['state'] == 'sending'
            return TaskResponse(429)
        return TaskResponse(data={'messages': []})
    posts = []
    def post(*args, **kw):
        posts.append(kw)
        return TaskResponse(data={'message': {'body': {'mid': 'posted-mid'}}})
    monkeypatch.setattr('xtrek.task_notifier.requests.get', get)
    monkeypatch.setattr('xtrek.task_notifier.requests.post', post)
    assert notifier.process_object(bucket_id=TASK_BUCKET, object_key=KEY).state == 'uncertain'
    assert notifier.process_object(bucket_id=TASK_BUCKET, object_key=KEY).state == 'uncertain'
    assert len(posts) == 1
    assert notifier._row(TASK_BUCKET, TASK_ID)['message_id'] == 'posted-mid'


def test_celery_only_retries_explicit_deferred_checks(monkeypatch):
    from test_tasks_unit_set_flow import import_tasks
    from types import SimpleNamespace
    tasks = import_tasks(monkeypatch, 'xtrek.tasks')
    monkeypatch.setattr(tasks, 'load_config', lambda *args: {})
    retries = []
    class Scheduled(Exception): pass
    def retry(**kw):
        retries.append(kw)
        raise Scheduled()
    def deliver(**kw): raise MaxRetryLater(42)
    monkeypatch.setattr('xtrek.pallet_label_celery_runtime.deliver_pallet_report', deliver)
    with pytest.raises(Scheduled):
        tasks.deliver_pallet_report._func(SimpleNamespace(retry=retry), 'equipment-reports/test.json')
    assert retries[0]['countdown'] == 42 and retries[0]['max_retries'] is None
    def invalid(**kw): raise ValueError('invalid source')
    monkeypatch.setattr('xtrek.pallet_label_celery_runtime.deliver_pallet_report', invalid)
    with pytest.raises(ValueError):
        tasks.deliver_pallet_report._func(SimpleNamespace(retry=retry), 'equipment-reports/test.json')
    assert len(retries) == 1


def test_deferred_exception_survives_celery_serialization():
    import pickle
    from xtrek.pallet_label_max_notifier import DeliveryRecheck
    for error in (MaxRetryLater(35), DeliveryRecheck(35, 'GET only')):
        restored = pickle.loads(pickle.dumps(error))
        assert type(restored) is type(error)
        assert restored.retry_after == 35
        assert str(restored) == str(error)


def test_two_pallet_senders_cannot_post_twice(tmp_path):
    from concurrent.futures import ThreadPoolExecutor
    session = FakeSession()
    notifier, ledger = make_notifier(tmp_path, session)
    other = MaxPalletLabelNotifier(token='fake-token', chat_id=CHAT, expected_bot_user_id=BOT,
        session=session, ledger=DeliveryLedger(ledger.path), history_start_ms=0)
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda n: n.deliver(make_item()), [notifier, other]))
    assert session.message_number == 1
    assert all(r['message_id'] == 'mid-1' for r in results)


def test_partial_history_never_authorizes_unknown_post(tmp_path):
    session = FakeSession(history=[{'timestamp': NOW_MS - i, 'body': {}} for i in range(100)])
    notifier, ledger = make_notifier(tmp_path, session)
    notifier.max_history_pages = 1
    notifier.allow_truncated_history = True
    label = make_item()
    remember(notifier, label, 'POST_STARTED')
    with pytest.raises(DeliveryUncertain, match='never auto-repeat'):
        notifier.deliver(label)
    assert not session.posts
    assert ledger.get(label.report_id, label.sscc, CHAT)['state'] == 'POST_STARTED'


def test_mid_batches_are_bounded(tmp_path):
    calls = []
    class Session(FakeSession):
        def get(self, url, params=None, **kw):
            calls.append(params['message_ids'].split(','))
            return Response(value={'messages': []})
    notifier, _ = make_notifier(tmp_path, Session())
    for i in range(1, 102):
        remember(notifier, item(i), 'POSTED_UNCONFIRMED', f'mid-{i}')
    notifier.reconcile_pending_mids()
    assert [len(batch) for batch in calls] == [100, 1]


def test_confirmed_state_cannot_be_downgraded(tmp_path):
    notifier, ledger = make_notifier(tmp_path, FakeSession())
    label = make_item()
    notifier.deliver(label)
    ledger.set_state(label, CHAT, 'POST_UNCERTAIN', 'incorrect-mid')
    row = ledger.get(label.report_id, label.sscc, CHAT)
    assert row['state'] == 'CONFIRMED' and row['message_id'] == 'mid-1'


def test_real_celery_can_schedule_get_checks_beyond_default_retry_limit(monkeypatch):
    from celery import Celery
    from celery.exceptions import Retry
    from test_tasks_unit_set_flow import import_tasks
    tasks = import_tasks(monkeypatch, 'xtrek.tasks', celery_class=Celery)
    monkeypatch.setattr(tasks, 'load_config', lambda *args: {})
    def deferred(**kw): raise MaxRetryLater(70)
    monkeypatch.setattr('xtrek.pallet_label_celery_runtime.deliver_pallet_report', deferred)
    task = tasks.deliver_pallet_report
    assert task.max_retries is None
    task.push_request(retries=8, called_directly=False, is_eager=True, id='offline-retry',
                      args=('equipment-reports/test.json',), kwargs={})
    try:
        with pytest.raises(Retry) as error:
            task.run('equipment-reports/test.json')
        assert error.value.when == 70
    finally:
        task.pop_request()


def test_pallet_caption_does_not_conflict_with_task_link_in_shared_history(tmp_path, monkeypatch):
    from xtrek.task_notifier import TASK_BUCKET
    from test_task_notifier import Response as TaskResponse
    notifier = make_task_notifier(tmp_path)
    task_rows(notifier)
    task_id, key, link = task_message(1)
    pallet = message(item(1), 'pallet-mid')
    pallet['sender']['user_id'] = 'bot-1'
    pallet['body']['text'] = f'Ярлык паллета 1/1\nОтчёт: {task_id}\nSSCC: 346070517900000057'
    def get(url, **kw):
        return TaskResponse(data={'user_id': 'bot-1'} if url.endswith('/me') else {'messages': [pallet, link]})
    monkeypatch.setattr('xtrek.task_notifier.requests.get', get)
    assert notifier._reconcile(task_id=task_id, bucket=TASK_BUCKET, key=key, created_ms=1) == (True, 'task-mid-1', False)


def test_cached_positive_evidence_confirms_backed_off_mid_without_network(tmp_path):
    session = FakeSession()
    notifier, ledger = make_notifier(tmp_path, session)
    label = make_item()
    remember(notifier, label, 'POSTED_UNCONFIRMED', 'cached-mid')
    row = ledger.get(label.report_id, label.sscc, CHAT)
    notifier.retries.fail(notifier._check_scope(row))
    notifier.api_retries.fail(notifier.api_scope)
    notifier._history_cache = [message(label, 'cached-mid')]
    def no_get(*args, **kwargs): raise AssertionError('Positive evidence already available')
    session.get = no_get
    assert notifier.deliver(label)['message_id'] == 'cached-mid'
    assert ledger.get(label.report_id, label.sscc, CHAT)['state'] == 'CONFIRMED'
    assert not session.posts


@pytest.mark.parametrize('payload', [{}, {'messages': None}, {'messages': [None]}, {'code': 'error'}])
def test_malformed_pallet_history_never_authorizes_post(tmp_path, payload):
    session = FakeSession()
    original = session.get
    session.get = lambda url, **kw: Response(value=payload) if url.endswith('/messages') else original(url, **kw)
    notifier, _ = make_notifier(tmp_path, session)
    with pytest.raises(MaxRetryLater, match='Invalid MAX history'):
        notifier.deliver(make_item())
    assert not session.posts


@pytest.mark.parametrize('payload', [{}, {'messages': None}, {'messages': [None]}, {'code': 'error'}])
def test_malformed_task_history_never_authorizes_post(tmp_path, monkeypatch, payload):
    from xtrek.task_notifier import TASK_BUCKET
    from test_task_notifier import Response as TaskResponse, KEY
    notifier = make_task_notifier(tmp_path)
    monkeypatch.setattr('xtrek.task_notifier.requests.get', lambda url, **kw:
                        TaskResponse(data={'user_id': 'bot-1'} if url.endswith('/me') else payload))
    def no_post(*args, **kwargs): raise AssertionError('Malformed history must not authorize POST')
    monkeypatch.setattr('xtrek.task_notifier.requests.post', no_post)
    assert notifier.process_object(bucket_id=TASK_BUCKET, object_key=KEY).state == 'deferred'


def reopen_pallet_notifier(ledger, session):
    return MaxPalletLabelNotifier(token='fake-token', chat_id=CHAT, expected_bot_user_id=BOT,
        ledger=DeliveryLedger(ledger.path), session=session, history_start_ms=NOW_MS - 100000,
        sleep=lambda _: None)


def test_different_mids_on_separate_pages_remain_blocked_after_restart(tmp_path):
    label = make_item()
    class Pages(FakeSession):
        def get(self, url, params=None, **kw):
            if url.endswith('/me'):
                return super().get(url, params=params, **kw)
            if 'from' not in params:
                return Response(value={'messages': [message(label, 'first-mid')] +
                    [{'timestamp': NOW_MS - i, 'body': {}} for i in range(1, 100)]})
            return Response(value={'messages': [dict(message(label, 'second-mid'), timestamp=NOW_MS - 101)]})
    session = Pages()
    notifier, ledger = make_notifier(tmp_path, session)
    with pytest.raises(DeliveryUncertain, match='Multiple GET matches'):
        notifier.deliver(label)
    assert ledger.get(label.report_id, label.sscc, CHAT)['state'] == 'POST_UNCERTAIN'
    assert ledger.get(label.report_id, label.sscc, CHAT)['message_id'] == 'first-mid'
    other = reopen_pallet_notifier(ledger, session)
    # Neither a later exact match nor a direct transition may erase the conflict.
    other.reconcile_history([message(label, 'first-mid')])
    ledger.set_state(label, CHAT, 'CONFIRMED', 'first-mid')
    with pytest.raises(DeliveryUncertain, match='Multiple GET matches'):
        other.deliver(label)
    assert ledger.get(label.report_id, label.sscc, CHAT)['state'] == 'POST_UNCERTAIN'
    assert not session.posts


def test_conflict_discovered_after_page_confirmation_and_restart(tmp_path):
    label, unrelated = item(1), item(2)
    notifier, ledger = make_notifier(tmp_path, FakeSession())
    remember(notifier, label)
    remember(notifier, unrelated)
    assert notifier.reconcile_history([message(label, 'first')]) == ['first']
    other = reopen_pallet_notifier(ledger, FakeSession())
    assert other.reconcile_history([message(label, 'second'), message(unrelated, 'unrelated')]) == ['unrelated']
    with pytest.raises(DeliveryUncertain, match='Multiple GET matches'):
        other.deliver(label)
    assert ledger.get(unrelated.report_id, unrelated.sscc, CHAT)['state'] == 'CONFIRMED'


def test_overlapping_pages_with_same_mid_do_not_create_conflict(tmp_path):
    notifier, ledger = make_notifier(tmp_path, FakeSession())
    label = make_item()
    remember(notifier, label)
    notifier.reconcile_history([message(label, 'same-mid')])
    other = reopen_pallet_notifier(ledger, FakeSession())
    other.reconcile_history([message(label, 'same-mid')])
    assert other.deliver(label)['state'] == 'ALREADY_CONFIRMED_BY_LEDGER'


def test_caption_is_frozen_across_prepost_failure_and_restart(tmp_path):
    label = make_item()
    class Session(FakeSession):
        fail = True
        def get(self, url, **kw):
            return Response(429) if self.fail else super().get(url, **kw)
    session = Session()
    notifier, ledger = make_notifier(tmp_path, session)
    with pytest.raises(MaxRetryLater):
        notifier.deliver(label)
    assert not session.posts
    assert ledger.get(label.report_id, label.sscc, CHAT)['state'] == 'UPLOAD_PENDING'
    session.fail = False
    notifier.api_retries.clear(notifier.api_scope)
    other = reopen_pallet_notifier(ledger, session)
    changed = replace(label, text=label.text + '\nNew caption format')
    assert other.deliver(changed)['state'] == 'CONFIRMED'
    assert session.posts[-1][1]['json']['text'] == label.text
    assert ledger.saved_caption(changed, CHAT) == label.text
    assert session.message_number == 1
    assert other.deliver(changed)['state'] == 'ALREADY_CONFIRMED_BY_LEDGER'
    assert session.message_number == 1


def test_frozen_caption_is_not_changed_by_post_uncertain_retry(tmp_path):
    session = FakeSession(timeout_after_message=True)
    notifier, ledger = make_notifier(tmp_path, session)
    label = make_item()
    with pytest.raises(DeliveryUncertain):
        notifier.deliver(label)
    other = reopen_pallet_notifier(ledger, session)
    changed = replace(label, text=label.text + '\nNew caption format')
    assert other.deliver(changed)['state'] == 'ALREADY_CONFIRMED_BY_GET'
    assert ledger.saved_caption(changed, CHAT) == label.text
    assert session.message_number == 1


@pytest.mark.parametrize('first_wrong_link', [False, True])
def test_task_conflict_across_pages_survives_restart(tmp_path, monkeypatch, first_wrong_link):
    from xtrek.task_notifier import TASK_BUCKET
    from test_task_notifier import Response as TaskResponse
    notifier = make_task_notifier(tmp_path)
    task_rows(notifier)
    task_id, key, exact = task_message(1)
    first = dict(exact, body=dict(exact['body'], mid='first-mid'))
    if first_wrong_link:
        first['body']['text'] = task_id + ' https://example.org/wrong-task'
    def get(url, **kw):
        if url.endswith('/me'): return TaskResponse(data={'user_id': 'bot-1'})
        if 'from' not in kw['params']:
            return TaskResponse(data={'messages': [first] +
                [{'timestamp': NOW_MS - i, 'body': {}} for i in range(1, 100)]})
        return TaskResponse(data={'messages': [dict(exact, timestamp=NOW_MS - 101)]})
    monkeypatch.setattr('xtrek.task_notifier.requests.get', get)
    assert notifier._reconcile(task_id=task_id, bucket=TASK_BUCKET, key=key, created_ms=1) == (True, None, True)
    assert notifier._row(TASK_BUCKET, task_id)['state'] == 'uncertain'
    other = make_task_notifier(tmp_path)
    other._bot_id = 'bot-1'
    other._apply_history([exact])
    assert other._reconcile(task_id=task_id, bucket=TASK_BUCKET, key=key, created_ms=1) == (True, None, True)
    assert other._row(TASK_BUCKET, task_id)['state'] == 'uncertain'
