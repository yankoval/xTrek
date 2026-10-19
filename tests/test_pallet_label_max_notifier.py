import json
import time

import pytest

from xtrek.pallet_label_max_notifier import (
    DeliveryLedger,
    DeliveryUncertain,
    HistoryIncomplete,
    LabelMessage,
    MaxPalletLabelNotifier,
)


BOT = '259171708'
CHAT = '-72448994566325'
REPORT = 'T-DEMO-00000000-0000-4000-8000-000000000001'
SSCC = '346070517900000057'
FILENAME = f'pallet_1_{SSCC}.png'
NOW_MS = int(time.time() * 1000)


class Response:
    def __init__(self, status=200, value=None):
        self.status_code = status
        self.value = value or {}

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f'HTTP {self.status_code}')

    def json(self):
        return self.value


class FakeSession:
    def __init__(self, *, bot=BOT, history=None, timeout_after_message=False, duplicate_history=False):
        self.bot = bot
        self.history = list(history or [])
        self.timeout_after_message = timeout_after_message
        self.duplicate_history = duplicate_history
        self.posts = []
        self.message_number = 0

    def get(self, url, params=None, headers=None, timeout=None):
        if url.endswith('/me'):
            return Response(value={'user_id': self.bot})
        if url.endswith('/messages'):
            return Response(value={'messages': self.history})
        raise AssertionError(url)

    def post(self, url, **kwargs):
        self.posts.append((url, kwargs))
        if url.endswith('/uploads'):
            return Response(value={'url': MAX_UPLOAD_URL})
        if url == MAX_UPLOAD_URL:
            return Response(value={'photos': {'one': {'token': 'fixture-image-token'}}})
        if url.endswith('/messages'):
            self.message_number += 1
            text = kwargs['json']['text']
            filename = FILENAME
            message = {
                'timestamp': NOW_MS + self.message_number,
                'sender': {'user_id': self.bot, 'is_bot': True},
                'recipient': {'chat_id': int(CHAT)},
                'body': {
                    'mid': f'mid-{self.message_number}',
                    'text': text,
                    'attachments': [{'type': 'image', 'payload': {'token': 'fixture-image-token'}}],
                },
            }
            self.history.insert(0, message)
            if self.timeout_after_message:
                self.timeout_after_message = False
                raise TimeoutError('simulated lost POST response after acceptance')
            return Response(value={'message': {'body': {'mid': f'mid-{self.message_number}'}}})
        raise AssertionError(url)


MAX_UPLOAD_URL = 'https://uploads.example/file'


def make_item():
    return LabelMessage(
        report_id=REPORT,
        sscc=SSCC,
        file_name=FILENAME,
        text=f'Ярлык паллета 1/1\nОтчёт: {REPORT}\nSSCC: {SSCC}\nКоробов: 2 · Штук: 10',
        png_bytes=b'PNG fixture',
        source_hash='a' * 64,
    )


def make_notifier(tmp_path, session):
    ledger = DeliveryLedger.initialize(str(tmp_path / 'max-label-ledger.sqlite3'))
    return MaxPalletLabelNotifier(
        token='fake-token', chat_id=CHAT, expected_bot_user_id=BOT,
        ledger=ledger, session=session, history_start_ms=NOW_MS - 100000,
        sleep=lambda _: None,
    ), ledger


def test_get_history_preflight_then_upload_post_and_exact_get_confirmation(tmp_path):
    session = FakeSession()
    notifier, ledger = make_notifier(tmp_path, session)
    result = notifier.deliver(make_item())
    assert result == {'state': 'CONFIRMED', 'message_id': 'mid-1'}
    assert ledger.get(REPORT, SSCC, CHAT)['state'] == 'CONFIRMED'
    assert len(session.posts) == 3
    assert session.posts[-1][1]['params'] == {'chat_id': CHAT}


def test_exact_existing_file_in_get_prevents_any_post(tmp_path):
    existing = {
        'timestamp': NOW_MS,
        'sender': {'user_id': BOT, 'is_bot': True},
        'recipient': {'chat_id': int(CHAT)},
        'body': {'mid': 'already-there', 'text': f'{REPORT}\nSSCC: {SSCC}',
                 'attachments': [{'type': 'file', 'payload': {'name': FILENAME}}]},
    }
    session = FakeSession(history=[existing])
    notifier, ledger = make_notifier(tmp_path, session)
    result = notifier.deliver(make_item())
    assert result == {'state': 'LEGACY_FILE_CONFIRMED_BY_GET', 'message_id': 'already-there'}
    assert session.posts == []
    assert ledger.get(REPORT, SSCC, CHAT)['state'] == 'CONFIRMED'


@pytest.mark.parametrize('legacy', [False, True])
def test_existing_image_with_compact_or_legacy_caption_prevents_post(tmp_path, legacy):
    item = make_item()
    text = (item.text.replace(' · Штук:', '\nШтук:') + f'\nФайл: {FILENAME}'
            if legacy else item.text)
    existing = {
        'timestamp': NOW_MS,
        'sender': {'user_id': BOT},
        'recipient': {'chat_id': int(CHAT)},
        'body': {'mid': 'existing-image', 'text': text,
                 'attachments': [{'type': 'image', 'payload': {}}]},
    }
    session = FakeSession(history=[existing])
    notifier, ledger = make_notifier(tmp_path, session)
    assert notifier.deliver(item) == {
        'state': 'ALREADY_CONFIRMED_BY_GET', 'message_id': 'existing-image'}
    assert not session.posts
    assert ledger.get(REPORT, SSCC, CHAT)['state'] == 'CONFIRMED'


def test_wrong_bot_identity_fails_before_upload_or_message_post(tmp_path):
    session = FakeSession(bot='wrong-bot')
    notifier, _ = make_notifier(tmp_path, session)
    with pytest.raises(Exception, match='different bot'):
        notifier.deliver(make_item())
    assert session.posts == []


def test_ambiguous_post_is_reconciled_by_get_and_never_repeated(tmp_path):
    session = FakeSession(timeout_after_message=True)
    notifier, ledger = make_notifier(tmp_path, session)
    with pytest.raises(DeliveryUncertain, match='ambiguous'):
        notifier.deliver(make_item())
    assert ledger.get(REPORT, SSCC, CHAT)['state'] == 'POST_UNCERTAIN'
    previous_posts = len(session.posts)
    result = notifier.deliver(make_item())
    assert result['state'] == 'ALREADY_CONFIRMED_BY_GET'
    assert len(session.posts) == previous_posts


def test_ambiguous_post_without_get_match_is_not_reposted(tmp_path):
    session = FakeSession()
    notifier, ledger = make_notifier(tmp_path, session)
    item = make_item()
    with pytest.raises(DeliveryUncertain, match='MAX returned success'):
        # The test removes the stored row before post-send GET, simulating a
        # stale/lagging GET API while retaining the ambiguous ledger state.
        original_post = session.post
        def post_without_visible_history(url, **kwargs):
            response = original_post(url, **kwargs)
            if url.endswith('/messages'):
                session.history.clear()
            return response
        session.post = post_without_visible_history
        notifier.deliver(item)
    assert ledger.get(REPORT, SSCC, CHAT)['state'] == 'POSTED_UNCONFIRMED'
    previous_posts = len(session.posts)
    with pytest.raises(DeliveryUncertain, match='never auto-repeat'):
        notifier.deliver(item)
    assert len(session.posts) == previous_posts


def test_imported_legacy_ambiguous_attempt_blocks_post_until_get_confirmation(tmp_path):
    session = FakeSession()
    notifier, ledger = make_notifier(tmp_path, session)
    ledger.block_legacy_attempt(REPORT, SSCC, CHAT, FILENAME)
    with pytest.raises(DeliveryUncertain, match='never auto-repeat'):
        notifier.deliver(make_item())
    assert session.posts == []
    assert ledger.get(REPORT, SSCC, CHAT)['state'] == 'POST_UNCERTAIN'


def test_pagination_limit_fails_closed_before_any_post(tmp_path):
    history = [{'timestamp': NOW_MS, 'sender': {'user_id': BOT}, 'recipient': {'chat_id': int(CHAT)}, 'body': {'mid': str(i)}} for i in range(100)]
    session = FakeSession(history=history)
    notifier, _ = make_notifier(tmp_path, session)
    with pytest.raises(HistoryIncomplete):
        notifier.deliver(make_item())
    assert session.posts == []


def test_seeded_exact_old_attachment_prevents_new_post(tmp_path):
    session=FakeSession()
    notifier,ledger=make_notifier(tmp_path,session)
    notifier.history_seed=[{'timestamp':1,'sender':{'user_id':BOT},'recipient':{'chat_id':int(CHAT)},
       'body':{'mid':'old-seed','text':REPORT+' '+SSCC+' '+FILENAME,'attachments':[{'type':'file','payload':{'name':FILENAME}}]}}]
    assert notifier.deliver(make_item())['state']=='LEGACY_FILE_CONFIRMED_BY_GET'
    assert not session.posts


def test_prior_unnamed_attachment_is_held(tmp_path):
    session=FakeSession(history=[{'timestamp':NOW_MS,'sender':{'user_id':BOT},'recipient':{'chat_id':int(CHAT)},
        'body':{'mid':'unknown','text':REPORT+' '+SSCC,'attachments':[{'type':'image','payload':{}}]}}])
    notifier,ledger=make_notifier(tmp_path,session)
    with pytest.raises(DeliveryUncertain,match='filename or caption proof'):
        notifier.deliver(make_item())
    assert not session.posts
    assert ledger.get(REPORT,SSCC,CHAT)['state']=='POST_UNCERTAIN'


def test_history_checkpoint_resumes_and_stops_only_at_real_end(tmp_path):
    from xtrek.max_pallet_history import scan_history_batch
    class Pager:
        chat_id=CHAT;expected_bot_user_id=BOT
        def _verify_bot(self):pass
        def _get(self,path,params):
            if 'from' not in params:return {'messages':[{'timestamp':1000-i}for i in range(100)]}
            assert params['from']==900
            return {'messages':[{'timestamp':899}]}
    path=tmp_path/'history.json'
    first=scan_history_batch(Pager(),path,page_budget=1)
    assert not first['complete'] and len(first['messages'])==100
    second=scan_history_batch(Pager(),path,page_budget=1)
    assert second['complete'] and len(second['messages'])==101 and second['pages']==2


def test_explicit_approved_bounded_history_accepts_page_limit(tmp_path):
    class Pages(FakeSession):
        def get(self,url,params=None,**kw):
            if url.endswith('/me'):return Response(value={'user_id':BOT})
            stamp=10000 if 'from' not in params else params['from']
            return Response(value={'messages':[{'timestamp':stamp-i}for i in range(100)]})
    notifier,ledger=make_notifier(tmp_path,Pages())
    notifier.history_start_ms=0;notifier.max_history_pages=2;notifier.allow_truncated_history=True
    assert len(notifier._history())==200


def test_image_upload_contract_and_compact_caption(tmp_path):
    session = FakeSession()
    notifier, _ = make_notifier(tmp_path, session)
    notifier.deliver(make_item())
    assert session.posts[0][1]['params'] == {'type': 'image'}
    assert set(session.posts[1][1]['files']) == {'data'}
    assert session.posts[1][1]['files']['data'] == (FILENAME, make_item().png_bytes, 'image/png')
    payload = session.posts[2][1]['json']
    assert payload['attachments'] == [{'type': 'image', 'payload': {'token': 'fixture-image-token'}}]
    assert payload['text'] == make_item().text
    assert 'Файл:' not in payload['text']


@pytest.mark.parametrize('value', [
    {}, {'code': 'upload.error'}, {'photos': {}}, {'photos': []},
    {'photos': {'a': {'token': ''}}}, {'photos': {'a': {'token': '   '}}},
    {'photos': {'a': {'token': 'one'}, 'b': {'token': 'two'}}},
    {'token': 'file-only'}, ['invalid'],
])
def test_invalid_image_upload_never_posts_message_and_can_retry(tmp_path, value):
    session = FakeSession()
    post = session.post
    session.post = lambda url, **kw: Response(value=value) if url == MAX_UPLOAD_URL else post(url, **kw)
    notifier, ledger = make_notifier(tmp_path, session)
    with pytest.raises(DeliveryUncertain):
        notifier.deliver(make_item())
    assert session.message_number == 0
    assert ledger.get(REPORT, SSCC, CHAT)['state'] == 'UPLOAD_PENDING'
    session.post = post
    assert notifier.deliver(make_item())['state'] == 'CONFIRMED'


def test_non_json_image_upload_never_posts_message(tmp_path):
    class BadJSON(Response):
        def json(self): raise ValueError('bad json')
    session = FakeSession()
    post = session.post
    session.post = lambda url, **kw: BadJSON() if url == MAX_UPLOAD_URL else post(url, **kw)
    notifier, _ = make_notifier(tmp_path, session)
    with pytest.raises(DeliveryUncertain, match='invalid JSON'):
        notifier.deliver(make_item())
    assert session.message_number == 0


def test_only_explicit_attachment_not_ready_retries_message(tmp_path):
    session = FakeSession()
    post = session.post
    attempts = []
    def send(url, **kw):
        if url.endswith('/messages'):
            attempts.append(kw['json'])
            if len(attempts) == 1:
                return Response(status=400, value={'code': 'attachment.not.ready'})
        return post(url, **kw)
    session.post = send
    notifier, _ = make_notifier(tmp_path, session)
    assert notifier.deliver(make_item())['state'] == 'CONFIRMED'
    assert len(attempts) == 2 and attempts[0] == attempts[1]
    assert session.message_number == 1


def test_changed_renderer_does_not_repost_confirmed_ledger(tmp_path):
    from dataclasses import replace
    session = FakeSession()
    notifier, ledger = make_notifier(tmp_path, session)
    notifier.deliver(make_item())
    session.history.clear()
    count = len(session.posts)
    changed = replace(make_item(), png_bytes=b'new renderer', text='new caption')
    assert notifier.deliver(changed)['state'] == 'ALREADY_CONFIRMED_BY_LEDGER'
    assert len(session.posts) == count
    assert ledger.get(REPORT, SSCC, CHAT)['file_sha256'] != __import__('hashlib').sha256(changed.png_bytes).hexdigest()


@pytest.mark.parametrize('field,value', [('sender', {'user_id': 'wrong'}), ('recipient', {'chat_id': 'wrong'})])
def test_wrong_identity_image_does_not_confirm_our_delivery(tmp_path, field, value):
    session = FakeSession()
    notifier, _ = make_notifier(tmp_path, session)
    notifier.deliver(make_item())
    message = session.history[0]
    message[field] = value
    # A fresh ledger cannot confirm another sender/recipient's image.
    other, _ = make_notifier(tmp_path / 'other', session)
    assert other._exact_message([message], make_item()) is None


def test_file_response_cannot_confirm_new_image_post(tmp_path):
    session = FakeSession()
    post = session.post
    def file_reply(url, **kw):
        result = post(url, **kw)
        if url.endswith('/messages'):
            session.history[0]['body']['attachments'][0]['type'] = 'file'
        return result
    session.post = file_reply
    notifier, ledger = make_notifier(tmp_path, session)
    with pytest.raises(DeliveryUncertain, match='exact attachment is absent'):
        notifier.deliver(make_item())
    assert ledger.get(REPORT, SSCC, CHAT)['state'] == 'POSTED_UNCONFIRMED'


@pytest.mark.parametrize('value', [{}, ['invalid'], {'code': 'error', 'url': MAX_UPLOAD_URL}])
def test_invalid_upload_init_fails_before_binary_upload(tmp_path, value):
    session = FakeSession()
    session.post = lambda *args, **kw: Response(value=value)
    notifier, ledger = make_notifier(tmp_path, session)
    with pytest.raises(DeliveryUncertain):
        notifier.deliver(make_item())
    assert ledger.get(REPORT, SSCC, CHAT)['state'] == 'UPLOAD_PENDING'


def test_oversized_caption_is_rejected_before_any_upload(tmp_path):
    from dataclasses import replace
    session = FakeSession()
    notifier, _ = make_notifier(tmp_path, session)
    with pytest.raises(ValueError, match='4000-character'):
        notifier.deliver(replace(make_item(), text='x' * 4001))
    assert session.posts == []
