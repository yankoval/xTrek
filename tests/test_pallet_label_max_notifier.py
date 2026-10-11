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
            return Response(value={'token': 'fixture-file-token'})
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
                    'attachments': [{'type': 'file', 'payload': {'name': filename, 'token': 'fixture-file-token'}}],
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
        text=f'Ярлык паллета 1/1\nОтчёт: {REPORT}\nSSCC: {SSCC}\nКоробов: 2\nШтук: 10',
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
    assert result == {'state': 'ALREADY_CONFIRMED_BY_GET', 'message_id': 'already-there'}
    assert session.posts == []
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
