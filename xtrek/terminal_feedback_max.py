"""MAX adapter. Reuse existing GET bot identity and complete history mechanism.

POST_STARTED is deliberately permanent until a matching GET proves delivery.
A timeout, HTTP error, crash or successful POST response never grants a resend.
"""
import hashlib
import time
from urllib.parse import urlparse

from .terminal_feedback_messages import frozen_attachment
from .pallet_label_max_notifier import MAX_API, DeliveryConflict, DeliveryUncertain, HistoryIncomplete


class TerminalHistoryReader:
    """GET-only reader: terminal deliveries use their own ledger and fresh history.

    Do not inherit pallet delivery state or its preflight cache: find() is also
    called immediately after a terminal POST and must observe new messages.
    """
    def __init__(self, *, token, chat_id, expected_bot_user_id, session,
                 history_start_ms, max_history_pages=100):
        self.token = token
        self.chat_id = str(chat_id)
        self.expected_bot_user_id = str(expected_bot_user_id)
        self.session = session
        self.history_start_ms = int(history_start_ms)
        self.max_history_pages = int(max_history_pages)
        if not 1 <= self.max_history_pages <= 10000:
            raise ValueError('Invalid MAX history page limit')

    def _get(self, path, params=None):
        response = self.session.get(MAX_API + path, params=params,
                                    headers={'Authorization': self.token}, timeout=20)
        response.raise_for_status()
        try:
            payload = response.json()
        except ValueError as exc:
            raise HistoryIncomplete('Malformed MAX response; delivery held') from exc
        if not isinstance(payload, dict):
            raise HistoryIncomplete('Malformed MAX response; delivery held')
        if path == '/messages':
            if (not isinstance(payload.get('messages'), list)
                    or any(not isinstance(item, dict) or not isinstance(item.get('timestamp'), (int, float))
                           for item in payload['messages'])):
                raise HistoryIncomplete('Malformed MAX history; delivery held')
        return payload

    def _verify_bot(self):
        if str(self._get('/me').get('user_id')) != self.expected_bot_user_id:
            raise DeliveryConflict('Configured MAX token belongs to a different bot')

    def _history(self):
        messages, cursor = [], None
        for _ in range(self.max_history_pages):
            params = {'chat_id': self.chat_id, 'count': 100}
            if cursor is not None:
                params['from'] = cursor - 1
            batch = self._get('/messages', params)['messages']
            messages.extend(m for m in batch if int(m['timestamp']) >= self.history_start_ms)
            stamps = [int(m['timestamp']) for m in batch]
            if len(batch) < 100 or (stamps and min(stamps) < self.history_start_ms):
                return messages
            if not stamps or (cursor is not None and min(stamps) >= cursor):
                raise HistoryIncomplete('MAX history pagination cursor did not advance')
            cursor = min(stamps)
        raise HistoryIncomplete('MAX history page limit exceeded; delivery is held')


class MaxTerminalTransport:
    def __init__(self, *, session, token, route, created_at, max_history_pages=100):
        self.reader = TerminalHistoryReader(
            token=token, chat_id=route['chat_id'], expected_bot_user_id=route['bot_user_id'],
            session=session, history_start_ms=int(created_at * 1000) - 60000,
            max_history_pages=max_history_pages)

    @staticmethod
    def _media_url(url):
        parsed = urlparse(url if isinstance(url, str) else '')
        host = (parsed.hostname or '').lower()
        if (parsed.scheme != 'https' or parsed.username or parsed.password
                or not any(host == domain or host.endswith('.' + domain) for domain in ('max.ru', 'oneme.ru'))):
            raise DeliveryUncertain('Unexpected MAX media URL')
        return url

    def _verify_file(self, body, expected):
        attachments = body.get('attachments') or []
        if len(attachments) != 1 or attachments[0].get('type') != 'file':
            raise DeliveryUncertain('Expected terminal details file is missing')
        item = attachments[0]
        payload = item.get('payload') or {}
        if (item.get('filename') or payload.get('filename')) != expected['filename']:
            raise DeliveryUncertain('Terminal details filename mismatch')
        url = self._media_url(payload.get('url'))
        response = self.reader.session.get(url, headers={'Authorization': None},
                                          stream=True, allow_redirects=False, timeout=30)
        try:
            if response.status_code != 200:
                raise DeliveryUncertain('Terminal details download unavailable')
            digest, size = hashlib.sha256(), 0
            for chunk in response.iter_content(chunk_size=65536):
                size += len(chunk)
                if size > expected['size']:
                    raise DeliveryUncertain('Terminal details size mismatch')
                digest.update(chunk)
            if size != expected['size'] or digest.hexdigest() != expected['sha256']:
                raise DeliveryUncertain('Terminal details checksum mismatch')
        finally:
            response.close()

    def _upload_file(self, expected):
        response = self.reader.session.post(
            MAX_API + '/uploads', params={'type': 'file'},
            headers={'Authorization': self.reader.token}, timeout=20, allow_redirects=False)
        response.raise_for_status()
        init = response.json()
        if not isinstance(init, dict) or init.get('code'):
            raise DeliveryUncertain('MAX file upload initialization failed')
        url = self._media_url(init.get('url'))
        uploaded = self.reader.session.post(
            url, headers={'Authorization': None},
            files={'data': (expected['filename'], expected['text'].encode('utf-8'), 'text/plain; charset=utf-8')},
            timeout=60, allow_redirects=False)
        uploaded.raise_for_status()
        result = uploaded.json()
        if (not isinstance(result, dict) or result.get('code')
                or not isinstance(result.get('token'), str) or not result['token'].strip()):
            raise DeliveryUncertain('MAX file upload returned no token')
        # Allow small TXT processing. A refusal/timeout still cannot grant a
        # second message POST; the permanent reservation remains authoritative.
        time.sleep(2)
        return {'type': 'file', 'payload': {'token': result['token']}}

    def find(self, row):
        self.reader._verify_bot()
        matches = []
        for message in self.reader._history():
            body = message.get('body') or {}
            if (str((message.get('sender') or {}).get('user_id')) == self.reader.expected_bot_user_id
                    and str((message.get('recipient') or {}).get('chat_id')) == self.reader.chat_id
                    and body.get('text') == row['message_text'] and body.get('mid')):
                attachment = frozen_attachment(row)
                if attachment:
                    self._verify_file(body, attachment)
                matches.append(str(body['mid']))
        if len(set(matches)) > 1:
            raise DeliveryUncertain('Multiple GET matches require manual reconciliation')
        return matches[0] if matches else None

    def post(self, row):
        payload = {'text': row['message_text'], 'notify': True}
        attachment = frozen_attachment(row)
        if attachment:
            payload['attachments'] = [self._upload_file(attachment)]
        response = self.reader.session.post(
            MAX_API + '/messages', params={'chat_id': self.reader.chat_id},
            headers={'Authorization': self.reader.token},
            json=payload, timeout=20, allow_redirects=False)
        response.raise_for_status()
        # The POST response is not proof of delivery. Always require GET.


def deliver(ledger, row, transport):
    current = ledger.get(row['event_id'])
    if current['delivery_state'] == 'CONFIRMED':
        return 'CONFIRMED'
    if not current['route_json']:
        return 'ROUTE_HELD'
    message_id = transport.find(current)
    if message_id:
        ledger.confirmed(current['event_id'], message_id)
        return 'CONFIRMED'
    if not ledger.claim_post(current['event_id']):
        return 'GET_RECONCILIATION_REQUIRED'
    try:
        transport.post(current)
    except Exception:
        # Never expose exception text, HTTP payloads or request headers.
        return 'GET_RECONCILIATION_REQUIRED'
    message_id = transport.find(current)
    if message_id:
        ledger.confirmed(current['event_id'], message_id)
        return 'CONFIRMED'
    return 'GET_RECONCILIATION_REQUIRED'
