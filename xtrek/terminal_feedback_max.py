"""MAX adapter. Reuse existing GET bot identity and complete history mechanism.

POST_STARTED is deliberately permanent until a matching GET proves delivery.
A timeout, HTTP error, crash or successful POST response never grants a resend.
"""
from .pallet_label_max_notifier import MAX_API, MaxPalletLabelNotifier, DeliveryUncertain, HistoryIncomplete


class TerminalHistoryReader(MaxPalletLabelNotifier):
    def _get(self, path, params=None):
        payload = super()._get(path, params)
        if path == '/messages':
            if (not isinstance(payload, dict) or not isinstance(payload.get('messages'), list)
                    or any(not isinstance(item, dict) or not isinstance(item.get('timestamp'), (int, float))
                           for item in payload['messages'])):
                raise HistoryIncomplete('Malformed MAX history; delivery held')
        return payload


class MaxTerminalTransport:
    def __init__(self, *, session, token, route, created_at, max_history_pages=100):
        self.reader = TerminalHistoryReader(
            token=token, chat_id=route['chat_id'], expected_bot_user_id=route['bot_user_id'],
            ledger=None, session=session, history_start_ms=int(created_at * 1000) - 60000,
            max_history_pages=max_history_pages)

    def find(self, row):
        self.reader._verify_bot()
        matches = []
        for message in self.reader._history():
            body = message.get('body') or {}
            if (str((message.get('sender') or {}).get('user_id')) == self.reader.expected_bot_user_id
                    and str((message.get('recipient') or {}).get('chat_id')) == self.reader.chat_id
                    and body.get('text') == row['message_text'] and body.get('mid')):
                matches.append(str(body['mid']))
        if len(set(matches)) > 1:
            raise DeliveryUncertain('Multiple GET matches require manual reconciliation')
        return matches[0] if matches else None

    def post(self, row):
        response = self.reader.session.post(
            MAX_API + '/messages', params={'chat_id': self.reader.chat_id},
            headers={'Authorization': self.reader.token},
            json={'text': row['message_text'], 'notify': True}, timeout=20)
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
