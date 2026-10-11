"""Bounded, restartable GET-only MAX history scan stored on the VPS."""
import json
import os
import time
from pathlib import Path
from .pallet_label_max_notifier import HistoryIncomplete


def scan_history_batch(notifier, checkpoint_path, *, page_budget=100):
    if not 1 <= page_budget <= 1000:
        raise ValueError('Invalid history batch budget')
    path = Path(checkpoint_path)
    if path.exists():
        state = json.loads(path.read_text())
        if state['chat_id'] != notifier.chat_id or state['bot_user_id'] != notifier.expected_bot_user_id:
            raise ValueError('History checkpoint identity mismatch')
    else:
        state = {'chat_id': notifier.chat_id, 'bot_user_id': notifier.expected_bot_user_id,
                 'through_ms': int(time.time() * 1000), 'cursor': None,
                 'messages': [], 'complete': False, 'pages': 0}
    notifier._verify_bot()
    for _ in range(page_budget):
        if state['complete']:
            break
        params = {'chat_id': notifier.chat_id, 'count': 100}
        if state['cursor'] is not None:
            params['from'] = state['cursor'] - 1
        payload = notifier._get('/messages', params)
        batch = payload.get('messages', [])
        stamps = [int(m.get('timestamp') or 0) for m in batch]
        if len(batch) >= 100 and (not stamps or min(stamps) == state['cursor']):
            raise HistoryIncomplete('MAX history cursor did not advance')
        state['messages'].extend(batch)
        state['pages'] += 1
        state['complete'] = len(batch) < 100
        if stamps:
            state['cursor'] = min(stamps)
        path.parent.mkdir(parents=True, exist_ok=True)
        temp = path.with_suffix('.tmp')
        with temp.open('w') as handle:
            json.dump(state, handle, ensure_ascii=False)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp, path)
    return state
