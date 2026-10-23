"""Yandex adapter: authenticated notification delivery, no business processing."""
import hashlib
import hmac
import json
import os
import time
import urllib.parse
import urllib.request


def handler(event, context):
    endpoint = os.environ['XTREK_S3_INGRESS_URL']
    url = urllib.parse.urlsplit(endpoint)
    if url.scheme != 'https' or not url.hostname or url.username or url.password or url.query or url.fragment:
        raise ValueError('A fixed authenticated HTTPS endpoint is required')
    routes = json.loads(os.environ['XTREK_S3_INGRESS_ROUTES_JSON'])
    messages = event.get('messages')
    if not isinstance(messages, list) or not messages or len(messages) > 10:
        raise ValueError('Invalid trigger envelope')
    items = []
    for message in messages:
        kind = message['event_metadata']['event_type']
        if kind not in {'yandex.cloud.events.storage.ObjectCreate', 'yandex.cloud.events.storage.ObjectUpdate'}:
            continue
        details = message['details']
        bucket, key = details['bucket_id'], details['object_id']
        if not isinstance(bucket, str) or not isinstance(key, str):
            raise ValueError('Invalid object reference')
        # Object IDs are already object keys; do not URL-decode or replace '+'.
        if any(key.startswith(p) for p in routes.get(bucket, [])) and not key.endswith('/'):
            items.append({'bucket': bucket, 'key': key})
    if not items:
        return {'statusCode': 200}
    body = json.dumps({'events': items}, ensure_ascii=False, separators=(',', ':')).encode('utf-8')
    if len(body) > 65536:
        raise ValueError('Notification batch too large')
    stamp = str(int(time.time()))
    secret = os.environ['XTREK_S3_INGRESS_SECRET'].encode()
    if len(secret) < 32:
        raise ValueError('Missing signing secret')
    sig = hmac.new(secret, stamp.encode() + b'\n' + body, hashlib.sha256).hexdigest()
    request = urllib.request.Request(endpoint, body, headers={'Content-Type': 'application/json',
        'X-Xtrek-Timestamp': stamp, 'X-Xtrek-Signature': sig}, method='POST')
    # Redirects must not forward signed requests to an unrelated endpoint.
    class NoRedirect(urllib.request.HTTPRedirectHandler):
        def redirect_request(self, req, fp, code, msg, headers, newurl):
            return None
    with urllib.request.build_opener(NoRedirect()).open(request, timeout=25) as response:
        if response.status != 202:
            raise RuntimeError('Ingress did not confirm durable registration')
        result = json.loads(response.read(8192))
        if set(result) != {'registered', 'known', 'ignored'} or any(type(v) is not int or v < 0 for v in result.values()) or sum(result.values()) != len(items):
            raise RuntimeError('Invalid registration acknowledgement')
    # Exceptions must escape: returning statusCode=500 alone may count as a
    # successful function execution and suppress the trigger's retry mechanism.
    return {'statusCode': 200}
