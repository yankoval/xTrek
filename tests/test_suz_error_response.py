import json
import logging

import pytest
import requests

from xtrek.suz import SUZ


@pytest.fixture
def signed_request(tmp_path):
    body = tmp_path / 'body.json'
    body.write_text('{"sntins":["private-code"]}')
    signature = tmp_path / 'body.sig'
    signature.write_text('private-signature')
    api = SUZ(token='private-client-token', omsId='test-oms', clientToken='connection')
    return api, str(body), str(signature)


@pytest.mark.parametrize('status', [400, 429, 500])
def test_error_body_is_retained_without_retry(monkeypatch, caplog, signed_request, status):
    api, body, signature = signed_request
    response = requests.Response()
    response.status_code = status
    response.encoding = 'utf-8'
    response._content = '{"error":"Недопустимый orderId"}\nDetails\tend'.encode()
    calls = []

    def post(*args, **kwargs):
        calls.append((args, kwargs))
        return response

    monkeypatch.setattr('xtrek.suz.requests.post', post)
    with caplog.at_level(logging.ERROR, logger='xtrek.suz'):
        with pytest.raises(requests.HTTPError) as error:
            api._send_signed_request('https://example.invalid/utilisation', body, signature)

    assert error.value.response is response
    assert len(calls) == 1
    records = [r for r in caplog.records if 'response_body=' in r.getMessage()]
    assert len(records) == 1
    assert json.loads(records[0].getMessage().split('response_body=', 1)[1]) == response.text
    assert '\n' not in records[0].getMessage()
    assert 'private-code' not in caplog.text
    assert 'private-signature' not in caplog.text
    assert 'private-client-token' not in caplog.text


def test_echoed_credentials_are_redacted(monkeypatch, caplog, signed_request):
    api, body, signature = signed_request
    response = requests.Response()
    response.status_code = 400
    response._content = b'Invalid private-client-token / private-signature / Bearer private-bearer / private-bearer'
    monkeypatch.setattr('xtrek.suz.requests.post', lambda *a, **kw: response)
    with caplog.at_level(logging.ERROR, logger='xtrek.suz'):
        with pytest.raises(requests.HTTPError):
            api._send_signed_request('https://example.invalid/utilisation', body, signature,
                                     extra_headers={'Authorization': 'Bearer private-bearer'})
    assert 'private-' not in caplog.text
    assert caplog.text.count('[REDACTED]') == 4


@pytest.mark.parametrize('status, body_text', [(200, '{"reportId":"ok"}'), (400, '')])
def test_success_and_empty_error_body(monkeypatch, caplog, signed_request, status, body_text):
    api, body, signature = signed_request
    response = requests.Response()
    response.status_code = status
    response._content = body_text.encode()
    monkeypatch.setattr('xtrek.suz.requests.post', lambda *a, **kw: response)
    with caplog.at_level(logging.ERROR, logger='xtrek.suz'):
        if status == 200:
            assert api._send_signed_request('https://example.invalid/utilisation', body, signature) is response
            assert not caplog.records
        else:
            with pytest.raises(requests.HTTPError):
                api._send_signed_request('https://example.invalid/utilisation', body, signature)
            assert 'response_body=""' in caplog.text
