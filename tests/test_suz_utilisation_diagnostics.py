import json
import logging
from unittest.mock import Mock

import pytest
import requests

from xtrek.suz import SUZ, SUZ_ERROR_LOG_LIMIT


@pytest.fixture
def send_error(tmp_path, monkeypatch, caplog):
    body = tmp_path / 'request.json'
    body.write_text('{"sntins":["request-only-code"]}')
    signature = tmp_path / 'request.sig'
    signature.write_text('private-signature')
    api = SUZ(token='private-token', omsId='example-oms', clientToken='example-connection')
    monkeypatch.setattr('xtrek.suz.tempfile.tempdir', str(tmp_path))
    caplog.set_level(logging.ERROR, logger='xtrek.suz')

    def send(text, status=400):
        response = requests.Response()
        response.status_code = status
        response.encoding = 'utf-8'
        response._content = text.encode('utf-8')
        response.url = 'https://example.invalid/utilisation'
        response.request = requests.Request('POST', response.url, headers={
            'clientToken': 'private-token', 'X-Signature': 'private-signature',
            'Authorization': 'Bearer private-bearer',
        }).prepare()
        post = Mock(return_value=response)
        monkeypatch.setattr('xtrek.suz.requests.post', post)
        with pytest.raises(requests.HTTPError) as raised:
            api.utilisation_send(str(body), str(signature), max_retries=100, orderId='example-order')
        assert raised.value.response is response
        post.assert_called_once()
        assert 'request-only-code' not in caplog.text
        return response

    return send


@pytest.mark.parametrize('status', [400, 429, 500])
def test_short_error_is_visible_and_http_error_is_preserved(send_error, caplog, tmp_path, status):
    text = json.dumps({'fieldErrors': [{'errorCode': 7745,
        'fieldError': 'Дата истечения срока годности должна быть равна или больше текущей даты',
        'fieldName': 'attributes.expirationDate'}], 'success': False}, ensure_ascii=False)
    send_error(text, status)
    record = next(r for r in caplog.records if 'тело ответа:' in r.getMessage())
    assert json.loads(record.getMessage().split('тело ответа: ', 1)[1]) == text
    assert not list(tmp_path.glob('xtrek-suz-error-*'))


def test_large_code_list_is_only_in_persistent_private_file(send_error, caplog, tmp_path):
    text = json.dumps({'fieldErrors': [{'errorCode': 7745, 'fieldError': 'Bad date'}],
                       'sntins': ['code-only-in-file-' + str(i) for i in range(1000)]})
    send_error(text)
    files = list(tmp_path.glob('xtrek-suz-error-*.txt'))
    assert len(files) == 1
    assert files[0].read_text() == text
    assert files[0].stat().st_mode & 0o777 == 0o600
    assert str(files[0]) in caplog.text
    assert 'code-only-in-file-' not in caplog.text
    assert 'Bad date' not in caplog.text
    assert max(len(r.getMessage().encode()) for r in caplog.records) < SUZ_ERROR_LOG_LIMIT


@pytest.mark.parametrize('text, saved', [
    ('a' * (SUZ_ERROR_LOG_LIMIT - 2), False),
    ('a' * (SUZ_ERROR_LOG_LIMIT - 1), True),
    ('я' * (SUZ_ERROR_LOG_LIMIT // 2), True),
    ('\n' * (SUZ_ERROR_LOG_LIMIT // 2), True),
    ('', False),
])
def test_limit_counts_encoded_rendering(send_error, caplog, tmp_path, text, saved):
    send_error(text)
    files = list(tmp_path.glob('xtrek-suz-error-*.txt'))
    assert bool(files) == saved
    if saved:
        assert files[0].read_text() == text
    else:
        record = next(r for r in caplog.records if 'тело ответа:' in r.getMessage())
        rendered = record.getMessage().split('тело ответа: ', 1)[1]
        assert len(rendered.encode()) <= SUZ_ERROR_LOG_LIMIT
        assert '\n' not in record.getMessage()


@pytest.mark.parametrize('large', [False, True])
def test_echoed_credentials_are_redacted_in_log_and_file(send_error, caplog, tmp_path, large):
    text = 'private-token private-signature Bearer private-bearer private-bearer'
    if large:
        text += 'x' * SUZ_ERROR_LOG_LIMIT
    send_error(text)
    saved = ''.join(p.read_text() for p in tmp_path.glob('xtrek-suz-error-*.txt'))
    assert 'private-' not in caplog.text + saved
    assert (saved if large else caplog.text).count('[REDACTED]') == 4


def test_repeated_errors_get_distinct_files(send_error, tmp_path):
    text = 'x' * SUZ_ERROR_LOG_LIMIT
    send_error(text)
    send_error(text)
    files = list(tmp_path.glob('xtrek-suz-error-*.txt'))
    assert len(files) == 2
    assert all(p.read_text() == text for p in files)


def test_tmp_failure_does_not_hide_http_error_or_dump_body(send_error, monkeypatch, caplog):
    monkeypatch.setattr('xtrek.suz.tempfile.NamedTemporaryFile', Mock(side_effect=OSError('disk full')))
    send_error('private-long-code-' * SUZ_ERROR_LOG_LIMIT)
    assert 'большой ответ не сохранён в tmp (OSError)' in caplog.text
    assert 'private-long-code-' not in caplog.text


def test_success_has_no_error_log_or_file(tmp_path, monkeypatch, caplog):
    body, signature = tmp_path / 'body.json', tmp_path / 'body.sig'
    body.write_text('{}')
    signature.write_text('c2ln')
    monkeypatch.setattr('xtrek.suz.tempfile.tempdir', str(tmp_path))
    response = requests.Response()
    response.status_code = 200
    response._content = b'{"reportId":"accepted-id"}'
    monkeypatch.setattr('xtrek.suz.requests.post', lambda *a, **kw: response)
    with caplog.at_level(logging.ERROR, logger='xtrek.suz'):
        api = SUZ(token='test-token', omsId='example-oms', clientToken='example-connection')
        assert api.utilisation_send(str(body), str(signature)) == 'accepted-id'
    assert not [r for r in caplog.records if r.levelno >= logging.ERROR]
    assert not list(tmp_path.glob('xtrek-suz-error-*'))
