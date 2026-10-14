"""Receipt 2.0 diagnostics: synthetic reports only, no live requests/sends."""
import json
import logging
from unittest.mock import Mock

import pytest
import requests

from xtrek.suz import SUZ, SUZ_ERROR_LOG_LIMIT
from xtrek.suz_api_models import UtilisationReportStatus
from xtrek.suz_receipts import collect_failure_details, log_failure_details


def receipt(doc_id='RESULT', result_id='REPORT', event='RNMS_GIS_PROCESSED'):
    return {'resultDocId': result_id, 'state': 'FAILED', 'code': 1, 'operations': [
        {'operationType': 'REPORT_UTILIZE_RECEIVED', 'docId': 'SOURCE'},
        {'operationType': event, 'docId': doc_id},
    ]}


def document(cis='SYNTHETIC-CIS', code=12):
    return {'content': json.dumps({'cisList': [
        {'cis': cis, 'code': code, 'description': 'Отсутствует карточка товара (GTIN) в НК'},
    ]}, ensure_ascii=False)}


def unavailable(status=404):
    response = requests.Response()
    response.status_code = status
    return requests.HTTPError('untrusted-secret-' * 1000, response=response)


def test_two_receipts_all_result_events_and_double_json_decode():
    api = Mock()
    first = receipt()
    first['operations'].append({'operationType': 'RNMS_GIS_ROUTED', 'docId': 'ROUTING'})
    # A duplicate event must not fetch/store the same document twice.
    first['operations'].append(first['operations'][1])
    api.receipt_info.return_value = {'results': [first, receipt('SECOND', 'REPORT-2')]}
    api.receipt_document.side_effect = [document(), document('ROUTE-CIS', 2), document('SECOND-CIS', 0)]
    details = collect_failure_details(api, 'REPORT')
    assert details['state'] == 'complete'
    assert details['receipts'] == api.receipt_info.return_value['results']
    assert [d['cisList'][0]['code'] for d in details['documents']] == [12, 2, 0]
    assert [c.args for c in api.receipt_document.call_args_list] == [
        ('REPORT', 'RESULT'), ('REPORT', 'ROUTING'), ('REPORT-2', 'SECOND')]
    api.utilisation_send.assert_not_called()


def test_partial_read_retries_only_missing_document_then_caches_complete():
    api = Mock()
    api.receipt_info.return_value = {'results': [receipt('ONE'), receipt('TWO')]}
    api.receipt_document.side_effect = [document(), unavailable(), document('SECOND-CIS')]
    first = collect_failure_details(api, 'REPORT')
    assert first['state'] == 'partial'
    assert first['issues'] == [{'stage': 'document', 'resultDocId': 'REPORT',
                               'docId': 'TWO', 'errorType': 'HTTPError', 'httpStatus': 404}]
    second = collect_failure_details(api, 'REPORT', first)
    assert second['state'] == 'complete'
    assert len(second['documents']) == 2
    assert api.receipt_document.call_count == 3
    assert collect_failure_details(api, 'REPORT', second) is second
    assert api.receipt_info.call_count == 2


@pytest.mark.parametrize('answer', [None, {}, {'results': []}, {'results': [None]},
                                    {'results': [{'resultDocId': 'REPORT', 'operations': []}]}])
def test_empty_or_malformed_receipt_remains_retryable(answer):
    api = Mock()
    api.receipt_info.return_value = answer
    details = collect_failure_details(api, 'REPORT')
    assert details['state'] == 'unavailable'
    assert details['issues']
    api.receipt_document.assert_not_called()


@pytest.mark.parametrize('content', ['not-json', '[]', '{"cisList":[]}',
                                    '{"cisList":[{"cis":"X","code":false}]}'])
def test_bad_content_does_not_become_complete(content):
    api = Mock()
    api.receipt_info.return_value = {'results': [receipt()]}
    api.receipt_document.return_value = {'content': content}
    details = collect_failure_details(api, 'REPORT')
    assert details['state'] == 'unavailable'
    assert details['issues'][0]['stage'] == 'document'


def test_lost_receipt_read_preserves_previous_per_cis_results():
    api = Mock()
    api.receipt_info.return_value = {'results': [receipt('ONE'), receipt('TWO')]}
    api.receipt_document.side_effect = [document(), unavailable()]
    first = collect_failure_details(api, 'REPORT')
    api.receipt_info.side_effect = unavailable(503)
    second = collect_failure_details(api, 'REPORT', first)
    assert second['state'] == 'partial'
    assert second['documents'] == first['documents']
    assert second['receipts'] == first['receipts']
    assert 'untrusted-secret' not in json.dumps(second)


def test_api_uses_receipt_result_id_and_shared_scoped_headers(monkeypatch):
    api = SUZ(token='SCOPED-TOKEN', omsId='OMS', clientToken='CON')
    get = Mock(return_value={})
    monkeypatch.setattr(api, '_get', get)
    api.receipt_info('REPORT')
    api.receipt_document('RECEIPT', 'DOC')
    assert get.call_args_list[0].args == ('https://suzgrid.crpt.ru/api/v3/receipts/receipt',)
    assert get.call_args_list[0].kwargs['params'] == {'omsId': 'OMS', 'resultDocId': 'REPORT'}
    assert get.call_args_list[1].args == ('https://suzgrid.crpt.ru/api/v3/receipts/document',)
    assert get.call_args_list[1].kwargs['params'] == {
        'omsId': 'OMS', 'resultDocId': 'RECEIPT', 'docId': 'DOC'}
    assert api.headers['clientToken'] == 'SCOPED-TOKEN'


def test_large_diagnostics_reuse_private_tmp_and_keep_reason_in_journal(tmp_path, monkeypatch, caplog):
    monkeypatch.setattr('xtrek.suz.tempfile.tempdir', str(tmp_path))
    records = [{'cis': f'file-only-cis-{i}', 'code': 12,
                'description': 'Отсутствует карточка товара (GTIN) в НК'} for i in range(1000)]
    details = {'state': 'complete', 'receipts': [], 'documents': [
        {'resultDocId': 'REPORT', 'docId': 'DOC', 'cisList': records}], 'issues': []}
    status = UtilisationReportStatus('OMS', 'REPORT', 'FAILED', failureDetails=details)
    assert 'file-only-cis' not in repr(status)
    assert json.loads(status.to_json())['failureDetails']['documents'][0]['cisList'] == records
    with caplog.at_level(logging.ERROR, logger='xtrek.suz'):
        log_failure_details(details, {}, 'REPORT', 's3://bucket/status/ORDER.json')
    files = list(tmp_path.glob('xtrek-suz-error-*.txt'))
    assert len(files) == 1
    assert json.loads(files[0].read_text()) == details
    assert files[0].stat().st_mode & 0o777 == 0o600
    assert 'Отсутствует карточка товара (GTIN) в НК' in caplog.text
    assert '1000' in caplog.text and str(files[0]) in caplog.text
    assert 'file-only-cis' not in caplog.text
    assert max(len(r.getMessage().encode()) for r in caplog.records) < SUZ_ERROR_LOG_LIMIT


def test_diagnostic_tmp_failure_preserves_bounded_log_and_redacts_auth(tmp_path, monkeypatch, caplog):
    monkeypatch.setattr('xtrek.suz.tempfile.NamedTemporaryFile', Mock(side_effect=OSError('full')))
    details = {'state': 'partial', 'documents': [], 'issues': ['private-token' * 1000]}
    with caplog.at_level(logging.ERROR, logger='xtrek.suz'):
        log_failure_details(details, {'clientToken': 'private-token'}, 'REPORT', 'status.json')
    assert 'большой ответ не сохранён в tmp (OSError)' in caplog.text
    assert 'private-token' not in caplog.text
    assert max(len(r.getMessage().encode()) for r in caplog.records) < SUZ_ERROR_LOG_LIMIT


def test_unchanged_diagnostics_do_not_create_another_tmp_file(tmp_path, monkeypatch, caplog):
    monkeypatch.setattr('xtrek.suz.tempfile.tempdir', str(tmp_path))
    details = {'state': 'complete', 'documents': [], 'issues': ['large' * 2000]}
    with caplog.at_level(logging.ERROR, logger='xtrek.suz'):
        log_failure_details(details, {}, 'REPORT', 'status.json')
        log_failure_details(details, {}, 'REPORT', 'status.json', include_details=False)
    assert len(list(tmp_path.glob('xtrek-suz-error-*.txt'))) == 1


def test_receipt_http_error_body_obeys_existing_log_limit(tmp_path, monkeypatch, caplog):
    monkeypatch.setattr('xtrek.suz.tempfile.tempdir', str(tmp_path))
    api = SUZ(token='private-token', omsId='OMS', clientToken='CON')
    response = requests.Response()
    response.status_code = 500
    response.encoding = 'utf-8'
    response._content = ('private-token body-only-in-tmp ' * 1000).encode()
    response.request = requests.Request('GET', 'https://example.invalid', headers=api.headers).prepare()
    monkeypatch.setattr('xtrek.suz.requests.get', Mock(return_value=response))
    with caplog.at_level(logging.ERROR, logger='xtrek.suz'), pytest.raises(requests.HTTPError):
        api.receipt_info('REPORT')
    files = list(tmp_path.glob('xtrek-suz-error-*.txt'))
    assert len(files) == 1
    assert '[REDACTED]' in files[0].read_text()
    assert 'private-token' not in files[0].read_text()
    assert 'body-only-in-tmp' not in caplog.text
