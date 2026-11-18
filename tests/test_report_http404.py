"""HTTP adapter regressions for missing-code batches; no external requests."""
import json
from unittest.mock import MagicMock

import pytest

from xtrek.trueapi import HonestSignAPI
from xtrek.utils import AggregationAnalyzer
from test_report_status_review_regressions import missing_code_response


def test_final_http404_batch_preserves_unused_sscc(monkeypatch, tmp_path):
    boxes = [{'boxNumber': f'{i:018d}', 'productNumbersFull': [f'010468003824076821{i:06d}']}
             for i in range(501)]
    path = tmp_path / 'report.json'
    path.write_text(json.dumps({'readyBox': boxes}))
    batches = []

    def post(url, *, json, **kwargs):
        status = 404 if all(c.startswith('00') for c in json) else 200
        batches.append((len(json), status))
        response = MagicMock(status_code=status)
        response.json.return_value = [missing_code_response(c) if c.startswith('00') else
                                     {'cisInfo': {'cis': c, 'requestedCis': c, 'status': 'EMITTED'}}
                                     for c in json]
        return response

    monkeypatch.setattr('xtrek.trueapi.requests.post', post)
    nk = MagicMock()
    nk.product_info.return_value = {'isSet': False}
    analyzer = AggregationAnalyzer(HonestSignAPI(token='offline', host='https://example.test'), nk)
    # Fix set iteration order to exercise the real batcher with an SSCC-only tail.
    batcher = analyzer.check_statuses
    monkeypatch.setattr(analyzer, 'check_statuses', lambda codes: batcher(sorted(codes, key=lambda c: c.startswith('00'))))
    assert analyzer.check_report(str(path), strict_prerequisites=True) is None
    assert batches == [(1000, 200), (2, 404)]


@pytest.mark.parametrize('body', [None, {}, {'errorCode': '404'}, [], ['missing'],
                                [{'errorCode': '404', 'cisInfo': None}], 'invalid-json'])
def test_unstructured_http404_does_not_confirm_absence(monkeypatch, tmp_path, body):
    code = '00000000000000000001'
    path = tmp_path / 'report.json'
    child = '010468003824076821TEST'
    path.write_text(json.dumps({'readyBox': [{'boxNumber': code[2:], 'productNumbersFull': [child]}]}))
    response = MagicMock(status_code=404)
    if body == 'invalid-json':
        response.json.side_effect = ValueError('Invalid JSON')
    else:
        response.json.return_value = body
    monkeypatch.setattr('xtrek.trueapi.requests.post', MagicMock(return_value=response))
    api, nk = HonestSignAPI(token='offline', host='https://example.test'), MagicMock()
    nk.product_info.return_value = {'isSet': False}
    checked = AggregationAnalyzer(api, nk).check_report(str(path), strict_prerequisites=True)
    assert set(checked) == {'waitingaggregatestatus', 'waitingstatus'}
    assert checked.feedback_facts['scan_case'] is None


@pytest.mark.parametrize('defect', ['identity', 'status', 'other_error'])
def test_http404_does_not_bypass_identity_and_status_checks(monkeypatch, tmp_path, defect):
    code = '00000000000000000001'
    child = '010468003824076821TEST'
    path = tmp_path / 'report.json'
    path.write_text(json.dumps({'readyBox': [{'boxNumber': code[2:], 'productNumbersFull': [child]}]}))
    row = missing_code_response(code)
    if defect == 'identity':
        row['cisInfo']['requestedCis'] = '00999999999999999999'
    elif defect == 'status':
        row['cisInfo']['status'] = 'INTRODUCED'
    else:
        row['errorCode'] = '503'
        row['cisInfo']['status'] = 'INTRODUCED'
    response = MagicMock(status_code=404)
    response.json.return_value = [row]
    monkeypatch.setattr('xtrek.trueapi.requests.post', MagicMock(return_value=response))
    nk = MagicMock()
    nk.product_info.return_value = {'isSet': False}
    analyzer = AggregationAnalyzer(HonestSignAPI(token='offline', host='https://example.test'), nk)
    checked = analyzer.check_report(str(path), strict_prerequisites=True)
    assert set(checked) == {'waitingaggregatestatus', 'waitingstatus'}
    assert checked.feedback_facts['scan_case'] is None
