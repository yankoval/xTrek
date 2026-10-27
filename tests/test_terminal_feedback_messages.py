import hashlib
import json
from unittest.mock import MagicMock

import pytest
from celery.exceptions import Retry

from xtrek import terminal_feedback as feedback
from xtrek.terminal_feedback_facts import validated_facts
from xtrek.terminal_feedback_messages import compact_message, details_text, RESCAN_BOX, RESCAN_REMAINDER
from xtrek.terminal_feedback_s3 import S3TerminalLedger
from xtrek.utils import AggregationAnalyzer
from test_terminal_feedback import settings, enable
from test_terminal_feedback_s3 import Store
from test_equipment_report_prerequisites import router

RID = 'T-2133-77-002-10.2028-С-08-13-7eb1daae-9e91-4bf6-bb2c-9cad6a5b43c6'
SSCC = '346070517900856326'


def analyze(tmp_path, *, states=('EMITTED',), box_states=('INTRODUCED',), wrong_composition=False,
            missing_composition=False, duplicates=False, missing_status=False, conflicting_status=False):
    boxes, status, composition = [], [], {}
    for index, state in enumerate(states):
        box = SSCC if index == 0 else f'{index:018d}'
        code = f'010461011762137921{index:013d}'
        codes = [code, code] if duplicates else [code]
        boxes.append({'boxNumber': box, 'productNumbersFull': [c + '\x1d93PRIVATE' for c in codes]})
        status.append({'cisInfo': {'cis': '00' + box, 'status': box_states[index]}})
        if not missing_status:
            if conflicting_status:
                status.append({'cisInfo': {'cis': code, 'status': 'RETIRED'}})
            status.append({'cisInfo': {'cis': code, 'status': state}})
        composition['00' + box] = ['different'] if wrong_composition else [code]
    path = tmp_path / 'report.json'
    path.write_text(json.dumps({'readyBox': boxes}))
    api, nk = MagicMock(), MagicMock()
    api.get_list_cis_info.return_value = status
    api.get_aggregated_cis_list.return_value = {} if missing_composition else composition
    nk.product_info.return_value = {'isSet': True}
    result = AggregationAnalyzer(api, nk).check_report(str(path))
    return result, api, path


def row_for(facts, category='report_aggregate_conflict'):
    return {'event_id': feedback.event_identity('bucket', 'equipment-reports/' + RID + '.json'),
            'production_order_id': RID, 'category': category, 'stage': 'report-validation',
            'facts_json': json.dumps(facts),
            'context_json': json.dumps({'article': '2133-77-002', 'batch': '10.2028-С-08-13'})}


def test_only_sscc_verified_contents_and_source_hash(tmp_path):
    result, api, path = analyze(tmp_path)
    facts = result.feedback_facts
    assert facts['products_ok'] is True
    assert facts['source_sha256'] == hashlib.sha256(path.read_text().encode()).hexdigest()
    body = compact_message(row_for(facts))
    assert SSCC in body and 'короб №1' in body and RESCAN_BOX in body
    assert RID in body and 'PRIVATE' not in json.dumps(facts) + body
    assert len(body.splitlines()) == 6 and len(body) < 1500
    api.get_aggregated_cis_list.assert_not_called()


@pytest.mark.parametrize('kwargs', [dict(missing_status=True), dict(conflicting_status=True),
    dict(duplicates=True), dict(states=('RETIRED',)), dict(states=('APPLIED',))])
def test_never_replace_passport_when_contents_not_proven(tmp_path, kwargs):
    result, _, _ = analyze(tmp_path, **kwargs)
    assert result.feedback_facts['products_ok'] is False
    assert RESCAN_BOX not in compact_message(row_for(result.feedback_facts))


@pytest.mark.parametrize('states,box_states,options,case,category,action', [
    (('INTRODUCED',), ('INTRODUCED',), {}, 'full_repeat', 'report_repeat_scan', 'Не сканируйте эти короба повторно'),
    (('INTRODUCED','EMITTED'), ('INTRODUCED','NOT_FOUND'), {}, 'partial_repeat', 'report_partial_repeat', RESCAN_REMAINDER),
    (('INTRODUCED','RETIRED'), ('INTRODUCED','NOT_FOUND'), {}, 'mixed', 'report_scan_conflict', 'сверки кодов'),
    (('INTRODUCED',), ('NOT_FOUND',), {}, 'mixed', 'report_scan_conflict', 'сверки кодов'),
    (('INTRODUCED',), ('INTRODUCED',), {'wrong_composition':True}, 'mixed', 'report_scan_conflict', 'сверки кодов'),
    (('INTRODUCED',), ('INTRODUCED',), {'missing_composition':True}, 'pending', 'report_scan_conflict', 'сверки кодов'),
    (('INTRODUCED',), ('INTRODUCED',), {'duplicates':True}, 'mixed', 'report_scan_conflict', 'сверки кодов'),
])
def test_repeat_scan_matrix(tmp_path, states, box_states, options, case, category, action):
    result, _, _ = analyze(tmp_path, states=states, box_states=box_states, **options)
    facts = result.feedback_facts
    assert validated_facts(facts) == facts
    assert facts['scan_case'] == case
    text = compact_message(row_for(facts, category))
    assert action in text and RESCAN_BOX not in text
    assert SSCC in text


@pytest.mark.parametrize('backend', ['sqlite', 's3'])
def test_full_list_frozen_and_exported_without_hidden_omissions(tmp_path, backend):
    result, _, _ = analyze(tmp_path)
    facts = result.feedback_facts
    items = [{'kind':'box', 'id': f'{i:018d}', 'position': i+1} for i in range(3000)]
    facts['issues'][0].update(count=len(items), items=items)
    ledger = (feedback.TerminalLedger.initialize(tmp_path / 'ledger.db') if backend == 'sqlite'
              else S3TerminalLedger(Store(str(tmp_path / 's3.db')), 's3://bucket/events'))
    row = ledger.record('bucket', 'equipment-reports/T.json', 'report-validation', 'report_aggregate_conflict', facts=facts)
    first = ledger.get(row['event_id'])
    facts['issues'][0]['items'] = []
    assert ledger.record('bucket', 'equipment-reports/T.json', 'report-validation', 'invalid_report', facts=facts) == first
    row = ledger.bind_route(row['event_id'], 'T', None, {'chat_id':'test'}, context={'article':'test'})
    assert '3000' in row['message_text'] and 'ещё 2998 — в деталях' in row['message_text']
    assert len(row['message_text']) < 3500
    details = details_text(row)
    for item in items:
        assert item['id'] in details
    assert len(items) == 3000


@pytest.mark.parametrize('bad', [{'token':'PRIVATE'}, {'version':99}, None])
def test_bad_facts_use_safe_fallback(bad):
    text = compact_message(row_for(bad))
    assert 'состояние вложений не подтверждено' in text
    assert RESCAN_BOX not in text and 'PRIVATE' not in text


def test_unknown_number_format_explicitly_withheld(tmp_path):
    result, _, _ = analyze(tmp_path)
    facts = result.feedback_facts
    facts['issues'][0]['items'][0]['id'] = None
    text = compact_message(row_for(facts))
    assert 'номер скрыт' in text and RESCAN_BOX not in text


@pytest.mark.parametrize('case', ['full_repeat', 'partial_repeat', 'mixed'])
def test_new_scan_router_records_once_without_business_posts(tmp_path, router, settings, monkeypatch, case):
    tasks, task, _ = router
    enable(tasks, settings, tmp_path)
    options = {'states': ('INTRODUCED',)}
    if case != 'full_repeat':
        options = {'states': ('INTRODUCED', 'EMITTED' if case == 'partial_repeat' else 'RETIRED'),
                   'box_states': ('INTRODUCED', 'NOT_FOUND')}
    result, _, _ = analyze(tmp_path, **options)
    monkeypatch.setattr('xtrek.equipment_progress.has_report_progress', lambda *args: False)
    check = MagicMock(return_value={'path': result})
    monkeypatch.setattr(tasks, 'check_aggregation_reports', check)
    post = MagicMock(side_effect=AssertionError('No business post'))
    monkeypatch.setattr(tasks, 'sign_and_send_utilisation', post)
    data = {'bucket':'internal-bucket', 'key':'equipment-reports/T.json'}
    assert isinstance(task.apply(args=[data]).result, feedback.TerminalRecorded)
    assert isinstance(task.apply(args=[data]).result, feedback.TerminalRecorded)
    check.assert_called_once(); post.assert_not_called()
    saved = feedback.TerminalLedger(settings['ledger_path']).pending()[0]
    assert json.loads(saved['facts_json']) == result.feedback_facts


def test_report_own_completion_is_not_repeat_scan(tmp_path, router, monkeypatch):
    tasks, _, _ = router
    result, _, _ = analyze(tmp_path, states=('INTRODUCED',))
    monkeypatch.setattr(tasks, 'check_aggregation_reports', lambda *args: {'path':result})
    monkeypatch.setattr('xtrek.equipment_progress.has_report_progress', lambda *args: True)
    assert tasks.logic_start_equipment_reports('bucket/equipment-reports/T.json') == 'Equipment report already finished'


def test_safe_facts_survive_persistence_only_retry(tmp_path, router, settings, monkeypatch):
    tasks, task, _ = router
    enable(tasks, settings, tmp_path)
    result, _, _ = analyze(tmp_path)
    monkeypatch.setattr(tasks, 'check_aggregation_reports', lambda *args: {'path':result})
    original = feedback.TerminalLedger.record
    monkeypatch.setattr(feedback.TerminalLedger, 'record', MagicMock(side_effect=OSError()))
    retry = MagicMock(side_effect=Retry())
    monkeypatch.setattr(task, 'retry', retry)
    data = {'bucket':'internal-bucket', 'key':'equipment-reports/T.json'}
    assert task.apply(args=[data]).state == 'RETRY'
    queued = retry.call_args.kwargs['kwargs']
    assert queued['terminal_facts'] == result.feedback_facts
    assert 'PRIVATE' not in json.dumps(queued)
    monkeypatch.setattr(feedback.TerminalLedger, 'record', original)
    monkeypatch.setattr(tasks, 'check_aggregation_reports', MagicMock(side_effect=AssertionError('No business replay')))
    assert isinstance(task.apply(kwargs=queued).result, feedback.TerminalRecorded)
    saved = feedback.TerminalLedger(settings['ledger_path']).pending()[0]
    assert json.loads(saved['facts_json']) == result.feedback_facts


def test_all_480_emitted_only_box_107_used(tmp_path):
    from collections import Counter
    from xtrek.terminal_feedback_facts import report_facts
    boxes, children, aggregates, statuses = [], Counter(), Counter(), {}
    for index in range(120):
        sscc = SSCC if index == 106 else f'{index:018d}'
        codes = [f'010461011762137921{index * 4 + i:013d}' for i in range(4)]
        boxes.append({'boxNumber':sscc, 'productNumbersFull':codes})
        children.update(codes); aggregates['00' + sscc] += 1
        statuses.update({c:{'status':'EMITTED'} for c in codes})
    statuses['00' + SSCC] = {'status':'INTRODUCED'}
    facts = report_facts({'alreadyregistered':['00' + SSCC + ' (Статус: INTRODUCED)']}, boxes, (),
                         statuses, children, aggregates, json.dumps(boxes))
    assert facts['products_total'] == facts['products_checked'] == 480
    body = compact_message(row_for(facts))
    assert 'короб №107: ' + SSCC in body
    assert 'Во всём отчёте ещё не в обороте: 480 из 480 кодов (эмитированы)' in body
    assert RESCAN_BOX in body


@pytest.mark.parametrize('failure', ['missing_config', 'storage_error', 'own_task', 'own_guard', 'none'])
def test_progress_absence_must_be_proven(monkeypatch, failure):
    from xtrek.equipment_progress import has_report_progress
    from xtrek.terminal_feedback import WorkflowPending
    store = MagicMock()
    store.exists.return_value = failure == 'own_task'
    store.read_lock_object.return_value = ('{}','etag') if failure == 'own_guard' else None
    if failure == 'storage_error':
        store.exists.side_effect = OSError('PRIVATE')
    monkeypatch.setattr('xtrek.equipment_progress.get_storage', lambda *a: store)
    config = {k:'s3://synthetic/' + k for k in ('utilisation_tasks_path','utilisation_receipts','utilisation_reports','operation_state_path')}
    if failure == 'missing_config':
        config = {}
    if failure in {'missing_config','storage_error'}:
        with pytest.raises((WorkflowPending, OSError)):
            has_report_progress('T', config)
    else:
        assert has_report_progress('T', config) == (failure != 'none')
    store.set_tags.assert_not_called(); store.write_lock_object.assert_not_called()


def test_missing_composition_waits_without_terminal_event(tmp_path, router, settings, monkeypatch):
    tasks, task, _ = router
    enable(tasks, settings, tmp_path)
    result, _, _ = analyze(tmp_path, states=('INTRODUCED',), missing_composition=True)
    monkeypatch.setattr(tasks, 'check_aggregation_reports', lambda *a: {'path':result})
    monkeypatch.setattr('xtrek.equipment_progress.has_report_progress', lambda *a: False)
    monkeypatch.setattr(task, 'retry', MagicMock(side_effect=Retry()))
    assert task.apply(args=[{'bucket':'internal-bucket','key':'equipment-reports/T.json'}]).state == 'RETRY'
    assert not feedback.TerminalLedger(settings['ledger_path']).pending()


def test_pallet_composition_must_match_for_full_repeat():
    from xtrek.terminal_feedback_scan import diagnose_scan
    code = '010461011762137921SERIAL'
    box = {'boxNumber':SSCC, 'productNumbersFull':[code]}
    pallet = {'palletNumber':'123456789012345678', 'readyBox':[box]}
    statuses = {c:{'status':'INTRODUCED'} for c in ['00'+SSCC, '00'+pallet['palletNumber'], code]}
    api = MagicMock()
    api.get_aggregated_cis_list.return_value = {'00'+SSCC:[code], '00'+pallet['palletNumber']:['00'+SSCC]}
    assert diagnose_scan([box], [pallet], statuses, {}, api)['case'] == 'full_repeat'
    api.get_aggregated_cis_list.return_value['00'+pallet['palletNumber']] = ['00999999999999999999']
    assert diagnose_scan([box], [pallet], statuses, {}, api)['case'] == 'mixed'


def test_sqlite_details_lookup_is_read_only_and_refuses_ambiguity(tmp_path):
    import sqlite3
    from xtrek.terminal_feedback_messages import read_sqlite_event
    ledger = feedback.TerminalLedger.initialize(tmp_path / 'ledger.db')
    row = ledger.record('bucket', 'equipment-reports/T.json', 'report-validation', 'invalid_report')
    assert read_sqlite_event(ledger.path, row['event_id'][:16])['event_id'] == row['event_id']
    with sqlite3.connect(ledger.path) as db:
        db.execute('INSERT INTO terminal_events SELECT ?,'
                   'bucket,source_key,stage,category,terminal_state,delivery_state,production_order_id,'
                   'operator_raw,route_json,message_text,created_at,message_id,context_json,facts_json,attachment_json FROM terminal_events',
                   (row['event_id'][:16] + 'f' * 48,))
    with pytest.raises(ValueError, match='ambiguous'):
        read_sqlite_event(ledger.path, row['event_id'][:16])
    assert read_sqlite_event(ledger.path, row['event_id'])['event_id'] == row['event_id']
    missing = tmp_path / 'missing.db'
    with pytest.raises(sqlite3.OperationalError):
        read_sqlite_event(missing, row['event_id'])
    assert not missing.exists()


def test_old_reserved_message_survives_schema_migration(tmp_path):
    import sqlite3
    ledger = feedback.TerminalLedger.initialize(tmp_path / 'ledger.db')
    row = ledger.record('bucket', 'equipment-reports/T.json', 'report-validation', 'invalid_report')
    row = ledger.bind_route(row['event_id'], 'T', 'Alice', {'chat_id':'legacy'})
    with sqlite3.connect(ledger.path) as db:
        db.execute("UPDATE terminal_events SET message_text='LEGACY FROZEN BODY', delivery_state='POST_STARTED'")
        db.execute('ALTER TABLE terminal_events DROP COLUMN facts_json')
    reopened = feedback.TerminalLedger(ledger.path)
    saved = reopened.bind_route(row['event_id'], 'T', 'Alice', {'chat_id':'changed'})
    assert saved['message_text'] == 'LEGACY FROZEN BODY'
    assert saved['delivery_state'] == 'POST_STARTED' and saved['facts_json'] is None


def test_strict_repeat_precheck_never_marks_new_report_finished(tmp_path, monkeypatch):
    result, api, path = analyze(tmp_path, states=('INTRODUCED',))
    monkeypatch.setattr('xtrek.equipment_progress.has_report_progress', lambda *args: False)
    store = MagicMock(); store.read_text.return_value = path.read_text()
    monkeypatch.setattr('xtrek.utils.get_storage', lambda *a: store)
    nk = MagicMock(); nk.product_info.return_value = {'isSet':True}
    checked = AggregationAnalyzer(api, nk).check_report(str(path), strict_prerequisites=True)
    assert checked.feedback_facts['scan_case'] == 'full_repeat'
    store.set_tags.assert_not_called()
    monkeypatch.setattr('xtrek.equipment_progress.has_report_progress', lambda *args: True)
    checked = AggregationAnalyzer(api, nk).check_report(str(path), strict_prerequisites=True)
    assert checked.feedback_facts['scan_case'] is None
    store.set_tags.assert_called_once_with(str(path), {'check':'finished'})


def test_missing_composition_does_not_hide_rejected_codes(tmp_path, router, settings, monkeypatch):
    tasks, task, _ = router
    enable(tasks, settings, tmp_path)
    result, _, _ = analyze(tmp_path, states=('INTRODUCED','RETIRED'),
                           box_states=('INTRODUCED','NOT_FOUND'), missing_composition=True)
    assert result.feedback_facts['scan_case'] == 'pending'
    monkeypatch.setattr(tasks, 'check_aggregation_reports', lambda *a: {'path':result})
    monkeypatch.setattr('xtrek.equipment_progress.has_report_progress', lambda *a: False)
    assert isinstance(task.apply(args=[{'bucket':'internal-bucket','key':'equipment-reports/T.json'}]).result,
                      feedback.TerminalRecorded)


def test_conflicting_status_responses_never_prove_repeat(tmp_path):
    result, api, _ = analyze(tmp_path, states=('INTRODUCED',), conflicting_status=True)
    assert result.feedback_facts['scan_case'] == 'pending'
    api.get_aggregated_cis_list.assert_not_called()


@pytest.mark.parametrize('box_state,introduced,expected_case', [
    ('INTRODUCED', 0, None),
    ('NOT_FOUND', 2, 'mixed'),
    ('INTRODUCED', 2, 'mixed'),
    ('INTRODUCED', 6, 'full_repeat'),
    ('NOT_FOUND', 6, 'mixed'),
])
def test_operator_sees_box_and_current_contents_states(tmp_path, box_state, introduced, expected_case):
    codes = [f'010461011762137921{i:013d}' for i in range(6)]
    path = tmp_path / 'states.json'
    path.write_text(json.dumps({'readyBox':[{'boxNumber':SSCC,'productNumbersFull':codes}]}))
    api, nk = MagicMock(), MagicMock()
    nk.product_info.return_value = {'isSet':True}
    api.get_list_cis_info.return_value = [{'cisInfo':{'cis':'00'+SSCC,'status':box_state}}] + [
        {'cisInfo':{'cis':c,'status':'INTRODUCED' if i < introduced else 'EMITTED'}} for i,c in enumerate(codes)]
    api.get_aggregated_cis_list.return_value = {'00'+SSCC:codes}
    result = AggregationAnalyzer(api,nk).check_report(str(path))
    facts = result.feedback_facts
    assert facts['scan_case'] == expected_case
    category = ('report_aggregate_conflict' if expected_case is None else
                'report_repeat_scan' if expected_case == 'full_repeat' else 'report_scan_conflict')
    row = row_for(facts,category)
    text = compact_message(row)
    assert SSCC in text and 'короб №1' in text
    if box_state == 'INTRODUCED':
        assert SSCC + ' (в обороте)' in text
    else:
        assert SSCC + ' (не найден)' in text
    if introduced:
        assert f'вложения из отчёта уже в обороте: {introduced} из 6' in text
        assert RESCAN_BOX not in text
    else:
        assert 'вложения из отчёта ещё не в обороте: 6 из 6 (эмитированы)' in text
        assert RESCAN_BOX in text
    if introduced == 2:
        assert 'ещё не в обороте (эмитированы): 4' in text
    if expected_case == 'full_repeat':
        assert 'повторное сканирование' in text
    else:
        assert '— повторное сканирование' not in text
    assert len(text.splitlines()) == 6
    assert (f'вложения из отчёта уже в обороте: {introduced} из 6' if introduced else
            'вложения из отчёта ещё не в обороте: 6 из 6') in details_text(row)


@pytest.mark.parametrize('counts', [{'EMITTED':-1}, {'INTRODUCED':True}, {'TOKEN-secret':6}, {'EMITTED':'6'}])
def test_invalid_contents_counts_are_rejected(tmp_path, counts):
    result, _, _ = analyze(tmp_path)
    facts = result.feedback_facts
    facts['issues'][0]['items'][0]['contents'] = counts
    assert validated_facts(facts) is None
    assert RESCAN_BOX not in compact_message(row_for(facts))
