"""Offline regression of the 49-box report: confirmed violations stop immediately."""
import copy
import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from celery.exceptions import Retry

from xtrek import terminal_feedback as feedback, terminal_feedback_runtime as runtime
from xtrek.terminal_feedback_facts import validated_facts
from xtrek.utils import AggregationAnalyzer
from test_terminal_feedback import settings, enable
from test_equipment_report_prerequisites import router
from test_terminal_feedback_s3 import Store


EVIDENCE = json.loads((Path(__file__).parent / 'fixtures/pending_scan_t3515.json').read_text())
RID = EVIDENCE['id']
KEY = 'equipment-reports/' + RID + '.json'


@pytest.fixture
def scenario(monkeypatch):
    boxes, responses, composition = [], [], {}
    for group in EVIDENCE['issue_groups']:
        for item in group['items']:
            codes = [f'010468003824076821{item["position"]:05d}{i:05d}' for i in range(6)]
            boxes.append((item['position'], {'boxNumber': item['id'], 'productNumbersFull': codes}))
            sscc = '00' + item['id']
            if group['kind'] == 'repeatedbox':
                responses.append({'cisInfo': {'cis': sscc, 'status': 'INTRODUCED'}})
                composition[sscc] = codes
                state = 'INTRODUCED'
            else:
                # 343 responses can still contain 46 objects without a status.
                responses.append({'requestedCis': sscc, 'cisInfo': {}})
                state = 'EMITTED'
            responses.extend({'cisInfo': {'cis': c, 'status': state}} for c in codes)
    source = {'readyBox': [box for _, box in sorted(boxes)]}
    store, api, nk = MagicMock(), MagicMock(), MagicMock()
    store.read_text.return_value = json.dumps(source)
    api.get_list_cis_info.return_value = responses
    api.get_aggregated_cis_list.return_value = composition
    nk.product_info.return_value = {'isSet': False}
    monkeypatch.setattr('xtrek.utils.get_storage', lambda *a: store)
    monkeypatch.setattr('xtrek.equipment_progress.has_report_progress', lambda *a: False)
    analyzer = AggregationAnalyzer(api, nk)
    return analyzer, api, store


def check(scenario):
    return scenario[0].check_report('s3://internal-bucket/' + KEY, strict_prerequisites=True)


def ready_responses(scenario):
    responses = scenario[1].get_list_cis_info.return_value
    for row in responses:
        code = row['cisInfo'].get('cis') or row['requestedCis']
        row['cisInfo']['status'] = 'EMITTED' if code.startswith('01') else 'NOT_FOUND'
    return responses


def wire_review_router(scenario, router, settings, tmp_path, monkeypatch):
    tasks, task, _ = router
    enable(tasks, settings, tmp_path)
    checker = MagicMock(side_effect=lambda *args: {'path': check(scenario)})
    monkeypatch.setattr(tasks, 'check_aggregation_reports', checker)
    monkeypatch.setattr(task, 'retry', MagicMock(side_effect=Retry()))
    blocked = MagicMock(side_effect=AssertionError('No business document or continuation'))
    for name in ('create_utilisation_task_from_report', 'sign_and_send_utilisation',
                 'applied_checkpoint', 'create_introduce_task_from_report', 'sign_and_send_introduce'):
        monkeypatch.setattr(tasks, name, blocked)
    return tasks, task, checker, blocked


@pytest.mark.parametrize('aggregate', ['box', 'pallet'])
@pytest.mark.parametrize('state', ['missing', None, '', 'UNKNOWN', 'NEW_UNKNOWN_STATUS', 'conflict'])
def test_unconfirmed_sscc_with_ready_contents_waits_without_notification(
        scenario, router, settings, tmp_path, monkeypatch, state, aggregate):
    responses = ready_responses(scenario)
    if aggregate == 'pallet':
        source = json.loads(scenario[2].read_text.return_value)
        source = {'schemaVersion': 2, 'readyPallet': [{
            'palletNumber': '123456789012345678', 'palletAggregate': True,
            'readyBox': source['readyBox']}]}
        scenario[2].read_text.return_value = json.dumps(source)
        responses.insert(0, {'cisInfo': {'cis': '00123456789012345678', 'status': 'NOT_FOUND'}})
    box = next(r for r in responses if r['cisInfo'].get('cis', '').startswith('00'))
    code = box['cisInfo']['cis']
    if state == 'missing':
        responses.remove(box)
    elif state == 'conflict':
        responses.append({'cisInfo': {'cis': code, 'status': 'INTRODUCED'}})
    else:
        box['cisInfo']['status'] = state
    tasks, task, checker, blocked = wire_review_router(scenario, router, settings, tmp_path, monkeypatch)
    checked = check(scenario)
    assert set(checked) == {'waitingaggregatestatus'}
    assert validated_facts(checked.feedback_facts) == checked.feedback_facts
    assert checked.feedback_facts['scan_case'] is None
    for retries in [0, 200, 1200]:
        assert task.apply(args=[{'bucket': 'internal-bucket', 'key': KEY}], retries=retries).state == 'RETRY'
    assert checker.call_count == 3
    assert not feedback.TerminalLedger(settings['ledger_path']).pending()
    factory = MagicMock()
    assert runtime.drain(settings, {}, read_json=MagicMock(), transport_factory=factory) == {}
    factory.return_value.post.assert_not_called()
    blocked.assert_not_called()
    tasks.get_storage.return_value.set_tags.assert_not_called()


@pytest.mark.parametrize('backend', ['sqlite', 's3'])
@pytest.mark.parametrize('progress_available', [False, True])
@pytest.mark.parametrize('other_introduced', [False, True])
@pytest.mark.parametrize('state', ['WRITTEN_OFF', 'RETIRED', 'DISAGGREGATED'])
def test_rejected_content_ignores_progress_and_notifies_once(
        scenario, router, settings, tmp_path, monkeypatch, backend, progress_available, state, other_introduced):
    responses = ready_responses(scenario)
    children = [r['cisInfo'] for r in responses if r['cisInfo'].get('cis', '').startswith('01')]
    children[0]['status'] = state
    # Another code could be our own progress; it cannot excuse a rejection.
    if other_introduced:
        children[1]['status'] = 'INTRODUCED'
    progress = MagicMock(return_value=True if progress_available else None,
                         side_effect=None if progress_available else feedback.WorkflowPending('Progress unavailable'))
    monkeypatch.setattr('xtrek.equipment_progress.has_report_progress', progress)
    if backend == 's3':
        store = Store(str(tmp_path / 's3.db'))
        monkeypatch.setattr('xtrek.storage.get_storage', lambda *args: store)
        settings.update(ledger_backend='s3', ledger_path='s3://shared/events')
    tasks, task, checker, blocked = wire_review_router(scenario, router, settings, tmp_path, monkeypatch)
    for retries in [0, 200, 1200]:
        result = task.apply(args=[{'bucket': 'internal-bucket', 'key': KEY}], retries=retries)
        assert isinstance(result.result, feedback.TerminalRecorded)
    checker.assert_called_once()
    progress.assert_not_called()
    blocked.assert_not_called()
    tasks.get_storage.return_value.set_tags.assert_called_with(
        's3://internal-bucket/' + KEY, {'check': 'report_scan_conflict'})
    assert len(runtime.open_ledger(settings, tasks.config).pending()) == 1
    factory = MagicMock()
    factory.return_value.find.side_effect = [None, 'mid-rejected']
    reader = MagicMock(return_value={'operator': 'Alice'})
    assert runtime.drain(settings, {}, read_json=reader, transport_factory=factory) == {'CONFIRMED': 1}
    assert runtime.drain(settings, {}, read_json=reader, transport_factory=factory) == {}
    factory.return_value.post.assert_called_once()


def test_unconfirmed_sscc_waits_even_with_own_applied_progress(
        scenario, router, settings, tmp_path, monkeypatch):
    responses = ready_responses(scenario)
    for row in responses:
        if row['cisInfo'].get('cis', '').startswith('01'):
            row['cisInfo']['status'] = 'APPLIED'
    boxes = [r['cisInfo'] for r in responses if r['cisInfo'].get('cis', '').startswith('00')]
    boxes[0]['status'] = 'INTRODUCED'
    boxes[1]['status'] = 'UNKNOWN'
    progress = MagicMock(return_value=True)
    monkeypatch.setattr('xtrek.equipment_progress.has_report_progress', progress)
    tasks, task, _, blocked = wire_review_router(scenario, router, settings, tmp_path, monkeypatch)
    assert task.apply(args=[{'bucket': 'internal-bucket', 'key': KEY}]).state == 'RETRY'
    progress.assert_called_once()
    blocked.assert_not_called()
    assert not feedback.TerminalLedger(settings['ledger_path']).pending()


def test_restored_explicit_not_found_allows_ready_report_to_proceed(
        scenario, router, settings, tmp_path, monkeypatch):
    responses = ready_responses(scenario)
    box = next(r['cisInfo'] for r in responses if r['cisInfo'].get('cis', '').startswith('00'))
    box['status'] = 'UNKNOWN'
    tasks, task, checker, blocked = wire_review_router(scenario, router, settings, tmp_path, monkeypatch)
    event = {'bucket': 'internal-bucket', 'key': KEY}
    assert task.apply(args=[event]).state == 'RETRY'
    blocked.assert_not_called()
    box['status'] = 'NOT_FOUND'
    create, send = MagicMock(return_value=True), MagicMock(return_value=True)
    monkeypatch.setattr(tasks, 'create_utilisation_task_from_report', create)
    monkeypatch.setattr(tasks, 'sign_and_send_utilisation', send)
    monkeypatch.setattr(tasks, 'get_production_order_data', lambda *args: {'GtinType': 'UNIT'})
    assert task.apply(args=[event]).successful()
    assert checker.call_count == 2
    create.assert_called_once()
    send.assert_called_once()
    blocked.assert_not_called()
    assert not feedback.TerminalLedger(settings['ledger_path']).pending()


def test_all_343_responses_still_leave_46_sscc_unconfirmed(scenario):
    checked = check(scenario)
    facts = checked.feedback_facts
    assert validated_facts(facts) == facts
    assert facts['scan_case'] == 'mixed'
    groups = {i['kind']: i for i in facts['issues']}
    assert groups['repeatedbox']['count'] == 3
    assert {i['id'] for i in groups['repeatedbox']['items']} == {
        '346070517903434491', '346070517903434774', '346070517903434507'}
    assert groups['unconfirmedbox']['count'] == 46
    assert all(i['contents'] == {'EMITTED': 6} for i in groups['unconfirmedbox']['items'])
    assert facts['products_total'] == facts['products_checked'] == 294
    assert len(scenario[1].get_list_cis_info.return_value) == 343
    scenario[2].set_tags.assert_not_called()


@pytest.mark.parametrize('state', [None, '', 'UNKNOWN', 'NEW_STATUS', 'NOT_FOUND'])
def test_only_explicit_not_found_proves_partial_repeat(scenario, state):
    for response in scenario[1].get_list_cis_info.return_value:
        if 'requestedCis' in response:
            response['cisInfo']['status'] = state
    assert check(scenario).feedback_facts['scan_case'] == ('partial_repeat' if state == 'NOT_FOUND' else 'mixed')


def test_conflicting_response_does_not_hide_other_repeat_proof(scenario):
    response = next(r for r in scenario[1].get_list_cis_info.return_value
                    if r.get('cisInfo', {}).get('status') == 'INTRODUCED')
    scenario[1].get_list_cis_info.return_value.append(
        {'cisInfo': dict(response['cisInfo'], status='RETIRED')})
    facts = check(scenario).feedback_facts
    assert facts['scan_case'] == 'mixed'
    assert next(i for i in facts['issues'] if i['kind'] == 'repeatedbox')['count'] == 2


def test_confirmed_violation_stops_once_tags_and_notifies(scenario, router, settings, tmp_path, monkeypatch):
    tasks, task, _ = router
    enable(tasks, settings, tmp_path)
    checker = MagicMock(side_effect=lambda *a: {'path': check(scenario)})
    monkeypatch.setattr(tasks, 'check_aggregation_reports', checker)
    retry = MagicMock(side_effect=Retry())
    monkeypatch.setattr(task, 'retry', retry)
    post = MagicMock(side_effect=AssertionError('No business POST'))
    monkeypatch.setattr(tasks, 'sign_and_send_utilisation', post)
    ledger = feedback.TerminalLedger(settings['ledger_path'])
    for retries in [0, 200, 1200]:
        result = task.apply(args=[{'bucket': 'internal-bucket', 'key': KEY}], retries=retries)
        assert isinstance(result.result, feedback.TerminalRecorded)
    assert checker.call_count == 1
    retry.assert_not_called()
    post.assert_not_called()
    tasks.get_storage.return_value.set_tags.assert_called_with(
        's3://internal-bucket/' + KEY, {'check': 'report_scan_conflict'})
    row = ledger.get(feedback.event_identity('internal-bucket', KEY))
    assert json.loads(row['facts_json'])['scan_case'] == 'mixed'
    factory = MagicMock()
    factory.return_value.find.side_effect = [None, 'mid-test']
    reader = MagicMock(return_value={'operator': 'Alice'})
    assert runtime.drain(settings, {}, read_json=reader, transport_factory=factory) == {'CONFIRMED': 1}
    assert runtime.drain(settings, {}, read_json=reader, transport_factory=factory) == {}
    factory.return_value.post.assert_called_once()
    sent = factory.return_value.post.call_args.args[0]
    assert 'Обработка остановлена до сверки.' in sent['message_text']
    assert 'повторное сканирование' in sent['message_text']
    attachment = json.loads(sent['attachment_json'])
    assert attachment['box_count'] == 49
    for group in EVIDENCE['issue_groups']:
        for item in group['items']:
            assert item['id'] in attachment['text']
    assert '?' not in sent['message_text']


def test_restored_statuses_are_rechecked_then_recorded_once(scenario, router, settings, tmp_path, monkeypatch):
    tasks, task, _ = router
    enable(tasks, settings, tmp_path)
    checker = MagicMock(side_effect=lambda *a: {'path': check(scenario)})
    monkeypatch.setattr(tasks, 'check_aggregation_reports', checker)
    monkeypatch.setattr(task, 'retry', MagicMock(side_effect=Retry()))
    post = MagicMock(side_effect=AssertionError('No business POST'))
    monkeypatch.setattr(tasks, 'sign_and_send_utilisation', post)
    data = {'bucket': 'internal-bucket', 'key': KEY}
    responses = scenario[1].get_list_cis_info.return_value
    complete = copy.deepcopy(responses)
    for response in responses:
        response['cisInfo'].pop('status', None)
    assert task.apply(args=[data]).state == 'RETRY'
    assert not feedback.TerminalLedger(settings['ledger_path']).pending()
    scenario[1].get_list_cis_info.return_value = complete
    for response in scenario[1].get_list_cis_info.return_value:
        if 'requestedCis' in response:
            response['cisInfo']['status'] = 'NOT_FOUND'
    assert isinstance(task.apply(args=[data]).result, feedback.TerminalRecorded)
    assert isinstance(task.apply(args=[data]).result, feedback.TerminalRecorded)
    assert checker.call_count == 2
    rows = feedback.TerminalLedger(settings['ledger_path']).pending()
    assert len(rows) == 1 and rows[0]['category'] == 'report_partial_repeat'
    post.assert_not_called()


def test_recorded_stop_blocks_recheck(scenario, router, settings, tmp_path, monkeypatch):
    tasks, task, _ = router
    enable(tasks, settings, tmp_path)
    ledger = feedback.TerminalLedger(settings['ledger_path'])
    ledger.record('internal-bucket', KEY, 'report-validation', 'report_scan_conflict', facts=check(scenario).feedback_facts)
    logic = MagicMock(side_effect=AssertionError('Terminal fence blocks even a fresh precheck'))
    monkeypatch.setattr(tasks, 'logic_start_equipment_reports', logic)
    assert isinstance(task.apply(args=[{'bucket': 'internal-bucket', 'key': KEY}]).result, feedback.TerminalRecorded)
    logic.assert_not_called()


@pytest.mark.parametrize('backend', ['sqlite', 's3'])
def test_failed_check_write_retries_only_persistence(scenario, router, settings, tmp_path, monkeypatch, backend):
    tasks, task, _ = router
    if backend == 's3':
        store = Store(str(tmp_path / 's3.db'))
        monkeypatch.setattr('xtrek.storage.get_storage', lambda *args: store)
        settings.update(ledger_backend='s3', ledger_path='s3://shared/events')
    enable(tasks, settings, tmp_path)
    checker = MagicMock(side_effect=lambda *a: {'path': check(scenario)})
    monkeypatch.setattr(tasks, 'check_aggregation_reports', checker)
    retry = MagicMock(side_effect=Retry())
    monkeypatch.setattr(task, 'retry', retry)
    tag_store = tasks.get_storage.return_value
    tag_store.set_tags.side_effect = OSError('tag write unavailable')
    post = MagicMock(side_effect=AssertionError('No business POST'))
    monkeypatch.setattr(tasks, 'sign_and_send_utilisation', post)
    data = {'bucket': 'internal-bucket', 'key': KEY}
    assert task.apply(args=[data]).state == 'RETRY'
    intent = retry.call_args.kwargs['kwargs']
    assert intent['terminal_failure'] == 'report_scan_conflict'
    ledger = runtime.open_ledger(settings, tasks.config)
    first = ledger.get(feedback.event_identity('internal-bucket', KEY))
    assert first['category'] == 'report_scan_conflict'
    # Even if the source/evidence changes while S3 is down, do not run it again.
    checker.side_effect = AssertionError('No revalidation after durable stop')
    tag_store.set_tags.side_effect = None
    assert isinstance(task.apply(kwargs=intent).result, feedback.TerminalRecorded)
    assert ledger.get(first['event_id']) == first
    tag_store.set_tags.assert_called_with('s3://internal-bucket/' + KEY, {'check': 'report_scan_conflict'})
    assert checker.call_count == 1
    post.assert_not_called()


@pytest.mark.parametrize('state', ['INTRODUCED', 'RETIRED', 'WRITTEN_OFF', 'DISAGGREGATED', 'NOT_FOUND', 'EMITTED_WITH_PARENT'])
def test_one_confirmed_bad_content_stops_with_all_other_statuses_missing(scenario, router, settings, tmp_path, monkeypatch, state):
    tasks, task, _ = router
    enable(tasks, settings, tmp_path)
    responses = scenario[1].get_list_cis_info.return_value
    for response in responses:
        response['cisInfo'].pop('status', None)
    child = next(r['cisInfo'] for r in responses if r['cisInfo'].get('cis', '').startswith('01'))
    child['status'] = state if state != 'EMITTED_WITH_PARENT' else 'EMITTED'
    if state == 'EMITTED_WITH_PARENT':
        child['parent'] = '00346070517903434491'
    checker = MagicMock(side_effect=lambda *a: {'path': check(scenario)})
    monkeypatch.setattr(tasks, 'check_aggregation_reports', checker)
    post = MagicMock(side_effect=AssertionError('No business POST'))
    monkeypatch.setattr(tasks, 'sign_and_send_utilisation', post)
    assert isinstance(task.apply(args=[{'bucket': 'internal-bucket', 'key': KEY}]).result, feedback.TerminalRecorded)
    tasks.get_storage.return_value.set_tags.assert_called_with('s3://internal-bucket/' + KEY, {'check': 'report_scan_conflict'})
    post.assert_not_called()


def test_duplicate_stops_before_status_query_and_records_exact_namespaced_tag(scenario, router, settings, tmp_path, monkeypatch):
    tasks, task, _ = router
    enable(tasks, settings, tmp_path)
    tasks.config['event_key_prefix'] = 'test'
    source = json.loads(scenario[2].read_text.return_value)
    source['readyBox'][1]['boxNumber'] = source['readyBox'][0]['boxNumber']
    scenario[2].read_text.return_value = json.dumps(source)
    scenario[1].get_list_cis_info.side_effect = AssertionError('Source defect needs no status query')
    monkeypatch.setattr(tasks, 'check_aggregation_reports', lambda *a: {'path': check(scenario)})
    data = {'bucket': 'internal-bucket', 'key': 'test/' + KEY}
    assert isinstance(task.apply(args=[data]).result, feedback.TerminalRecorded)
    tasks.get_storage.return_value.set_tags.assert_called_with('s3://internal-bucket/test/' + KEY, {'check': 'invalid_report'})
    scenario[1].get_list_cis_info.assert_not_called()
    scenario[1].get_aggregated_cis_list.assert_not_called()


def test_only_conflicting_statuses_do_not_create_terminal_stop(scenario, router, settings, tmp_path, monkeypatch):
    tasks, task, _ = router
    enable(tasks, settings, tmp_path)
    original = copy.deepcopy(scenario[1].get_list_cis_info.return_value)
    for row in original:
        if row['cisInfo'].get('status'):
            scenario[1].get_list_cis_info.return_value.append(
                {'cisInfo': dict(row['cisInfo'], status='UNKNOWN')})
    monkeypatch.setattr(tasks, 'check_aggregation_reports', lambda *a: {'path': check(scenario)})
    monkeypatch.setattr(task, 'retry', MagicMock(side_effect=Retry()))
    assert task.apply(args=[{'bucket': 'internal-bucket', 'key': KEY}]).state == 'RETRY'
    assert not feedback.TerminalLedger(settings['ledger_path']).pending()
    tasks.get_storage.return_value.set_tags.assert_not_called()


def test_emitted_contents_with_parent_stop_even_without_legacy_errors(scenario, router, settings, tmp_path, monkeypatch):
    tasks, task, _ = router
    enable(tasks, settings, tmp_path)
    responses = scenario[1].get_list_cis_info.return_value
    for row in responses:
        code = row['cisInfo'].get('cis') or row['requestedCis']
        row['cisInfo']['status'] = 'EMITTED' if code.startswith('01') else 'NOT_FOUND'
    child = next(r['cisInfo'] for r in responses if r['cisInfo'].get('cis', '').startswith('01'))
    child['parent'] = '00346070517903434491'
    checked = check(scenario)
    assert dict(checked) == {}  # Old validation does not check parent here.
    assert checked.feedback_facts['scan_case'] == 'mixed'
    monkeypatch.setattr(tasks, 'check_aggregation_reports', lambda *a: {'path': checked})
    post = MagicMock(side_effect=AssertionError('No business POST'))
    monkeypatch.setattr(tasks, 'sign_and_send_utilisation', post)
    assert isinstance(task.apply(args=[{'bucket': 'internal-bucket', 'key': KEY}]).result, feedback.TerminalRecorded)
    post.assert_not_called()


def test_own_applied_progress_is_not_a_repeat_scan(scenario, monkeypatch):
    for row in scenario[1].get_list_cis_info.return_value:
        code = row['cisInfo'].get('cis') or row['requestedCis']
        row['cisInfo']['status'] = 'APPLIED' if code.startswith('01') else 'NOT_FOUND'
    monkeypatch.setattr('xtrek.equipment_progress.has_report_progress', lambda *a: True)
    checked = check(scenario)
    assert checked.feedback_facts['scan_case'] is None
    assert set(checked) == {'waitingstatus'}
    scenario[1].get_aggregated_cis_list.assert_not_called()


@pytest.mark.parametrize('backend', ['sqlite', 's3'])
def test_identical_rescan_under_new_id_stops_and_notifies_once_per_report(
        scenario, router, settings, tmp_path, monkeypatch, backend):
    """A new report is not a duplicate delivery event for the preceding report."""
    evidence = json.loads((Path(__file__).parent / 'fixtures/pending_scan_t3515_rescan.json').read_text())
    new_id = evidence['id']
    tasks, task, _ = router
    if backend == 's3':
        store = Store(str(tmp_path / 's3.db'))
        monkeypatch.setattr('xtrek.storage.get_storage', lambda *args: store)
        settings.update(ledger_backend='s3', ledger_path='s3://shared/events')
    enable(tasks, settings, tmp_path)
    first_report = json.loads(scenario[2].read_text.return_value)
    positions = {i['id']: i['position'] for g in evidence['issue_groups'] for i in g['items']}
    second_report = {'readyBox': sorted(copy.deepcopy(first_report['readyBox']),
                                        key=lambda box: positions[box['boxNumber']])}
    assert len(positions) == 49
    assert {b['boxNumber']: b['productNumbersFull'] for b in first_report['readyBox']} == {
        b['boxNumber']: b['productNumbersFull'] for b in second_report['readyBox']}
    reports = {RID: first_report, new_id: second_report}
    scenario[2].read_text.side_effect = lambda path: json.dumps(reports[Path(path).stem])
    def analyze_report(ids):
        path = 's3://internal-bucket/equipment-reports/' + ids[0] + '.json'
        return {path: scenario[0].check_report(path, strict_prerequisites=True)}
    checker = MagicMock(side_effect=analyze_report)
    monkeypatch.setattr(tasks, 'check_aggregation_reports', checker)
    retry = MagicMock(side_effect=Retry())
    monkeypatch.setattr(task, 'retry', retry)
    post = MagicMock(side_effect=AssertionError('No business POST'))
    monkeypatch.setattr(tasks, 'sign_and_send_utilisation', post)
    for rid in [RID, new_id, RID, new_id]:
        result = task.apply(args=[{'bucket': 'internal-bucket', 'key': 'equipment-reports/' + rid + '.json'}])
        assert isinstance(result.result, feedback.TerminalRecorded)
    assert checker.call_count == 2
    retry.assert_not_called()
    post.assert_not_called()
    ledger = runtime.open_ledger(settings, tasks.config)
    rows = ledger.pending()
    assert len(rows) == 2 and len({r['event_id'] for r in rows}) == 2
    second = next(r for r in rows if r['source_key'].endswith(new_id + '.json'))
    facts = json.loads(second['facts_json'])
    assert facts['scan_case'] == 'mixed'
    repeated = next(g for g in facts['issues'] if g['kind'] == 'repeatedbox')
    assert repeated['count'] == 3
    assert {i['position'] for i in repeated['items']} == {12, 15, 16}
    assert {call.args[0] for call in tasks.get_storage.return_value.set_tags.call_args_list} == {
        's3://internal-bucket/equipment-reports/' + rid + '.json' for rid in reports}
    delivered = {}
    transport = MagicMock()
    transport.find.side_effect = lambda row: delivered.get(row['event_id'])
    transport.post.side_effect = lambda row: delivered.update({row['event_id']: 'mid-' + row['event_id']})
    factory = MagicMock(return_value=transport)
    read = lambda config, path: {'operator': 'Alice'}
    assert runtime.drain(settings, {}, read_json=read, transport_factory=factory) == {'CONFIRMED': 2}
    assert runtime.drain(settings, {}, read_json=read, transport_factory=factory) == {}
    assert transport.post.call_count == 2
    messages = [call.args[0]['message_text'] for call in transport.post.call_args_list]
    for rid in reports:
        assert sum(rid in message for message in messages) == 1
