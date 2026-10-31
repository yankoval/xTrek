import json
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import MagicMock

from celery.exceptions import Retry
import pytest

from xtrek import terminal_feedback as feedback
from xtrek import terminal_feedback_runtime as runtime
from xtrek.terminal_feedback_max import MaxTerminalTransport, deliver
from xtrek.pallet_label_max_notifier import HistoryIncomplete
from test_equipment_report_prerequisites import router


@pytest.fixture
def settings(tmp_path):
    ledger = feedback.TerminalLedger.initialize(tmp_path / 'terminal.sqlite3')
    return {'version': 1, 'enabled': True, 'ledger_path': ledger.path,
            'operator_routes': {'Alice': 'line-a', 'Bob': 'line-b', 'Shared': '$default'},
            'default_route': 'line-a', 'production_order_overrides': {}, 'protected_ids': [],
            'routes': {
                'line-a': {'transport': 'max', 'chat_id': 'chat-a', 'bot_user_id': 'bot', 'token_env': 'BOT_TOKEN'},
                'line-b': {'transport': 'max', 'chat_id': 'chat-b', 'bot_user_id': 'bot', 'token_env': 'BOT_TOKEN'}}}


@pytest.fixture
def ledger(settings):
    return feedback.TerminalLedger(settings['ledger_path'])


def ready(ledger, settings, category='invalid_report', key='equipment-reports/order.json'):
    row = ledger.record('internal-bucket', key, feedback.source_stage(key), category)
    return ledger.bind_route(row['event_id'], 'order', 'Alice', feedback.select_route(settings, 'Alice'))


def enable(tasks, settings, tmp_path):
    path = tmp_path / 'settings.json'
    path.write_text(json.dumps(settings))
    tasks.config['terminal_feedback_config'] = str(path)


@pytest.mark.parametrize('operator,chat', [
    ('Alice', 'chat-a'), ('Bob', 'chat-b'), ('Shared', 'chat-a'), ('alice', 'chat-a'),
    (' Alice', 'chat-a'), ('Alice ', 'chat-a'), ('unknown', 'chat-a'), ('', 'chat-a'), ('  ', 'chat-a'),
    (None, 'chat-a'), (42, 'chat-a'), ('Alicе', 'chat-a'),  # last letter is Cyrillic
])
def test_exact_routing(settings, operator, chat):
    route = feedback.select_route(settings, operator)
    assert (route['chat_id'] if route else None) == chat


def test_invalid_default_is_an_explicit_error_and_known_route_still_works(settings):
    settings['default_route'] = None
    for operator in ('Shared', 'unknown', None):
        with pytest.raises(feedback.RouteConfigurationError, match='default route'):
            feedback.select_route(settings, operator)
    assert feedback.select_route(settings, 'Bob')['chat_id'] == 'chat-b'
    settings['routes']['line-a']['transport'] = 'unconfigured'
    with pytest.raises(feedback.RouteConfigurationError, match='operator route'):
        feedback.select_route(settings, 'Alice')


@pytest.mark.parametrize('category', feedback.ACTIONS)
def test_durable_category_identity_and_redaction(ledger, settings, category):
    row = ready(ledger, settings, category)
    same = ledger.record('internal-bucket', 'equipment-reports/order.json', 'report-validation', 'retry_exhausted')
    assert same['event_id'] == row['event_id']
    assert same['category'] == category
    assert same['terminal_state'] == 'RECONCILIATION_REQUIRED'
    reopened = feedback.TerminalLedger(ledger.path)
    assert len(reopened.pending()) == 1
    assert reopened.get(row['event_id']) == row
    sensitive = 'TOKEN-secret\n010000000000000021PRIVATE\x1d93PRIVATE'
    other = ledger.record('internal-bucket', 'equipment-reports/' + sensitive + '.json', 'report-validation', category)
    other = ledger.bind_route(other['event_id'], sensitive, sensitive, settings['routes']['line-a'])
    assert sensitive not in other['message_text']
    assert 'TOKEN-secret' not in other['message_text']
    assert 'PRIVATE' not in other['message_text']
    assert json.loads(other['operator_raw']) == sensitive
    from xtrek.terminal_feedback_messages import TEMPLATES
    assert TEMPLATES[category][0] in other['message_text']
    assert other['event_id'][:16] in other['message_text']
    assert 'Артикул:' in other['message_text'] and 'Партия:' in other['message_text']
    assert 'ID отчёта:' in other['message_text']


def test_frozen_destination_and_message(ledger, settings):
    row = ready(ledger, settings)
    rebound = ledger.bind_route(row['event_id'], 'different', 'Bob', settings['routes']['line-b'])
    assert rebound == row


def test_empty_report_message_explains_cause_and_rescan(ledger, settings):
    row = ready(ledger, settings, category='empty_report')
    assert 'Отчёт пуст или' in row['message_text']
    assert 'Повторно выгрузите задание' in row['message_text']
    assert 'отсканируйте продукцию заново' in row['message_text']


def test_missing_ledger_never_recreated(tmp_path, ledger):
    from pathlib import Path
    Path(ledger.path).unlink()
    with pytest.raises(Exception):
        ledger.pending()
    assert not Path(ledger.path).exists()
    with pytest.raises(RuntimeError):
        feedback.TerminalLedger(tmp_path / 'absent')


def test_concurrent_claim_is_single_winner(ledger, settings):
    row = ready(ledger, settings)
    with ThreadPoolExecutor(max_workers=8) as pool:
        claims = list(pool.map(lambda _: feedback.TerminalLedger(ledger.path).claim_post(row['event_id']), range(8)))
    assert sum(claims) == 1


@pytest.mark.parametrize('outcome', ['timeout', 'http-error', 'accepted-not-visible', 'crash-before-post'])
def test_ambiguous_post_never_repeated_after_restart(ledger, settings, outcome):
    row = ready(ledger, settings)
    transport = MagicMock()
    transport.find.return_value = None
    if outcome in {'timeout', 'http-error'}:
        transport.post.side_effect = RuntimeError('SECRET response')
    if outcome == 'crash-before-post':
        assert ledger.claim_post(row['event_id'])
    assert deliver(ledger, row, transport) == 'GET_RECONCILIATION_REQUIRED'
    reopened = feedback.TerminalLedger(ledger.path)
    assert deliver(reopened, row, transport) == 'GET_RECONCILIATION_REQUIRED'
    assert transport.post.call_count == (0 if outcome == 'crash-before-post' else 1)
    transport.find.return_value = 'get-confirmed-mid'
    assert deliver(reopened, row, transport) == 'CONFIRMED'
    assert reopened.get(row['event_id'])['message_id'] == 'get-confirmed-mid'
    transport.reset_mock()
    assert deliver(reopened, row, transport) == 'CONFIRMED'
    transport.find.assert_not_called()
    transport.post.assert_not_called()


def test_get_failure_before_post_is_retryable(ledger, settings):
    row = ready(ledger, settings)
    transport = MagicMock()
    transport.find.side_effect = HistoryIncomplete('incomplete')
    with pytest.raises(HistoryIncomplete):
        deliver(ledger, row, transport)
    assert ledger.get(row['event_id'])['delivery_state'] == 'READY'
    transport.post.assert_not_called()


def test_existing_get_match_prevents_post(ledger, settings):
    row = ready(ledger, settings)
    transport = MagicMock()
    transport.find.return_value = 'mid'
    assert deliver(ledger, row, transport) == 'CONFIRMED'
    transport.post.assert_not_called()


@pytest.mark.parametrize('wrong', ['sender', 'chat', 'text', 'mid', None])
def test_max_get_requires_exact_body_bot_chat_and_id(ledger, settings, wrong):
    row = ready(ledger, settings)
    message = {'sender': {'user_id': 'bot'}, 'recipient': {'chat_id': 'chat-a'},
               'body': {'text': row['message_text'], 'mid': 'mid'},
               'timestamp': int(row['created_at'] * 1000)}
    if wrong == 'sender':
        message['sender']['user_id'] = 'stranger'
    if wrong == 'chat':
        message['recipient']['chat_id'] = 'chat-b'
    if wrong == 'text':
        message['body']['text'] += ' changed'
    if wrong == 'mid':
        message['body'].pop('mid')
    session = MagicMock()
    response_me, response_history = MagicMock(), MagicMock()
    response_me.json.return_value = {'user_id': 'bot'}
    response_history.json.return_value = {'messages': [message]}
    session.get.side_effect = [response_me, response_history]
    transport = MaxTerminalTransport(session=session, token='SECRET', route=settings['routes']['line-a'],
                                     created_at=row['created_at'])
    assert transport.find(row) == ('mid' if wrong is None else None)
    session.post.assert_not_called()


@pytest.mark.parametrize('operator', ['Alice', 'Bob', 'Shared', None, '', 'Unknown'])
def test_drain_routes_or_holds_and_preserves_raw_operator(ledger, settings, operator):
    row = ledger.record('internal-bucket', 'equipment-reports/order.json', 'report-validation', 'invalid_report')
    read = MagicMock(return_value={'operator': operator})
    factory = MagicMock()
    factory.return_value.find.return_value = 'mid'
    runtime.drain(settings, {}, read_json=read, transport_factory=factory)
    saved = ledger.get(row['event_id'])
    assert json.loads(saved['operator_raw']) == operator
    assert saved['delivery_state'] == 'CONFIRMED'
    assert factory.call_args.args[0]['chat_id'] == ('chat-b' if operator == 'Bob' else 'chat-a')


def test_s3_failure_is_durable_and_retries_only_delivery(ledger, settings):
    row = ledger.record('internal-bucket', 'equipment-reports/order.json', 'report-validation', 'invalid_report')
    read = MagicMock(side_effect=TimeoutError('S3 unavailable SECRET'))
    factory = MagicMock()
    assert runtime.drain(settings, {}, read_json=read, transport_factory=factory) == {'HELD_OR_RETRYABLE': 1}
    assert ledger.get(row['event_id'])['delivery_state'] == 'ROUTE_PENDING'
    factory.assert_not_called()


def test_cis_legacy_requires_exact_mapping(ledger, settings):
    row = ledger.record('internal-bucket', 'cisInformationChangeReceipts/order-production-date.json',
                        'cis-status', 'cis_rejected')
    read = MagicMock(return_value={'operator': 'Alice'})
    with pytest.raises(ValueError):
        runtime.resolve_order(row, settings, {}, read)
    read.assert_not_called()
    settings['production_order_overrides'][row['source_key']] = 'order'
    assert runtime.resolve_order(row, settings, {}, read) == ('order', 'Alice')
    assert read.call_args.args[1] == 's3://internal-bucket/productionOrders/order.json'


def test_receipt_uses_explicit_production_order_not_filename(ledger, settings):
    row = ledger.record('internal-bucket', 'test/aggReceipts/document.json', 'aggregation-status', 'aggregation_rejected')
    read = MagicMock(side_effect=[{'productionOrderId': 'actual-order'}, {'operator': 'Bob'}])
    assert runtime.resolve_order(row, settings, {'event_key_prefix': 'test'}, read) == ('actual-order', 'Bob')
    assert read.call_args.args[1] == 's3://internal-bucket/test/productionOrders/actual-order.json'


@pytest.mark.parametrize('case,folder,logic,category', [
    ('cis', 'cisInformationChangeReceipts', 'logic_update_cis_information_change', 'cis_rejected'),
    ('aggregation', 'aggReceipts', 'logic_update_agg', 'aggregation_rejected'),
    ('aggregation', 'aggSetReceipts', 'logic_update_agg_set', 'aggregation_rejected'),
    ('invalid', 'equipment-reports', 'logic_start_equipment_reports', 'invalid_report'),
    ('reconciliation', 'equipment-reports', 'logic_start_equipment_reports', 'reconciliation_required'),
])
def test_real_celery_terminal_capture_restart_and_no_business_replay(
        router, monkeypatch, settings, ledger, tmp_path, case, folder, logic, category):
    from xtrek.utils import EquipmentReportValidationError
    tasks, task, _ = router
    enable(tasks, settings, tmp_path)
    error = {'cis': tasks.CisInformationChangeRejected('SECRET'),
             'aggregation': tasks.AggregationDocumentRejected('SECRET'),
             'invalid': EquipmentReportValidationError('SECRET'),
             'exhausted': TimeoutError('SECRET'),
             'reconciliation': feedback.ReconciliationRequired('SECRET')}[case]
    business = MagicMock(side_effect=error)
    monkeypatch.setattr(tasks, logic, business)
    retry = MagicMock(side_effect=Retry())
    monkeypatch.setattr(task, 'retry', retry)
    data = {'bucket': 'internal-bucket', 'key': folder + '/order.json'}
    first = task.apply(args=[data], retries=200 if case == 'exhausted' else 0)
    assert not first.successful()
    assert isinstance(first.result, feedback.TerminalRecorded)
    assert 'SECRET' not in str(first.result)
    assert ledger.pending()[0]['category'] == category
    second = task.apply(args=[data])
    assert isinstance(second.result, feedback.TerminalRecorded)
    business.assert_called_once()
    retry.assert_not_called()
    assert len(ledger.pending()) == 1


@pytest.mark.parametrize('error', [RuntimeError('IN_PROGRESS'), RuntimeError('404 after accepted'),
                                   TimeoutError('network'), RuntimeError('S3'), RuntimeError('registry')])
@pytest.mark.parametrize('retries', [0, 199, 200, 1200])
def test_retryable_failures_do_not_create_terminal_event(router, monkeypatch, settings, ledger, tmp_path, error, retries):
    tasks, task, _ = router
    enable(tasks, settings, tmp_path)
    monkeypatch.setattr(tasks, 'logic_start_equipment_reports', MagicMock(side_effect=error))
    retry = MagicMock(side_effect=Retry())
    monkeypatch.setattr(task, 'retry', retry)
    result = task.apply(args=[{'bucket': 'internal-bucket', 'key': 'equipment-reports/order.json'}], retries=retries)
    assert result.state == 'RETRY'
    retry.assert_called_once()
    assert not ledger.pending()


def test_persistence_failure_retry_contains_only_terminal_intent_and_does_not_reenter_business(
        router, monkeypatch, settings, ledger, tmp_path):
    tasks, task, _ = router
    enable(tasks, settings, tmp_path)
    business = MagicMock(side_effect=tasks.CisInformationChangeRejected('SECRET'))
    monkeypatch.setattr(tasks, 'logic_update_cis_information_change', business)
    original = feedback.TerminalLedger.record
    monkeypatch.setattr(feedback.TerminalLedger, 'record', MagicMock(side_effect=OSError('disk full SECRET')))
    retry = MagicMock(side_effect=Retry())
    monkeypatch.setattr(task, 'retry', retry)
    data = {'bucket': 'internal-bucket', 'key': 'cisInformationChangeReceipts/order.json'}
    assert task.apply(args=[data], retries=200).state == 'RETRY'
    queued = retry.call_args.kwargs
    assert queued['max_retries'] is None
    assert queued['args'] == ()
    assert queued['kwargs'] == {'data': data, 'terminal_failure': 'cis_rejected'}
    assert 'SECRET' not in str(queued)
    monkeypatch.setattr(feedback.TerminalLedger, 'record', original)
    assert isinstance(task.apply(kwargs=queued['kwargs'], retries=201).result, feedback.TerminalRecorded)
    business.assert_called_once()
    assert len(ledger.pending()) == 1


def test_config_prefix_stays_in_stable_event_key(router, monkeypatch, settings, ledger, tmp_path):
    tasks, task, _ = router
    enable(tasks, settings, tmp_path)
    tasks.config['event_key_prefix'] = 'isolated'
    monkeypatch.setattr(tasks, 'logic_start_equipment_reports', MagicMock(side_effect=feedback.TerminalBusinessError()))
    data = {'bucket': 'internal-bucket', 'key': 'isolated/equipment-reports/order.json'}
    task.apply(args=[data])
    assert ledger.pending()[0]['event_id'] == feedback.event_identity(data['bucket'], data['key'])


def test_protected_task_does_not_create_feedback(settings, tmp_path):
    path = tmp_path / 'settings.json'
    path.write_text(json.dumps(settings))
    config = {'terminal_feedback_config': str(path), 'internal_bucket': 'bucket'}
    assert runtime.worker_ledger(config, 'bucket', 'equipment-reports/T-' + next(iter(runtime.PROTECTED_IDS)) + '.json') is None


def test_unknown_operator_snapshot_is_immutable_after_default_repaired(ledger, settings):
    row = ledger.record('internal-bucket', 'equipment-reports/order.json', 'report-validation', 'invalid_report')
    settings['default_route'] = None
    read = MagicMock(return_value={'operator': 'Unknown'})
    factory = MagicMock()
    runtime.drain(settings, {}, read_json=read, transport_factory=factory)
    read.side_effect = AssertionError('Must not silently pick a new operator')
    settings['default_route'] = 'line-b'
    factory.return_value.find.return_value = 'mid'
    runtime.drain(settings, {}, read_json=read, transport_factory=factory)
    row = ledger.get(row['event_id'])
    assert json.loads(row['operator_raw']) == 'Unknown'
    assert json.loads(row['route_json'])['chat_id'] == 'chat-b'


@pytest.mark.parametrize('payload', [{}, {'messages': None}, {'messages': [{}]}, {'messages': 'invalid'}])
def test_malformed_max_history_never_grants_post(ledger, settings, payload):
    row = ready(ledger, settings)
    session = MagicMock()
    me, history = MagicMock(), MagicMock()
    me.json.return_value = {'user_id': 'bot'}
    history.json.return_value = payload
    session.get.side_effect = [me, history]
    transport = MaxTerminalTransport(session=session, token='SECRET', route=settings['routes']['line-a'],
                                     created_at=row['created_at'])
    with pytest.raises(HistoryIncomplete):
        deliver(ledger, row, transport)
    session.post.assert_not_called()
    assert ledger.get(row['event_id'])['delivery_state'] == 'READY'


def test_guard_and_existing_event_outbox_are_unchanged_on_terminal_failure(
        router, monkeypatch, settings, ledger, tmp_path):
    from xtrek import operation_state
    from xtrek.s3_poller import EventLedger
    from token_lock_store import MemoryLockStore
    from xtrek.utils import EquipmentReportValidationError
    tasks, task, _ = router
    enable(tasks, settings, tmp_path)
    store = MemoryLockStore()
    tasks.config['operation_state_path'] = 's3://isolated/guards'
    monkeypatch.setattr(operation_state, 'get_storage', lambda *args: store)
    monkeypatch.setattr(tasks, 'check_aggregation_reports', MagicMock(side_effect=EquipmentReportValidationError()))
    send = MagicMock()
    monkeypatch.setattr(tasks, 'sign_and_send_utilisation', send)
    data = {'bucket': 'internal-bucket', 'key': 'equipment-reports/order.json'}
    task.apply(args=[data])
    guard_before = store.lock_object
    assert json.loads(guard_before[0])['phase'] == 'ready'
    assert isinstance(task.apply(args=[data]).result, feedback.TerminalRecorded)
    assert store.lock_object == guard_before
    send.assert_not_called()
    assert len(ledger.pending()) == 1


def test_complete_historical_guard_is_authoritative_with_feedback_enabled(
        router, monkeypatch, settings, ledger, tmp_path):
    from xtrek import operation_state
    from token_lock_store import MemoryLockStore
    tasks, task, _ = router
    enable(tasks, settings, tmp_path)
    store = MemoryLockStore()
    root = 's3://isolated/guards'
    tasks.config['operation_state_path'] = root
    monkeypatch.setattr(operation_state, 'get_storage', lambda *args: store)
    guard = operation_state.OperationState(store, root, 'route-logic_start_equipment_reports',
                                           'internal-bucket/equipment-reports/order.json')
    guard.acquire()
    guard.finish('historical cached result')
    before = store.lock_object
    business = MagicMock()
    monkeypatch.setattr(tasks, 'check_aggregation_reports', business)
    assert task.apply(args=[{'bucket': 'internal-bucket', 'key': 'equipment-reports/order.json'}]).successful()
    assert store.lock_object == before
    assert not ledger.pending()
    business.assert_not_called()


@pytest.mark.parametrize('route,update', [('logic_update_disaggregation', 'update_disaggregation_status'),
                                        ('logic_update_reaggregation', 'update_reaggregation_status'),
                                        ('logic_update_introduce', 'update_introduce_status')])
@pytest.mark.parametrize('payload,terminal', [({'status': 'CHECKED_NOT_OK'}, True),
    ([{'status': 'CHECKED_NOT_OK'}], True),
    ([{'status': 'CHECKED_NOT_OK'}, {'status': 'CHECKED_OK'}], False),
    ({'status': 'CHECKED_NOT_OK', 'error': '404'}, False),
    ([{'status': 'CHECKED_NOT_OK', 'error': '404'}], False)])
def test_only_definitive_document_rejection_is_terminal(router, monkeypatch, route, update, payload, terminal):
    tasks, _, _ = router
    monkeypatch.setattr(tasks, update, MagicMock(return_value=payload))
    with pytest.raises(Exception) as exc:
        getattr(tasks, route)('internal-bucket/receipts/order.json')
    assert isinstance(exc.value, feedback.TerminalBusinessError) is terminal


def test_failed_retry_publish_requeues_original_delivery(router, monkeypatch, settings, tmp_path):
    from celery.exceptions import Reject
    tasks, task, _ = router
    enable(tasks, settings, tmp_path)
    monkeypatch.setattr(tasks, 'logic_start_equipment_reports', MagicMock(side_effect=feedback.TerminalBusinessError()))
    monkeypatch.setattr(feedback.TerminalLedger, 'record', MagicMock(side_effect=OSError()))
    retry = MagicMock(side_effect=RuntimeError('broker unavailable SECRET'))
    monkeypatch.setattr(task, 'retry', retry)
    task.push_request(retries=200)
    try:
        with pytest.raises(Reject) as rejected:
            task.run({'bucket': 'internal-bucket', 'key': 'equipment-reports/order.json'})
        assert rejected.value.requeue is True
        assert 'SECRET' not in str(rejected.value)
        retry.assert_called_once()
    finally:
        task.pop_request()


def test_event_outbox_not_republished_or_completed_with_feedback(
        router, monkeypatch, settings, ledger, tmp_path):
    from xtrek.s3_poller import EventLedger, publish_pending
    tasks, task, _ = router
    enable(tasks, settings, tmp_path)
    monkeypatch.setattr(tasks, 'logic_start_equipment_reports', MagicMock(side_effect=feedback.TerminalBusinessError()))
    data = {'bucket': 'internal-bucket', 'key': 'equipment-reports/order.json'}
    path = str(tmp_path / 'outbox.sqlite3')
    outbox = EventLedger(path)
    assert outbox.enqueue('original-event', data['bucket'], data['key'])
    outbox.claim()
    outbox.published('original-event')
    monkeypatch.setattr(tasks, 'LOCAL_POLLING', True)
    monkeypatch.setenv('XTREK_S3_POLL_LEDGER', path)
    # The router fixture mocks lifecycle recording; connect the actual ledger call.
    original = outbox.task_state
    monkeypatch.setattr(tasks, '_record_local_event_state', original)
    assert not task.apply(args=[data], task_id='original-event').successful()
    outbox.close()
    reopened = EventLedger(path)
    assert reopened.db.execute('SELECT state FROM s3_outbox').fetchone()[0] == 'published'
    assert not reopened.enqueue('original-event', data['bucket'], data['key'])
    sender = MagicMock()
    assert publish_pending(reopened, sender) == 0
    sender.send_task.assert_not_called()
    assert len(ledger.pending()) == 1
    reopened.close()


def test_route_binding_preserves_first_audit_snapshot_even_in_race(ledger, settings):
    row = ledger.record('internal-bucket', 'equipment-reports/order.json', 'report-validation', 'invalid_report')
    held = ledger.bind_route(row['event_id'], 'order', 'Unknown', None)
    competing = ledger.bind_route(row['event_id'], 'order', 'Bob', settings['routes']['line-b'])
    assert competing == held


def test_persistence_retry_can_exceed_business_limit_using_real_celery(router, monkeypatch):
    tasks, task, _ = router
    task.push_request(retries=1000, called_directly=False, is_eager=False)
    publish = MagicMock()
    monkeypatch.setattr(task, 'apply_async', publish)
    try:
        with pytest.raises(Retry):
            tasks._retry_terminal_persistence(task, {'bucket': 'b', 'key': 'k'}, 'invalid_report')
        publish.assert_called_once()
        assert publish.call_args.kwargs['retries'] == 1001
        assert publish.call_args.args[1]['terminal_failure'] == 'invalid_report'
    finally:
        task.pop_request()


def test_terminal_transport_fetches_fresh_history_after_post_without_pallet_ledger(ledger, settings):
    row = ready(ledger, settings)
    session = MagicMock()
    posted = []
    history_reads = []

    def get(url, **kwargs):
        response = MagicMock(status_code=200)
        if url.endswith('/me'):
            response.json.return_value = {'user_id': 'bot'}
        else:
            assert url.endswith('/messages')
            history_reads.append(kwargs['params'])
            response.json.return_value = {'messages': list(posted)}
        return response

    def post(url, **kwargs):
        assert url.endswith('/messages')
        posted.append({'timestamp': int(row['created_at'] * 1000),
                       'sender': {'user_id': 'bot'}, 'recipient': {'chat_id': 'chat-a'},
                       'body': {'mid': 'terminal-mid', 'text': kwargs['json']['text']}})
        return MagicMock(status_code=200)

    session.get.side_effect = get
    session.post.side_effect = post
    transport = MaxTerminalTransport(session=session, token='fixture',
                                     route=settings['routes']['line-a'], created_at=row['created_at'])
    assert deliver(ledger, row, transport) == 'CONFIRMED'
    assert len(history_reads) == 2
    session.post.assert_called_once()
    assert deliver(ledger, row, transport) == 'CONFIRMED'
    session.post.assert_called_once()
