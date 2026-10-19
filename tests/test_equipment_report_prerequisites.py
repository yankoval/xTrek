import json
import sys
from unittest.mock import MagicMock

import celery
import celery.signals
import pytest

from xtrek import utils
import test_tasks_unit_set_flow as helpers


@pytest.fixture
def prerequisites(monkeypatch):
    config = {"s3_config": {}, "tokens_registry_path": "s3://bucket/registry-v2.json"}
    storage = MagicMock()
    storage.exists.return_value = True
    monkeypatch.setattr(utils, "get_storage", MagicMock(return_value=storage))
    token = MagicMock(side_effect=AssertionError("must not select a token for invalid input"))
    api = MagicMock(side_effect=AssertionError("must not construct an API client"))
    nk = MagicMock(side_effect=AssertionError("must not construct an NK client"))
    monkeypatch.setattr(utils, "TokenProcessor", token)
    monkeypatch.setattr(utils, "HonestSignAPI", api)
    monkeypatch.setattr(utils, "NK", nk)
    return config, storage, token, api, nk


@pytest.mark.parametrize("report", [
    {}, {"readyBox": []}, {"readyBox": "invalid"}, {"readyBox": ["invalid"]},
    {"readyBox": [{}]}, {"readyBox": [{"productNumbersFull": []}]},
    {"readyBox": [{"productNumbersFull": "private-code"}]},
    {"readyBox": [{"productNumbersFull": [""]}]},
    {"readyBox": [{"productNumbersFull": ["   "]}]},
    {"readyBox": [{"productNumbersFull": [None]}]},
    {"schemaVersion": 2, "readyPallet": []},
    {"schemaVersion": 2, "readyPallet": [{"readyBox": []}]},
    {"schemaVersion": 2, "readyPallet": [
        {"readyBox": []}, {"readyBox": [{"productNumbersFull": ["code"]}]}]},
    {"schemaVersion": "private-code", "readyBox": []},
])
def test_invalid_report_blocks_before_tokens_or_clients(prerequisites, report):
    config, storage, token, api, nk = prerequisites
    storage.read_text.return_value = json.dumps(report)
    with pytest.raises(utils.EquipmentReportValidationError) as error:
        utils.check_aggregation_reports(["report.json"], config=config)
    assert "private-code" not in str(error.value)
    assert isinstance(error.value, utils.ReconciliationRequired)
    token.assert_not_called()
    api.assert_not_called()
    nk.assert_not_called()
    storage.set_tags.assert_not_called()
    storage.upload.assert_not_called()


def test_invalid_json_is_terminal_before_resources(prerequisites):
    config, storage, token, api, nk = prerequisites
    storage.read_text.return_value = "not JSON"
    with pytest.raises(utils.EquipmentReportValidationError, match="not valid JSON"):
        utils.check_aggregation_reports(["report.json"], config=config)
    token.assert_not_called()
    api.assert_not_called()
    nk.assert_not_called()


def test_explicit_clients_do_not_bypass_empty_report_validation(prerequisites):
    config, storage, *_ = prerequisites
    storage.read_text.return_value = '{"readyBox": []}'
    with pytest.raises(utils.EquipmentReportValidationError):
        utils.check_aggregation_reports(["report.json"], api=MagicMock(), nk=MagicMock(),
                                        config=config)


@pytest.mark.parametrize("report", [
    {}, {"readyBox": []}, {"readyBox": [{"productNumbersFull": []}]},
    {"schemaVersion": 2, "readyPallet": [{"readyBox": []}]},
])
def test_empty_report_has_specific_terminal_category(prerequisites, report):
    config, storage, *_ = prerequisites
    storage.read_text.return_value = json.dumps(report)
    with pytest.raises(utils.EquipmentReportValidationError) as error:
        utils.check_aggregation_reports(["report.json"], config=config)
    assert error.value.category == "empty_report"
    assert isinstance(error.value, utils.TerminalBusinessError)


@pytest.mark.parametrize("version", [1, 2])
def test_valid_report_keeps_analyzer_flow(prerequisites, monkeypatch, version):
    config, storage, token, *_ = prerequisites
    boxes = [{"boxNumber": "box", "productNumbersFull": ["code"]}]
    report = {"readyBox": boxes} if version == 1 else {
        "schemaVersion": 2, "readyPallet": [{"readyBox": boxes}]}
    storage.read_text.return_value = json.dumps(report)
    analyzer = MagicMock()
    analyzer.check_report.return_value = None
    constructor = MagicMock(return_value=analyzer)
    monkeypatch.setattr(utils, "AggregationAnalyzer", constructor)
    api, nk = MagicMock(), MagicMock()
    assert utils.check_aggregation_reports(["report.json"], api=api, nk=nk,
                                           config=config) == {"report.json": None}
    constructor.assert_called_once_with(api, nk, config)
    analyzer.check_report.assert_called_once_with("report.json", strict_prerequisites=True)
    token.assert_not_called()


@pytest.mark.parametrize("failure", ["missing", "s3", "registry", "inn"])
def test_unverified_resources_propagate_retryable_failure(prerequisites, monkeypatch, failure):
    config, storage, _, api, nk = prerequisites
    storage.read_text.return_value = json.dumps({"readyBox": [
        {"productNumbersFull": ["010461011765072021FAKE"]}]})
    if failure == "missing":
        storage.exists.return_value = False
    elif failure == "s3":
        storage.read_text.side_effect = TimeoutError("S3 timeout")
    elif failure == "registry":
        monkeypatch.setattr(utils, "get_inn_by_gtin", lambda gtin: "7733154124")
        monkeypatch.setattr(utils, "TokenProcessor",
                            MagicMock(side_effect=RuntimeError("registry unavailable")))
    else:
        monkeypatch.setattr(utils, "get_inn_by_gtin", lambda gtin: None)
    with pytest.raises(utils.EquipmentReportResourceError):
        utils.check_aggregation_reports(["report.json"], config=config)
    api.assert_not_called()
    nk.assert_not_called()


@pytest.fixture
def router(monkeypatch):
    options = {}
    real_module, real_signals = celery, celery.signals
    original_task = helpers._FakeCelery.task

    def capture_options(self, *args, **kwargs):
        options[kwargs.get("name")] = kwargs
        return original_task(self, *args, **kwargs)

    monkeypatch.setattr(helpers._FakeCelery, "task", capture_options)
    tasks = helpers.import_tasks(monkeypatch)
    monkeypatch.setitem(sys.modules, "celery", real_module)
    monkeypatch.setitem(sys.modules, "celery.signals", real_signals)
    app = celery.Celery("equipment-prerequisite-tests", broker="memory://", fixups=[])
    task = app.task(shared=False, **options["tasks.process_s3_event"])(
        tasks.process_s3_event._func)
    record = MagicMock()
    monkeypatch.setattr(tasks, "_record_local_event_state", record)
    real_signals.task_postrun.connect(tasks._finish_token_snapshot, weak=False)
    try:
        yield tasks, task, record
    finally:
        real_signals.task_postrun.disconnect(tasks._finish_token_snapshot)
        app.close()


@pytest.mark.parametrize("terminal", [False, True])
def test_router_does_not_cache_prerequisite_error_as_success(router, monkeypatch, terminal):
    tasks, task, record = router
    error = (utils.EquipmentReportValidationError("empty report") if terminal else
             utils.EquipmentReportResourceError("unverified resources"))
    monkeypatch.setattr(tasks, "check_aggregation_reports", MagicMock(side_effect=error))
    create, send = MagicMock(), MagicMock()
    monkeypatch.setattr(tasks, "create_utilisation_task_from_report", create)
    monkeypatch.setattr(tasks, "sign_and_send_utilisation", send)
    retry = MagicMock(side_effect=RuntimeError("retry scheduled"))
    monkeypatch.setattr(task, "retry", retry)
    result = task.apply(args=[{"bucket": "internal-bucket",
                              "key": "equipment-reports/existing.json"}])
    assert not result.successful()
    if terminal:
        assert isinstance(result.result, utils.EquipmentReportValidationError)
        retry.assert_not_called()
    else:
        retry.assert_called_once()
    record.assert_called_once_with(result.id, "failed")
    create.assert_not_called()
    send.assert_not_called()


@pytest.mark.parametrize("terminal", [False, True])
def test_new_route_guard_does_not_complete_on_prerequisite_failure(router, monkeypatch, terminal):
    from token_lock_store import MemoryLockStore
    from xtrek import operation_state

    tasks, task, _ = router
    store = MemoryLockStore()
    tasks.config["operation_state_path"] = "s3://isolated/guards"
    monkeypatch.setattr(operation_state, "get_storage", lambda *args: store)
    error = (utils.EquipmentReportValidationError("empty report") if terminal else
             utils.EquipmentReportResourceError("unverified resources"))
    monkeypatch.setattr(tasks, "check_aggregation_reports", MagicMock(side_effect=error))
    monkeypatch.setattr(task, "retry", MagicMock(side_effect=RuntimeError("retry scheduled")))
    result = task.apply(args=[{"bucket": "internal-bucket",
                              "key": "equipment-reports/existing.json"}])
    assert not result.successful()
    guard = json.loads(store.lock_object[0])
    assert guard["phase"] == "ready"
    assert "result" not in guard


def test_existing_complete_guard_is_not_reset_or_reprocessed(router, monkeypatch):
    from token_lock_store import MemoryLockStore
    from xtrek import operation_state

    tasks, task, _ = router
    root = "s3://isolated/guards"
    store = MemoryLockStore()
    tasks.config["operation_state_path"] = root
    monkeypatch.setattr(operation_state, "get_storage", lambda *args: store)
    guard = operation_state.OperationState(store, root, "route-logic_start_equipment_reports",
                                           "internal-bucket/equipment-reports/existing.json")
    guard.acquire()
    guard.finish("historical cached result")
    before = store.lock_object
    check, create, send = MagicMock(), MagicMock(), MagicMock()
    monkeypatch.setattr(tasks, "check_aggregation_reports", check)
    monkeypatch.setattr(tasks, "create_utilisation_task_from_report", create)
    monkeypatch.setattr(tasks, "sign_and_send_utilisation", send)
    assert task.apply(args=[{"bucket": "internal-bucket",
                             "key": "equipment-reports/existing.json"}]).successful()
    assert store.lock_object == before
    check.assert_not_called()
    create.assert_not_called()
    send.assert_not_called()


@pytest.mark.parametrize("replacement", ["empty", "s3_failure", "invalid_json"])
def test_analyzer_second_read_failure_cannot_be_a_successful_skip(prerequisites, replacement):
    config, storage, *_ = prerequisites
    valid = json.dumps({"readyBox": [{"productNumbersFull": ["code"]}]})
    next_read = ({"empty": '{"readyBox": []}', "s3_failure": TimeoutError("S3 timeout"),
                  "invalid_json": "not JSON"})[replacement]
    storage.read_text.side_effect = [valid, next_read]
    api = MagicMock()
    expected = (utils.EquipmentReportResourceError if replacement == "s3_failure"
                else utils.EquipmentReportValidationError)
    with pytest.raises(expected):
        utils.check_aggregation_reports(["report.json"], api=api, nk=MagicMock(), config=config)
    api.get_list_cis_info.assert_not_called()
    storage.set_tags.assert_not_called()


def test_check_tag_write_failure_is_retryable(prerequisites):
    config, storage, *_ = prerequisites
    report = {"readyBox": [{"boxNumber": "box", "productNumbersFull": ["code"]}]}
    storage.read_text.return_value = json.dumps(report)
    storage.set_tags.side_effect = TimeoutError("S3 tag timeout")
    api = MagicMock()
    api.get_list_cis_info.return_value = []
    with pytest.raises(utils.EquipmentReportResourceError):
        utils.check_aggregation_reports(["report.json"], api=api, nk=MagicMock(), config=config)


def test_failed_event_stays_deduplicated_after_ledger_reopen(tmp_path):
    from xtrek.s3_poller import EventLedger

    path = str(tmp_path / "events.sqlite3")
    ledger = EventLedger(path)
    assert ledger.enqueue("existing-event", "b", "equipment-reports/existing.json")
    ledger.claim()
    ledger.published("existing-event")
    ledger.task_state("existing-event", "failed")
    ledger.close()
    reopened = EventLedger(path)
    assert not reopened.enqueue("existing-event", "b", "equipment-reports/existing.json")
    assert reopened.claim() is None
    assert reopened.db.execute("SELECT state FROM s3_outbox").fetchone()[0] == "published"
    assert reopened.db.execute("SELECT state FROM task_attempts").fetchone()[0] == "failed"
    reopened.close()
