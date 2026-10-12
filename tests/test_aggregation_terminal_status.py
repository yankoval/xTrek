"""Run the production router's captured task options through real Celery."""
import sys
from unittest.mock import MagicMock

import celery
import celery.signals
import pytest

import test_tasks_unit_set_flow as helpers


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
    app = celery.Celery("aggregation-status-tests", broker="memory://", fixups=[])
    task = app.task(shared=False, **options["tasks.process_s3_event"])(
        tasks.process_s3_event._func
    )
    record = MagicMock()
    monkeypatch.setattr(tasks, "_record_local_event_state", record)
    real_signals.task_postrun.connect(tasks._finish_token_snapshot, weak=False)
    try:
        yield tasks, task, record
    finally:
        real_signals.task_postrun.disconnect(tasks._finish_token_snapshot)
        app.close()


@pytest.fixture(params=[("aggReceipts", "update_aggregation_status"),
                        ("aggSetReceipts", "update_aggregation_set_status")])
def route(request):
    return request.param


def install_response(monkeypatch, tasks, route, response=None, error=None):
    update = MagicMock(return_value=response, side_effect=error)
    monkeypatch.setattr(tasks, route[1], update)
    downstream = {}
    for name in ("check_aggregation_report", "create_aggregation_report",
                 "sign_and_send_aggregation", "sign_and_send_aggregation_set"):
        downstream[name] = MagicMock(return_value=True)
        monkeypatch.setattr(tasks, name, downstream[name])
    return update, downstream


def run(task, route):
    return task.apply(args=[{"bucket": "internal-bucket",
                            "key": route[0] + "/existing.json"}])


@pytest.mark.parametrize("as_list", [False, True])
def test_rejection_is_failure_without_retry_or_downstream_actions(router, route, monkeypatch,
                                                                as_list, capsys):
    tasks, task, record = router
    response = {"status": "CHECKED_NOT_OK", "number": "original-document",
                "commonErrors": [{"errorCode": "ERROR_32", "cis": "private-code"}]}
    update, downstream = install_response(monkeypatch, tasks, route,
                                          [response] if as_list else response)
    retry = MagicMock(side_effect=AssertionError("terminal status must not retry"))
    monkeypatch.setattr(task, "retry", retry)
    result = run(task, route)
    assert not result.successful()
    assert isinstance(result.result, tasks.AggregationDocumentRejected)
    assert isinstance(result.result, tasks.ReconciliationRequired)
    assert "private-code" not in str(result.result) + capsys.readouterr().out
    update.assert_called_once_with("existing", "chemistry")
    retry.assert_not_called()
    for action in downstream.values():
        action.assert_not_called()
    record.assert_called_once_with(result.id, "failed")


@pytest.mark.parametrize("response", [
    None, [], {}, "connection failed", {"status": "IN_PROGRESS"},
    {"status": "UNKNOWN"}, {"error": "upstream unavailable", "status_code": 503},
    {"error": "invalid response", "status": "CHECKED_NOT_OK"},
    [{"status": "CHECKED_OK"}, {"status": "CHECKED_NOT_OK"}],
    [{"status": "CHECKED_NOT_OK"}, {"status": "CHECKED_OK"}],
])
def test_pending_unknown_and_ambiguous_responses_keep_retry(router, route, monkeypatch, response):
    tasks, task, _ = router
    _, downstream = install_response(monkeypatch, tasks, route, response)
    retry = MagicMock(side_effect=RuntimeError("retry scheduled"))
    monkeypatch.setattr(task, "retry", retry)
    result = run(task, route)
    assert not result.successful()
    assert not isinstance(result.result, tasks.AggregationDocumentRejected)
    retry.assert_called_once()
    for action in downstream.values():
        action.assert_not_called()


def test_transport_error_keeps_retry(router, route, monkeypatch):
    tasks, task, _ = router
    install_response(monkeypatch, tasks, route, error=ConnectionError("connection lost"))
    retry = MagicMock(side_effect=RuntimeError("retry scheduled"))
    monkeypatch.setattr(task, "retry", retry)
    assert not run(task, route).successful()
    retry.assert_called_once()


@pytest.mark.parametrize("as_list", [False, True])
def test_checked_ok_preserves_existing_downstream_flow(router, route, monkeypatch, as_list):
    tasks, task, record = router
    response = {"status": "CHECKED_OK", "number": "original-document"}
    update, downstream = install_response(monkeypatch, tasks, route,
                                          [response] if as_list else response)
    retry = MagicMock(side_effect=AssertionError("successful status must not retry"))
    monkeypatch.setattr(task, "retry", retry)
    result = run(task, route)
    assert result.successful()
    update.assert_called_once_with("existing", "chemistry")
    retry.assert_not_called()
    if route[0] == "aggReceipts":
        downstream["check_aggregation_report"].assert_called_once_with("existing")
        downstream["sign_and_send_aggregation"].assert_not_called()
    else:
        downstream["create_aggregation_report"].assert_called_once_with("existing")
        downstream["sign_and_send_aggregation"].assert_called_once_with(
            "existing", "chemistry", "/tmp/sign", 120
        )
        downstream["check_aggregation_report"].assert_not_called()
    record.assert_called_once_with(result.id, "succeeded")


def test_failed_published_receipt_event_is_durable_and_not_republished(tmp_path):
    from xtrek.s3_poller import EventLedger

    path = str(tmp_path / "events.sqlite3")
    ledger = EventLedger(path)
    assert ledger.enqueue("existing-event", "b", "aggReceipts/existing.json")
    assert ledger.claim()[0] == "existing-event"
    ledger.published("existing-event")
    ledger.task_state("existing-event", "failed")
    ledger.close()

    reopened = EventLedger(path)
    assert not reopened.enqueue("existing-event", "b", "aggReceipts/existing.json")
    assert reopened.claim() is None
    assert reopened.db.execute("SELECT state FROM s3_outbox").fetchone()[0] == "published"
    assert reopened.db.execute("SELECT state FROM task_attempts").fetchone()[0] == "failed"
    reopened.close()
