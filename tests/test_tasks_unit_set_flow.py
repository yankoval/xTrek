import importlib
import sys
import types
from unittest.mock import MagicMock, patch


class _EagerResult:
    def __init__(self, value=None, error=None):
        self.result = value if error is None else error
        self._successful = error is None

    def successful(self):
        return self._successful


class _EagerTask:
    def __init__(self, func, bind=False):
        self._func = func
        self._bind = bind

    def __call__(self, *args, **kwargs):
        if self._bind:
            return self._func(types.SimpleNamespace(), *args, **kwargs)
        return self._func(*args, **kwargs)

    def apply(self, args=None, kwargs=None):
        try:
            return _EagerResult(self(*(args or ()), **(kwargs or {})))
        except Exception as exc:
            return _EagerResult(error=exc)


class _FakeCelery:
    def __init__(self, *args, **kwargs):
        self.conf = types.SimpleNamespace(update=lambda **kw: None)

    def task(self, *args, **kwargs):
        def decorator(func):
            return _EagerTask(func, bind=kwargs.get("bind", False))

        return decorator


class _FakeSignal:
    def connect(self, func):
        return func


def import_tasks(monkeypatch, module_name="tasks"):
    monkeypatch.setenv("YMQ_ACCESS_KEY", "test-access")
    monkeypatch.setenv("YMQ_SECRET_KEY", "test-secret")
    monkeypatch.setenv("YMQ_QUEUE_URL", "https://example.test/queue")
    sys.modules.pop("tasks", None)
    sys.modules.pop("xtrek.tasks", None)
    monkeypatch.setitem(sys.modules, "celery", types.SimpleNamespace(Celery=_FakeCelery))
    monkeypatch.setitem(
        sys.modules,
        "celery.signals",
        types.SimpleNamespace(task_prerun=_FakeSignal(), task_postrun=_FakeSignal()),
    )
    config = {
        "input_bucket": "input-bucket",
        "internal_bucket": "internal-bucket",
        "product_group": "chemistry",
        "contact_person": "scan",
        "sign": "/tmp/sign",
    }
    with patch("xtrek.config_loader.load_config", return_value=config):
        return importlib.import_module(module_name)


def test_tasks_module_is_available_from_package_and_legacy_entrypoint(monkeypatch):
    legacy_tasks = import_tasks(monkeypatch, "tasks")
    package_tasks = importlib.import_module("xtrek.tasks")

    assert legacy_tasks is package_tasks


def test_celery_queue_name_can_be_isolated_by_environment(monkeypatch):
    monkeypatch.setenv("YMQ_QUEUE_NAME", "queue_xtrek_aggregate_test")

    tasks = import_tasks(monkeypatch, "xtrek.tasks")

    assert tasks.REAL_QUEUE_NAME == "queue_xtrek_aggregate_test"


def test_celery_task_boundaries_clear_token_snapshot(monkeypatch):
    tasks = import_tasks(monkeypatch)
    clear = MagicMock()
    monkeypatch.setattr(tasks.TokenProcessor, "clear_command_snapshots", clear)

    tasks._start_token_snapshot()
    tasks._finish_token_snapshot()

    assert clear.call_count == 2


def test_process_s3_event_routes_equipment_report_through_celery(monkeypatch):
    tasks = import_tasks(monkeypatch)
    mock_logic = MagicMock(return_value="equipment report handled")
    monkeypatch.setattr(tasks, "logic_start_equipment_reports", mock_logic)

    result = tasks.process_s3_event.apply(args=[{
        "bucket": "internal-bucket",
        "key": "equipment-reports/T-UNIT.json",
    }])

    assert result.successful()
    mock_logic.assert_called_once_with("internal-bucket/equipment-reports/T-UNIT.json")


def test_unit_equipment_report_creates_utilisation_and_skips_virtual_tasks(monkeypatch):
    tasks = import_tasks(monkeypatch)
    monkeypatch.setattr(tasks, "check_aggregation_reports", MagicMock(return_value={"report": None}))
    create_util = MagicMock(return_value="T-UNIT")
    sign_util = MagicMock(return_value=True)
    create_virtual = MagicMock()
    monkeypatch.setattr(tasks, "create_utilisation_task_from_report", create_util)
    monkeypatch.setattr(tasks, "sign_and_send_utilisation", sign_util)
    monkeypatch.setattr(tasks, "create_virtual_tasks_from_equipment_report", create_virtual)
    monkeypatch.setattr(tasks, "get_production_order_data", MagicMock(return_value={"GtinType": "UNIT"}))

    tasks.logic_start_equipment_reports("internal-bucket/equipment-reports/T-UNIT.json")

    create_util.assert_called_once_with("T-UNIT", "chemistry")
    sign_util.assert_called_once_with("T-UNIT", "/tmp/sign", 120)
    create_virtual.assert_not_called()


def test_equipment_report_raises_when_utilisation_send_returns_error(monkeypatch):
    tasks = import_tasks(monkeypatch)
    monkeypatch.setattr(tasks, "check_aggregation_reports", MagicMock(return_value={"report": None}))
    monkeypatch.setattr(tasks, "create_utilisation_task_from_report", MagicMock(return_value="T-UNIT"))
    monkeypatch.setattr(tasks, "sign_and_send_utilisation", MagicMock(return_value={
        "error": "no healthy upstream",
        "status_code": 503,
    }))

    result = tasks.process_s3_event.apply(args=[{
        "bucket": "internal-bucket",
        "key": "equipment-reports/T-UNIT.json",
    }])

    assert not result.successful()
    assert "sign_and_send_utilisation failed for T-UNIT" in str(result.result)


def test_equipment_report_retries_when_precheck_returns_api_error(monkeypatch):
    tasks = import_tasks(monkeypatch)
    create_util = MagicMock()
    sign_util = MagicMock()
    monkeypatch.setattr(
        tasks,
        "check_aggregation_reports",
        MagicMock(return_value={
            "report": {"api_error": ["401 Client Error: Unauthorized"]}
        }),
    )
    monkeypatch.setattr(tasks, "create_utilisation_task_from_report", create_util)
    monkeypatch.setattr(tasks, "sign_and_send_utilisation", sign_util)

    result = tasks.process_s3_event.apply(args=[{
        "bucket": "internal-bucket",
        "key": "equipment-reports/T-UNIT.json",
    }])

    assert not result.successful()
    error = str(result.result)
    assert "equipment report precheck True API failure for T-UNIT" in error
    assert "HTTP 401 Unauthorized" in error
    assert (
        "retryable HTTP codes: 401, 408, 429, 500, 502, 503, 504"
        in error
    )
    create_util.assert_not_called()
    sign_util.assert_not_called()


def test_equipment_report_stops_without_retry_on_forbidden_precheck(monkeypatch):
    tasks = import_tasks(monkeypatch)
    create_util = MagicMock()
    sign_util = MagicMock()
    set_check_tag = MagicMock(return_value=True)
    monkeypatch.setattr(
        tasks,
        "check_aggregation_reports",
        MagicMock(return_value={
            "s3://internal-bucket/equipment-reports/T-UNIT.json": {
                "api_error": ["403 Client Error: Forbidden"]
            }
        }),
    )
    monkeypatch.setattr(
        tasks,
        "_set_equipment_report_check_tag",
        set_check_tag,
    )
    monkeypatch.setattr(tasks, "create_utilisation_task_from_report", create_util)
    monkeypatch.setattr(tasks, "sign_and_send_utilisation", sign_util)

    result = tasks.process_s3_event.apply(args=[{
        "bucket": "internal-bucket",
        "key": "equipment-reports/T-UNIT.json",
    }])

    assert result.successful()
    set_check_tag.assert_called_once_with(
        "s3://internal-bucket/equipment-reports/T-UNIT.json",
        "true-api-403-forbidden",
    )
    create_util.assert_not_called()
    sign_util.assert_not_called()


def test_equipment_report_skips_business_validation_errors_without_retry(monkeypatch):
    tasks = import_tasks(monkeypatch)
    create_util = MagicMock()
    sign_util = MagicMock()
    monkeypatch.setattr(
        tasks,
        "check_aggregation_reports",
        MagicMock(return_value={
            "report": {"wrongunitstatus": ["010... (Статус: INTRODUCED)"]}
        }),
    )
    monkeypatch.setattr(tasks, "create_utilisation_task_from_report", create_util)
    monkeypatch.setattr(tasks, "sign_and_send_utilisation", sign_util)

    result = tasks.process_s3_event.apply(args=[{
        "bucket": "internal-bucket",
        "key": "equipment-reports/T-UNIT.json",
    }])

    assert result.successful()
    create_util.assert_not_called()
    sign_util.assert_not_called()


def test_unit_utilisation_success_starts_introduce_from_report(monkeypatch):
    tasks = import_tasks(monkeypatch)
    status = tasks.UtilisationReportStatus(
        omsId="oms",
        reportId="report",
        reportStatus="SUCCESS",
    )
    create_intro = MagicMock(return_value="T-UNIT")
    sign_intro = MagicMock(return_value=True)
    monkeypatch.setattr(tasks, "update_utilisation_report_status", MagicMock(return_value=status))
    monkeypatch.setattr(tasks, "get_production_order_data", MagicMock(return_value={"GtinType": "UNIT"}))
    monkeypatch.setattr(tasks, "create_introduce_task_from_report", create_intro)
    monkeypatch.setattr(tasks, "sign_and_send_introduce", sign_intro)
    monkeypatch.setattr(tasks, "trigger_set_aggregation_if_ready", MagicMock())

    result = tasks.logic_utilisationReceipt("internal-bucket/utilisationReceipts/T-UNIT.json")

    assert "Introduction task for UNIT T-UNIT started successfully" in result
    create_intro.assert_called_once_with("T-UNIT", "chemistry")
    sign_intro.assert_called_once_with("T-UNIT", "chemistry", "/tmp/sign", 120)
    tasks.trigger_set_aggregation_if_ready.assert_not_called()


def test_unit_utilisation_raises_when_introduce_send_returns_error(monkeypatch):
    tasks = import_tasks(monkeypatch)
    status = tasks.UtilisationReportStatus(
        omsId="oms",
        reportId="report",
        reportStatus="SUCCESS",
    )
    monkeypatch.setattr(tasks, "update_utilisation_report_status", MagicMock(return_value=status))
    monkeypatch.setattr(tasks, "get_production_order_data", MagicMock(return_value={"GtinType": "UNIT"}))
    monkeypatch.setattr(tasks, "create_introduce_task_from_report", MagicMock(return_value="T-UNIT"))
    monkeypatch.setattr(tasks, "sign_and_send_introduce", MagicMock(return_value={
        "error": "no healthy upstream",
        "status_code": 503,
    }))

    result = tasks.process_s3_event.apply(args=[{
        "bucket": "internal-bucket",
        "key": "utilisationReceipts/T-UNIT.json",
    }])

    assert not result.successful()
    assert "sign_and_send_introduce failed for T-UNIT" in str(result.result)


def test_unit_checked_ok_introduce_starts_standard_aggregation(monkeypatch):
    tasks = import_tasks(monkeypatch)
    create_agg = MagicMock(return_value="T-UNIT")
    sign_agg = MagicMock(return_value=True)
    monkeypatch.setattr(tasks, "update_introduce_status", MagicMock(return_value=[{"status": "CHECKED_OK"}]))
    monkeypatch.setattr(tasks, "_find_production_order_id_by_suz_order_id", MagicMock(return_value="T-UNIT"))
    monkeypatch.setattr(tasks, "get_production_order_data", MagicMock(return_value={"GtinType": "UNIT"}))
    monkeypatch.setattr(tasks, "create_aggregation_report", create_agg)
    monkeypatch.setattr(tasks, "sign_and_send_aggregation", sign_agg)
    monkeypatch.setattr(tasks, "trigger_set_aggregation_if_ready", MagicMock())

    result = tasks.logic_update_introduce("internal-bucket/introduceReceipts/T-UNIT.json")

    assert "Standard aggregation for UNIT T-UNIT started successfully" in result
    create_agg.assert_called_once_with("T-UNIT")
    sign_agg.assert_called_once_with("T-UNIT", "chemistry", "/tmp/sign", 120)
    tasks.trigger_set_aggregation_if_ready.assert_not_called()


def test_unit_introduce_raises_when_aggregation_send_returns_error(monkeypatch):
    tasks = import_tasks(monkeypatch)
    monkeypatch.setattr(tasks, "update_introduce_status", MagicMock(return_value=[{"status": "CHECKED_OK"}]))
    monkeypatch.setattr(tasks, "_find_production_order_id_by_suz_order_id", MagicMock(return_value="T-UNIT"))
    monkeypatch.setattr(tasks, "get_production_order_data", MagicMock(return_value={"GtinType": "UNIT"}))
    monkeypatch.setattr(tasks, "create_aggregation_report", MagicMock(return_value="T-UNIT"))
    monkeypatch.setattr(tasks, "sign_and_send_aggregation", MagicMock(return_value={
        "error": "no healthy upstream",
        "status_code": 503,
    }))

    result = tasks.process_s3_event.apply(args=[{
        "bucket": "internal-bucket",
        "key": "introduceReceipts/T-UNIT.json",
    }])

    assert not result.successful()
    assert "sign_and_send_aggregation failed for T-UNIT" in str(result.result)


def test_unit_checked_ok_introduce_uses_status_production_order_id(monkeypatch):
    tasks = import_tasks(monkeypatch)
    create_agg = MagicMock(return_value="T-UNIT")
    sign_agg = MagicMock(return_value=True)
    find_prod = MagicMock(return_value=None)
    monkeypatch.setattr(tasks, "update_introduce_status", MagicMock(return_value=[{
        "status": "CHECKED_OK",
        "productionOrderId": "T-UNIT",
    }]))
    monkeypatch.setattr(tasks, "_find_production_order_id_by_suz_order_id", find_prod)
    monkeypatch.setattr(tasks, "get_production_order_data", MagicMock(return_value={"GtinType": "UNIT"}))
    monkeypatch.setattr(tasks, "create_aggregation_report", create_agg)
    monkeypatch.setattr(tasks, "sign_and_send_aggregation", sign_agg)

    result = tasks.logic_update_introduce("internal-bucket/introduceReceipts/T-UNIT.json")

    assert "Standard aggregation for UNIT T-UNIT started successfully" in result
    find_prod.assert_not_called()
    create_agg.assert_called_once_with("T-UNIT")
    sign_agg.assert_called_once_with("T-UNIT", "chemistry", "/tmp/sign", 120)


def test_set_equipment_report_still_creates_virtual_tasks(monkeypatch):
    tasks = import_tasks(monkeypatch)
    monkeypatch.setattr(tasks, "check_aggregation_reports", MagicMock(return_value={"report": None}))
    monkeypatch.setattr(tasks, "create_utilisation_task_from_report", MagicMock(return_value="T-SET"))
    monkeypatch.setattr(tasks, "sign_and_send_utilisation", MagicMock(return_value=True))
    create_virtual = MagicMock(return_value="created")
    monkeypatch.setattr(tasks, "create_virtual_tasks_from_equipment_report", create_virtual)
    monkeypatch.setattr(tasks, "get_production_order_data", MagicMock(return_value={"GtinType": "SET"}))

    tasks.logic_start_equipment_reports("internal-bucket/equipment-reports/T-SET.json")

    create_virtual.assert_called_once_with("T-SET")


def test_set_utilisation_success_still_checks_set_readiness(monkeypatch):
    tasks = import_tasks(monkeypatch)
    status = tasks.UtilisationReportStatus(
        omsId="oms",
        reportId="report",
        reportStatus="SUCCESS",
    )
    trigger_ready = MagicMock(return_value="Set T-SET not ready yet")
    monkeypatch.setattr(tasks, "update_utilisation_report_status", MagicMock(return_value=status))
    monkeypatch.setattr(tasks, "get_production_order_data", MagicMock(return_value={"GtinType": "SET"}))
    monkeypatch.setattr(tasks, "trigger_set_aggregation_if_ready", trigger_ready)
    monkeypatch.setattr(tasks, "create_introduce_task_from_report", MagicMock())
    monkeypatch.setattr(tasks, "sign_and_send_introduce", MagicMock())

    result = tasks.logic_utilisationReceipt("internal-bucket/utilisationReceipts/T-SET.json")

    assert "Utilization report for set T-SET is SUCCESS" in result
    trigger_ready.assert_called_once_with("T-SET")
    tasks.create_introduce_task_from_report.assert_not_called()
    tasks.sign_and_send_introduce.assert_not_called()


def test_process_s3_event_routes_all_aggregate_operation_stages(monkeypatch):
    tasks = import_tasks(monkeypatch)
    routes = {
        "equipment-reports-disaggregation/T-SSCC-1.json": (
            "logic_create_disaggregation",
            "internal-bucket/equipment-reports-disaggregation/T-SSCC-1.json",
        ),
        "disaggregationTasks/T-SSCC-1.json": (
            "logic_send_disaggregation",
            "internal-bucket/disaggregationTasks/T-SSCC-1.json",
        ),
        "disaggregationReceipts/T-SSCC-1.json": (
            "logic_update_disaggregation",
            "internal-bucket/disaggregationReceipts/T-SSCC-1.json",
        ),
        "equipment-reports-reaggregation-removing/T-SSCC-2.json": (
            "logic_create_reaggregation_removing",
            "internal-bucket/equipment-reports-reaggregation-removing/T-SSCC-2.json",
        ),
        "reaggregationTasks/T-SSCC-2.json": (
            "logic_send_reaggregation",
            "internal-bucket/reaggregationTasks/T-SSCC-2.json",
        ),
        "reaggregationReceipts/T-SSCC-2.json": (
            "logic_update_reaggregation",
            "internal-bucket/reaggregationReceipts/T-SSCC-2.json",
        ),
    }
    handlers = {
        name: MagicMock(return_value="handled")
        for name, _ in routes.values()
    }
    for name, handler in handlers.items():
        monkeypatch.setattr(tasks, name, handler)

    for key in routes:
        result = tasks.process_s3_event.apply(args=[{
            "bucket": "internal-bucket",
            "key": key,
        }])
        assert result.successful()

    for _, (handler_name, full_key) in routes.items():
        handlers[handler_name].assert_called_once_with(full_key)


def test_disaggregation_report_creates_task_with_report_task_id(monkeypatch):
    tasks = import_tasks(monkeypatch)
    report_path = (
        "s3://internal-bucket/"
        "equipment-reports-disaggregation/T-SSCC-uuid.json"
    )
    monkeypatch.setattr(
        tasks,
        "check_disaggregation_reports",
        MagicMock(return_value={report_path: None}),
    )
    create_task = MagicMock(return_value="T-SSCC-uuid")
    monkeypatch.setattr(tasks, "create_disaggregation_task_from_report", create_task)

    result = tasks.logic_create_disaggregation(report_path.removeprefix("s3://"))

    assert result == "Disaggregation task T-SSCC-uuid created"
    tasks.check_disaggregation_reports.assert_called_once_with(["T-SSCC-uuid"])
    create_task.assert_called_once_with(report_path, task_id="T-SSCC-uuid")


def test_reaggregation_report_creates_task_with_report_task_id(monkeypatch):
    tasks = import_tasks(monkeypatch)
    report_path = (
        "s3://internal-bucket/"
        "equipment-reports-reaggregation-removing/T-SSCC-uuid.json"
    )
    monkeypatch.setattr(
        tasks,
        "check_reaggregation_removing_reports",
        MagicMock(return_value={report_path: None}),
    )
    create_task = MagicMock(return_value="T-SSCC-uuid")
    monkeypatch.setattr(
        tasks,
        "create_reaggregation_removing_task_from_report",
        create_task,
    )

    result = tasks.logic_create_reaggregation_removing(
        report_path.removeprefix("s3://")
    )

    assert result == "Reaggregation REMOVING task T-SSCC-uuid created"
    tasks.check_reaggregation_removing_reports.assert_called_once_with(
        ["T-SSCC-uuid"]
    )
    create_task.assert_called_once_with(report_path, task_id="T-SSCC-uuid")


def test_aggregate_operation_precheck_api_error_retries_without_creating_task(
    monkeypatch,
):
    tasks = import_tasks(monkeypatch)
    report_path = (
        "s3://internal-bucket/"
        "equipment-reports-disaggregation/T-None-uuid.json"
    )
    monkeypatch.setattr(
        tasks,
        "check_disaggregation_reports",
        MagicMock(return_value={
            report_path: {"api_error": ["503 Service Unavailable"]},
        }),
    )
    create_task = MagicMock()
    monkeypatch.setattr(tasks, "create_disaggregation_task_from_report", create_task)

    result = tasks.process_s3_event.apply(args=[{
        "bucket": "internal-bucket",
        "key": "equipment-reports-disaggregation/T-None-uuid.json",
    }])

    assert not result.successful()
    assert "HTTP 503 Service Unavailable" in str(result.result)
    create_task.assert_not_called()


def test_aggregate_operation_business_error_stops_without_retry(monkeypatch):
    tasks = import_tasks(monkeypatch)
    report_path = (
        "s3://internal-bucket/"
        "equipment-reports-reaggregation-removing/T-SSCC-uuid.json"
    )
    monkeypatch.setattr(
        tasks,
        "check_reaggregation_removing_reports",
        MagicMock(return_value={
            report_path: {"wrongowner": ["00000123456789012345"]},
        }),
    )
    create_task = MagicMock()
    monkeypatch.setattr(
        tasks,
        "create_reaggregation_removing_task_from_report",
        create_task,
    )

    result = tasks.process_s3_event.apply(args=[{
        "bucket": "internal-bucket",
        "key": "equipment-reports-reaggregation-removing/T-SSCC-uuid.json",
    }])

    assert result.successful()
    create_task.assert_not_called()


def test_aggregate_operation_task_events_sign_and_send(monkeypatch):
    tasks = import_tasks(monkeypatch)
    send_disaggregation = MagicMock(return_value={"document_id": "doc-1"})
    send_reaggregation = MagicMock(return_value={"document_id": "doc-2"})
    monkeypatch.setattr(
        tasks,
        "sign_and_send_disaggregation",
        send_disaggregation,
    )
    monkeypatch.setattr(
        tasks,
        "sign_and_send_reaggregation",
        send_reaggregation,
    )

    tasks.logic_send_disaggregation(
        "internal-bucket/disaggregationTasks/T-SSCC-1.json"
    )
    tasks.logic_send_reaggregation(
        "internal-bucket/reaggregationTasks/T-SSCC-2.json"
    )

    send_disaggregation.assert_called_once_with(
        "T-SSCC-1",
        "chemistry",
        "/tmp/sign",
        120,
    )
    send_reaggregation.assert_called_once_with(
        "T-SSCC-2",
        "chemistry",
        "/tmp/sign",
        120,
    )


def test_disaggregation_checked_ok_runs_final_report_check(monkeypatch):
    tasks = import_tasks(monkeypatch)
    update_status = MagicMock(return_value={"status": "CHECKED_OK"})
    final_check = MagicMock(return_value={
        "finished": ["All aggregates are disaggregated"],
    })
    monkeypatch.setattr(tasks, "update_disaggregation_status", update_status)
    monkeypatch.setattr(tasks, "check_disaggregation_report", final_check)

    result = tasks.logic_update_disaggregation(
        "internal-bucket/disaggregationReceipts/T-SSCC-1.json"
    )

    assert result == "disaggregation T-SSCC-1 finished and report tagged"
    update_status.assert_called_once_with("T-SSCC-1", "chemistry")
    final_check.assert_called_once_with("T-SSCC-1", final=True)


def test_reaggregation_checked_ok_runs_final_report_check(monkeypatch):
    tasks = import_tasks(monkeypatch)
    update_status = MagicMock(return_value=[{"status": "CHECKED_OK"}])
    final_check = MagicMock(return_value={
        "finished": ["All requested codes are removed"],
    })
    monkeypatch.setattr(tasks, "update_reaggregation_status", update_status)
    monkeypatch.setattr(
        tasks,
        "check_reaggregation_removing_report",
        final_check,
    )

    result = tasks.logic_update_reaggregation(
        "internal-bucket/reaggregationReceipts/T-SSCC-2.json"
    )

    assert result == "reaggregation-removing T-SSCC-2 finished and report tagged"
    update_status.assert_called_once_with("T-SSCC-2", "chemistry")
    final_check.assert_called_once_with("T-SSCC-2")


def test_aggregate_operation_nonfinal_status_retries(monkeypatch):
    tasks = import_tasks(monkeypatch)
    final_check = MagicMock()
    monkeypatch.setattr(
        tasks,
        "update_disaggregation_status",
        MagicMock(return_value={"status": "IN_PROGRESS"}),
    )
    monkeypatch.setattr(tasks, "check_disaggregation_report", final_check)

    result = tasks.process_s3_event.apply(args=[{
        "bucket": "internal-bucket",
        "key": "disaggregationReceipts/T-SSCC-1.json",
    }])

    assert not result.successful()
    assert "status is IN_PROGRESS" in str(result.result)
    final_check.assert_not_called()


def test_aggregate_operation_waits_for_true_api_state_after_checked_ok(monkeypatch):
    tasks = import_tasks(monkeypatch)
    monkeypatch.setattr(
        tasks,
        "update_reaggregation_status",
        MagicMock(return_value={"status": "CHECKED_OK"}),
    )
    monkeypatch.setattr(
        tasks,
        "check_reaggregation_removing_report",
        MagicMock(return_value=None),
    )

    result = tasks.process_s3_event.apply(args=[{
        "bucket": "internal-bucket",
        "key": "reaggregationReceipts/T-SSCC-2.json",
    }])

    assert not result.successful()
    assert "not reflected in the current aggregate state yet" in str(result.result)
