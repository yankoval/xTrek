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


def import_tasks(monkeypatch, module_name="tasks"):
    monkeypatch.setenv("YMQ_ACCESS_KEY", "test-access")
    monkeypatch.setenv("YMQ_SECRET_KEY", "test-secret")
    monkeypatch.setenv("YMQ_QUEUE_URL", "https://example.test/queue")
    sys.modules.pop("tasks", None)
    sys.modules.pop("xtrek.tasks", None)
    monkeypatch.setitem(sys.modules, "celery", types.SimpleNamespace(Celery=_FakeCelery))
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
        "retryable HTTP codes: 401, 403, 408, 429, 500, 502, 503, 504"
        in error
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
