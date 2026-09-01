import base64
import json
import uuid
from unittest.mock import MagicMock

import pytest

from xtrek import create_emission_task_sample as workflow


def _config(tmp_path):
    return {
        "disaggregation-tasks": str(tmp_path / "disaggregation-tasks"),
        "disaggregation-receipts": str(tmp_path / "disaggregation-receipts"),
        "disaggregations": str(tmp_path / "disaggregations"),
        "reaggregation-tasks": str(tmp_path / "reaggregation-tasks"),
        "reaggregation-receipts": str(tmp_path / "reaggregation-receipts"),
        "reaggregations": str(tmp_path / "reaggregations"),
        "sign": str(tmp_path / "sign"),
        "SIGNING_TIMEOUT": 1,
        "s3_config": {},
    }


def _mock_token_lookup(monkeypatch):
    token_processor = MagicMock()
    token_processor.get_token_value_by_inn.return_value = "JWT"
    monkeypatch.setattr(workflow, "OrganizationManager", MagicMock())
    monkeypatch.setattr(
        workflow,
        "TokenProcessor",
        MagicMock(return_value=token_processor),
    )
    return token_processor


def test_create_disaggregation_from_txt(tmp_path, monkeypatch):
    config = _config(tmp_path)
    monkeypatch.setattr(workflow, "load_config", lambda name: config)
    source = tmp_path / "aggregates.txt"
    source.write_text("000123456789012345\n000123456789012352\n", encoding="utf-8")

    task_id = workflow.create_disaggregation_task(
        str(source),
        "7701234567",
        task_id="DISAGG-1",
    )

    assert task_id == "DISAGG-1"
    payload = json.loads(
        (tmp_path / "disaggregation-tasks" / "DISAGG-1.json").read_text()
    )
    assert payload == {
        "participant_inn": "7701234567",
        "products_list": [
            {"uitu": "00000123456789012345"},
            {"uitu": "00000123456789012352"},
        ],
    }


def test_create_disaggregation_from_equipment_report_uses_filename_as_task_id(
    tmp_path,
    monkeypatch,
):
    config = _config(tmp_path)
    monkeypatch.setattr(workflow, "load_config", lambda name: config)
    report = tmp_path / "T-000123456789012345-11111111-1111-1111-1111-111111111111.json"
    report.write_text(json.dumps({
        "participant_inn": "7701234567",
        "products_list": [{"uitu": "000123456789012345"}],
    }))

    task_id = workflow.create_disaggregation_task_from_report(str(report))

    assert task_id == report.stem
    payload = json.loads(
        (tmp_path / "disaggregation-tasks" / f"{report.stem}.json").read_text()
    )
    assert payload == {
        "participant_inn": "7701234567",
        "products_list": [{"uitu": "00000123456789012345"}],
    }


def test_create_reaggregation_removing_normalizes_product_codes(tmp_path, monkeypatch):
    config = _config(tmp_path)
    monkeypatch.setattr(workflow, "load_config", lambda name: config)
    source = tmp_path / "children.json"
    source.write_text(
        json.dumps({"codes": ["010460000000000021ABC\u001d93CRYPTO"]}),
        encoding="utf-8",
    )

    task_id = workflow.create_reaggregation_removing_task(
        "000123456789012345",
        str(source),
        "7701234567",
        task_id="REMOVE-1",
    )

    assert task_id == "REMOVE-1"
    payload = json.loads(
        (tmp_path / "reaggregation-tasks" / "REMOVE-1.json").read_text()
    )
    assert payload == {
        "participant_inn": "7701234567",
        "reaggregation_type": "REMOVING",
        "uitu": "00000123456789012345",
        "uit_uitu_list": [{"uit_uitu": "010460000000000021ABC"}],
    }


def test_create_reaggregation_removing_nested_kitu(tmp_path, monkeypatch):
    config = _config(tmp_path)
    monkeypatch.setattr(workflow, "load_config", lambda name: config)
    source = tmp_path / "boxes.json"
    source.write_text(json.dumps(["000987654321098765"]), encoding="utf-8")

    workflow.create_reaggregation_removing_task(
        "000123456789012345",
        str(source),
        "7701234567",
        code_field="kitu",
        task_id="REMOVE-BOX",
    )

    payload = json.loads(
        (tmp_path / "reaggregation-tasks" / "REMOVE-BOX.json").read_text()
    )
    assert payload["uit_uitu_list"] == [{"kitu": "00000987654321098765"}]


def test_create_reaggregation_removing_from_equipment_report(tmp_path, monkeypatch):
    config = _config(tmp_path)
    monkeypatch.setattr(workflow, "load_config", lambda name: config)
    report = tmp_path / "T-KIZ-22222222-2222-2222-2222-222222222222.json"
    report.write_text(json.dumps({
        "participant_inn": "7701234567",
        "reaggregation_type": "REMOVING",
        "uitu": "000123456789012345",
        "uit_uitu_list": [
            {"uit_uitu": "010460000000000021ABC\u001d93CRYPTO"},
        ],
    }))

    task_id = workflow.create_reaggregation_removing_task_from_report(str(report))

    assert task_id == report.stem
    payload = json.loads(
        (tmp_path / "reaggregation-tasks" / f"{report.stem}.json").read_text()
    )
    assert payload == {
        "participant_inn": "7701234567",
        "reaggregation_type": "REMOVING",
        "uitu": "00000123456789012345",
        "uit_uitu_list": [{"uit_uitu": "010460000000000021ABC"}],
    }


def test_reaggregation_rejects_mixed_fields():
    with pytest.raises(ValueError, match="Нельзя смешивать"):
        workflow._validate_reaggregation_removing_payload({
            "participant_inn": "7701234567",
            "reaggregation_type": "REMOVING",
            "uitu": "00000123456789012345",
            "uit_uitu_list": [
                {"uit_uitu": "010460000000000021ABC"},
                {"kitu": "00000987654321098765"},
            ],
        })


@pytest.mark.parametrize(
    ("operation", "create_task", "send", "task_id", "document_type"),
    [
        (
            "disaggregation",
            lambda source: workflow.create_disaggregation_task(
                source, "7701234567", task_id="DISAGG-SEND"
            ),
            workflow.sign_and_send_disaggregation,
            "DISAGG-SEND",
            "DISAGGREGATION_DOCUMENT",
        ),
        (
            "reaggregation",
            lambda source: workflow.create_reaggregation_removing_task(
                "000123456789012345",
                source,
                "7701234567",
                task_id="REAGG-SEND",
            ),
            workflow.sign_and_send_reaggregation,
            "REAGG-SEND",
            "REAGGREGATION_DOCUMENT",
        ),
    ],
)
def test_sign_and_send_aggregate_operations(
    tmp_path,
    monkeypatch,
    operation,
    create_task,
    send,
    task_id,
    document_type,
):
    config = _config(tmp_path)
    monkeypatch.setattr(workflow, "load_config", lambda name: config)
    source = tmp_path / f"{operation}.txt"
    source.write_text(
        "000123456789012345\n"
        if operation == "disaggregation"
        else "010460000000000021ABC\n",
        encoding="utf-8",
    )
    create_task(str(source))

    fixed_uuid = uuid.UUID("12345678-1234-5678-1234-567812345678")
    monkeypatch.setattr(workflow.uuid, "uuid4", lambda: fixed_uuid)
    sign_dir = tmp_path / "sign"
    sign_dir.mkdir()
    signature_name = f"7701234567_{fixed_uuid}_{operation}.json.sig"
    (sign_dir / signature_name).write_text("BASE64-SIGNATURE", encoding="utf-8")
    _mock_token_lookup(monkeypatch)

    api = MagicMock()
    api.documents_create.return_value = {"id": "DOC-1"}
    api_factory = MagicMock(return_value=api)
    monkeypatch.setattr(workflow, "HonestSignAPI", api_factory)
    monkeypatch.setenv("TRUE_API_HOST", "https://markirovka.sandbox.crptech.ru")

    result = send(task_id, "chemistry", str(sign_dir), 1)

    assert result["document_id"] == "DOC-1"
    assert result["apiHost"] == "https://markirovka.sandbox.crptech.ru"
    api_factory.assert_called_once_with(
        token="JWT",
        host="https://markirovka.sandbox.crptech.ru",
    )
    wrapped_json, = api.documents_create.call_args.args
    wrapper = json.loads(wrapped_json)
    assert wrapper["document_format"] == "MANUAL"
    assert wrapper["type"] == document_type
    assert wrapper["signature"] == "BASE64-SIGNATURE"
    assert json.loads(base64.b64decode(wrapper["product_document"]))


@pytest.mark.parametrize(
    ("operation", "create_task", "status_function", "task_id", "status_dir"),
    [
        (
            "disaggregation",
            lambda source: workflow.create_disaggregation_task(
                source, "7701234567", task_id="DISAGG-STATUS"
            ),
            workflow.update_disaggregation_status,
            "DISAGG-STATUS",
            "disaggregations",
        ),
        (
            "reaggregation",
            lambda source: workflow.create_reaggregation_removing_task(
                "000123456789012345",
                source,
                "7701234567",
                task_id="REAGG-STATUS",
            ),
            workflow.update_reaggregation_status,
            "REAGG-STATUS",
            "reaggregations",
        ),
    ],
)
def test_update_aggregate_operation_status_uses_receipt_host(
    tmp_path,
    monkeypatch,
    operation,
    create_task,
    status_function,
    task_id,
    status_dir,
):
    config = _config(tmp_path)
    monkeypatch.setattr(workflow, "load_config", lambda name: config)
    source = tmp_path / f"{operation}.txt"
    source.write_text(
        "000123456789012345\n"
        if operation == "disaggregation"
        else "010460000000000021ABC\n",
        encoding="utf-8",
    )
    create_task(str(source))

    receipt_dir = tmp_path / f"{operation}-receipts"
    receipt_dir.mkdir()
    (receipt_dir / f"{task_id}.json").write_text(json.dumps({
        "document_id": "DOC-STATUS",
        "productGroup": "chemistry",
        "apiHost": "https://markirovka.sandbox.crptech.ru",
    }))
    _mock_token_lookup(monkeypatch)
    api = MagicMock()
    api.doc.return_value = {"status": "CHECKED_OK"}
    api_factory = MagicMock(return_value=api)
    monkeypatch.setattr(workflow, "HonestSignAPI", api_factory)

    result = status_function(task_id)

    assert result == {"status": "CHECKED_OK"}
    api_factory.assert_called_once_with(
        token="JWT",
        host="https://markirovka.sandbox.crptech.ru",
    )
    api.doc.assert_called_once_with("DOC-STATUS", pg="chemistry")
    assert json.loads((tmp_path / status_dir / f"{task_id}.json").read_text()) == {
        "status": "CHECKED_OK"
    }


def test_operation_paths_fall_back_next_to_aggregation_paths():
    config = {
        "agg-tasks": "s3://bucket/aggTasks/",
        "agg-receipts": "s3://bucket/aggReceipts/",
        "aggs": "s3://bucket/aggs/",
    }

    assert workflow._aggregate_operation_paths(config, "disaggregation") == (
        "s3://bucket/disaggregationTasks",
        "s3://bucket/disaggregationReceipts",
        "s3://bucket/disaggregations",
    )
    assert workflow._aggregate_operation_paths(config, "reaggregation") == (
        "s3://bucket/reaggregationTasks",
        "s3://bucket/reaggregationReceipts",
        "s3://bucket/reaggregations",
    )


def test_true_api_host_rejects_non_crpt_destination(monkeypatch):
    monkeypatch.setenv("TRUE_API_HOST", "https://example.invalid")

    with pytest.raises(ValueError, match="Недопустимый True API host"):
        workflow._resolve_true_api_host({})
