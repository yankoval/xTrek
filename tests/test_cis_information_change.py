import base64
import json
import uuid
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from xtrek import create_emission_task_sample as workflow


def _config(tmp_path):
    return {
        "cis-information-change-tasks": str(tmp_path / "tasks"),
        "cis-information-change-receipts": str(tmp_path / "receipts"),
        "cis-information-changes": str(tmp_path / "statuses"),
        "equipment-reports": str(tmp_path / "equipment-reports"),
        "production_orders_path": str(tmp_path / "production-orders"),
        "emission_receipts": str(tmp_path / "emission-receipts"),
        "kodes": str(tmp_path / "kodes"),
        "sign": str(tmp_path / "sign"),
        "SIGNING_TIMEOUT": 1,
        "s3_config": {},
    }


def _code(serial):
    return f"{_short_code(serial)}\u001d93FULLCRYPTO"


def _short_code(serial):
    return f"010460000000000021{serial}"


def test_create_from_local_txt_builds_production_date_only(tmp_path, monkeypatch):
    config = _config(tmp_path)
    monkeypatch.setattr(workflow, "load_config", lambda name: config)
    source = tmp_path / "codes.txt"
    source.write_text(f"{_code('AAA')}\n{_code('BBB')}\n", encoding="utf-8")

    result = workflow.create_cis_information_change_from_source(
        str(source),
        "7701234567",
        "productionDate",
        "01.08.2026",
    )

    assert result == "codes-production-date"
    task = json.loads((tmp_path / "tasks" / f"{result}.json").read_text())
    assert task == {
        "participantInn": "7701234567",
        "codes": [{
            "code": [_short_code("AAA"), _short_code("BBB")],
            "productionDate": "2026-08-01",
        }],
    }
    assert "expirationDate" not in task["codes"][0]


def test_create_from_equipment_report_uses_storage_source(tmp_path, monkeypatch):
    config = _config(tmp_path)
    monkeypatch.setattr(workflow, "load_config", lambda name: config)
    report_dir = tmp_path / "equipment-reports"
    report_dir.mkdir()
    (report_dir / "REPORT-1.json").write_text(json.dumps({
        "readyBox": [
            {
                "boxNumber": "000000000000000024",
                "productNumbersFull": [_code("AAA")],
            },
            {
                "boxNumber": "000000000000000031",
                "productNumbersFull": [_code("BBB")],
            },
        ]
    }))

    result = workflow.create_cis_information_change_from_equipment_report(
        "REPORT-1",
        "7701234567",
        "expirationDate",
        "2099-12-31",
    )

    assert result == "REPORT-1-expiration-date"
    task = json.loads((tmp_path / "tasks" / f"{result}.json").read_text())
    assert task["codes"][0]["expirationDate"] == "2099-12-31"
    assert task["codes"][0]["code"] == [_short_code("AAA"), _short_code("BBB")]


def test_create_from_equipment_report_supports_pallet_v2(tmp_path, monkeypatch):
    config = _config(tmp_path)
    monkeypatch.setattr(workflow, "load_config", lambda name: config)
    report_dir = tmp_path / "equipment-reports"
    report_dir.mkdir()
    (report_dir / "REPORT-V2.json").write_text(json.dumps({
        "schemaVersion": 2,
        "readyPallet": [{
            "palletNumber": "000000000000000017",
            "palletAggregate": True,
            "readyBox": [{
                "boxNumber": "000000000000000024",
                "boxAgregate": True,
                "productNumbersFull": [_code("AAA"), _code("BBB")],
                "productNumbers": [_short_code("AAA"), _short_code("BBB")],
            }],
        }],
    }))

    result = workflow.create_cis_information_change_from_equipment_report(
        "REPORT-V2",
        "7701234567",
        "productionDate",
        "2026-08-01",
    )

    assert result == "REPORT-V2-production-date"
    task = json.loads((tmp_path / "tasks" / f"{result}.json").read_text())
    assert task["codes"][0]["code"] == [_short_code("AAA"), _short_code("BBB")]


def test_universal_source_reads_s3_uri_through_storage(monkeypatch):
    source = "s3://bucket/path/codes.json"
    storage = MagicMock()
    storage.exists.return_value = True
    storage.read_text.return_value = json.dumps({"codes": [_code("AAA")]})
    monkeypatch.setattr(workflow, "load_config", lambda name: {"s3_config": {}})
    get_storage = MagicMock(return_value=storage)
    monkeypatch.setattr(workflow, "get_storage", get_storage)

    codes = workflow._read_cis_information_change_codes_source(source)

    assert codes == [_short_code("AAA")]
    get_storage.assert_called_once_with(source, {})


def test_create_from_emission_resolves_receipt_codes_and_manufacturer(tmp_path, monkeypatch):
    config = _config(tmp_path)
    monkeypatch.setattr(workflow, "load_config", lambda name: config)
    for directory in ("emission-receipts", "kodes", "production-orders"):
        (tmp_path / directory).mkdir()
    (tmp_path / "emission-receipts" / "PROD-1.json").write_text(
        json.dumps({"orderId": "ORDER-1"})
    )
    (tmp_path / "kodes" / "ORDER-1.json").write_text(
        json.dumps({"codes": [_code("AAA")]})
    )
    (tmp_path / "production-orders" / "PROD-1.json").write_text(json.dumps({
        "PasportData": {"Manufacturer_inn": "7701234567"}
    }))

    result = workflow.create_cis_information_change_from_emission(
        "PROD-1",
        "productionDate",
        "2026-08-01",
    )

    assert result == "PROD-1-production-date"
    task = json.loads((tmp_path / "tasks" / f"{result}.json").read_text())
    assert task["participantInn"] == "7701234567"
    assert task["codes"][0]["code"] == [_short_code("AAA")]


def test_payload_rejects_mixed_date_documents():
    with pytest.raises(ValueError, match="не может одновременно"):
        workflow._validate_cis_information_change_payload({
            "participantInn": "7701234567",
            "codes": [
                {"code": [_short_code("AAA")], "productionDate": "2026-08-01"},
                {"code": [_short_code("BBB")], "expirationDate": "2027-08-01"},
            ],
        })


def test_payload_rejects_both_dates_for_same_codes():
    with pytest.raises(ValueError, match="ровно один вид даты"):
        workflow._validate_cis_information_change_payload({
            "participantInn": "7701234567",
            "codes": [{
                "code": [_short_code("AAA")],
                "productionDate": "2026-08-01",
                "expirationDate": "2027-08-01",
            }],
        })


def test_sign_and_send_uses_manual_cis_information_change_wrapper(tmp_path, monkeypatch):
    config = _config(tmp_path)
    monkeypatch.setattr(workflow, "load_config", lambda name: config)
    task_id = workflow.create_cis_information_change_task(
        "TASK-1",
        "7701234567",
        [_code("AAA")],
        "productionDate",
        "2026-08-01",
    )

    fixed_uuid = uuid.UUID("12345678-1234-5678-1234-567812345678")
    monkeypatch.setattr(workflow.uuid, "uuid4", lambda: fixed_uuid)
    sign_dir = tmp_path / "sign"
    sign_dir.mkdir()
    signature_name = (
        f"7701234567_{fixed_uuid}_cis_information_change.json.sig"
    )
    (sign_dir / signature_name).write_text("BASE64-SIGNATURE")

    token_processor = MagicMock()
    token_processor.get_token_value_by_inn.return_value = "JWT"
    monkeypatch.setattr(workflow, "OrganizationManager", MagicMock())
    monkeypatch.setattr(workflow, "TokenProcessor", MagicMock(return_value=token_processor))
    api = MagicMock()
    api.documents_create.return_value = {"document_id": "DOC-1"}
    monkeypatch.setattr(workflow, "HonestSignAPI", MagicMock(return_value=api))

    result = workflow.sign_and_send_cis_information_change(
        task_id,
        "chemistry",
        str(sign_dir),
        1,
    )

    assert result["document_id"] == "DOC-1"
    wrapped_json, = api.documents_create.call_args.args
    wrapper = json.loads(wrapped_json)
    assert api.documents_create.call_args.kwargs == {"pg": "chemistry"}
    assert wrapper["document_format"] == "MANUAL"
    assert wrapper["type"] == "CIS_INFORMATION_CHANGE"
    assert wrapper["signature"] == "BASE64-SIGNATURE"
    decoded = json.loads(base64.b64decode(wrapper["product_document"]))
    assert decoded["codes"][0]["productionDate"] == "2026-08-01"
    assert (tmp_path / "receipts" / "TASK-1.json").exists()


def test_update_status_uses_receipt_group_and_persists_result(tmp_path, monkeypatch):
    config = _config(tmp_path)
    monkeypatch.setattr(workflow, "load_config", lambda name: config)
    workflow.create_cis_information_change_task(
        "TASK-STATUS",
        "7701234567",
        [_code("AAA")],
        "expirationDate",
        "2027-08-01",
    )
    receipts = tmp_path / "receipts"
    receipts.mkdir()
    (receipts / "TASK-STATUS.json").write_text(json.dumps({
        "document_id": "DOC-STATUS",
        "productGroup": "chemistry",
    }))

    token_processor = MagicMock()
    token_processor.get_token_value_by_inn.return_value = "JWT"
    monkeypatch.setattr(workflow, "OrganizationManager", MagicMock())
    monkeypatch.setattr(workflow, "TokenProcessor", MagicMock(return_value=token_processor))
    api = MagicMock()
    api.doc.return_value = {"status": "CHECKED_OK"}
    monkeypatch.setattr(workflow, "HonestSignAPI", MagicMock(return_value=api))

    result = workflow.update_cis_information_change_status("TASK-STATUS")

    assert result == {"status": "CHECKED_OK"}
    api.doc.assert_called_once_with("DOC-STATUS", pg="chemistry")
    status_path = tmp_path / "statuses" / "TASK-STATUS.json"
    assert json.loads(status_path.read_text()) == {"status": "CHECKED_OK"}
    tags_path = tmp_path / "statuses" / "TASK-STATUS.json.tags"
    assert json.loads(tags_path.read_text()) == {"status": "CHECKED_OK"}


def test_create_normalizes_full_marking_codes_to_identification_codes(tmp_path, monkeypatch):
    config = _config(tmp_path)
    monkeypatch.setattr(workflow, "load_config", lambda name: config)

    task_id = workflow.create_cis_information_change_task(
        "TASK-NORMALIZED",
        "7701234567",
        [_code("AAA")],
        "productionDate",
        "2026-08-01",
    )

    task = json.loads((tmp_path / "tasks" / f"{task_id}.json").read_text())
    assert task["codes"][0]["code"] == [_short_code("AAA")]


def test_create_rejects_duplicates_after_crypto_tail_normalization(tmp_path, monkeypatch):
    config = _config(tmp_path)
    monkeypatch.setattr(workflow, "load_config", lambda name: config)
    short_code = _short_code("AAA")

    with pytest.raises(ValueError, match="должны быть уникальны"):
        workflow.create_cis_information_change_task(
            "TASK-DUPLICATE",
            "7701234567",
            [f"{short_code}\u001d93FIRST", f"{short_code}\u001d93SECOND"],
            "expirationDate",
            "2029-06-01",
        )
