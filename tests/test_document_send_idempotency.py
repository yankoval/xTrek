import json

import pytest

from xtrek import create_emission_task_sample as workflow


class FakeReceiptStorage:
    def __init__(self, objects=None, acquire_result=True):
        self.objects = dict(objects or {})
        self.acquire_result = acquire_result
        self.acquired = []
        self.released = []

    def exists(self, path):
        return path in self.objects

    def read_text(self, path):
        return self.objects[path]

    def acquire_lock(self, path, content=""):
        self.acquired.append((path, json.loads(content)))
        return self.acquire_result

    def release_lock(self, path):
        self.released.append(path)
        return path


def test_claim_reuses_existing_receipt_without_acquiring_lock(monkeypatch):
    receipts_path = "s3://internal/introduceReceipts"
    receipt_path = f"{receipts_path}/ORDER-1.json"
    receipt = {
        "document_id": "DOC-1",
        "productionOrderId": "V-ORDER-1",
    }
    storage = FakeReceiptStorage({receipt_path: json.dumps(receipt)})
    monkeypatch.setattr(workflow, "get_storage", lambda path, config: storage)

    result, claimed_storage, claimed_receipt_path, lock_path = (
        workflow._claim_document_submission(
            receipts_path,
            {},
            "introduce",
            "ORDER-1",
        )
    )

    assert result == receipt
    assert claimed_storage is storage
    assert claimed_receipt_path == receipt_path
    assert lock_path is None
    assert storage.acquired == []


def test_claim_uses_atomic_lock_outside_watched_receipt_prefix(monkeypatch):
    storage = FakeReceiptStorage()
    monkeypatch.setattr(workflow, "get_storage", lambda path, config: storage)

    result, _, receipt_path, lock_path = workflow._claim_document_submission(
        "s3://internal/aggReceipts",
        {},
        "aggregation",
        "T-1",
    )

    assert result is None
    assert receipt_path == "s3://internal/aggReceipts/T-1.json"
    assert lock_path == (
        "s3://internal/documentSubmissionLocks/aggregation/T-1.lock"
    )
    assert storage.acquired[0][0] == lock_path
    assert storage.acquired[0][1]["operation"] == "aggregation"
    assert storage.acquired[0][1]["taskId"] == "T-1"


def test_claim_fails_closed_when_another_worker_holds_lock(monkeypatch):
    storage = FakeReceiptStorage(acquire_result=False)
    monkeypatch.setattr(workflow, "get_storage", lambda path, config: storage)

    with pytest.raises(RuntimeError, match="уже выполняется другим воркером"):
        workflow._claim_document_submission(
            "s3://internal/aggSetReceipts",
            {},
            "aggregation-set",
            "T-1",
        )

    assert storage.released == []


def test_claim_fails_closed_for_malformed_existing_receipt(monkeypatch):
    receipts_path = "s3://internal/aggReceipts"
    receipt_path = f"{receipts_path}/T-1.json"
    storage = FakeReceiptStorage({receipt_path: json.dumps({"error": "bad"})})
    monkeypatch.setattr(workflow, "get_storage", lambda path, config: storage)

    with pytest.raises(RuntimeError, match="Повторная отправка заблокирована"):
        workflow._claim_document_submission(
            receipts_path,
            {},
            "aggregation",
            "T-1",
        )

    assert storage.acquired == []


@pytest.mark.parametrize(
    ("function_name", "identifier", "config", "args", "receipts_path"),
    [
        (
            "sign_and_send_introduce",
            "ORDER-1",
            {
                "introduce-tasks": "s3://internal/introduceTasks",
                "introduce-receipts": "s3://internal/introduceReceipts",
                "s3_config": {},
            },
            ("ORDER-1", "chemistry", "s3://internal/sign", 120),
            "s3://internal/introduceReceipts",
        ),
        (
            "sign_and_send_aggregation_set",
            "T-1",
            {
                "agg_set_tasks": "s3://internal/aggSetTasks",
                "agg_set_receipts": "s3://internal/aggSetReceipts",
                "s3_config": {},
            },
            ("T-1", "chemistry", "s3://internal/sign", 120),
            "s3://internal/aggSetReceipts",
        ),
        (
            "sign_and_send_aggregation",
            "T-1",
            {
                "agg-tasks": "s3://internal/aggTasks",
                "agg-receipts": "s3://internal/aggReceipts",
                "s3_config": {},
            },
            ("T-1", "chemistry", "s3://internal/sign", 120),
            "s3://internal/aggReceipts",
        ),
    ],
)
def test_send_functions_skip_before_signing_when_receipt_exists(
    monkeypatch,
    function_name,
    identifier,
    config,
    args,
    receipts_path,
):
    receipt_path = f"{receipts_path}/{identifier}.json"
    receipt = {
        "document_id": "ALREADY-SENT-DOC",
        "productionOrderId": identifier,
    }
    storage = FakeReceiptStorage({receipt_path: json.dumps(receipt)})
    storage_requests = []

    monkeypatch.setattr(workflow, "load_config", lambda name: config)

    def get_storage(path, s3_config):
        storage_requests.append(path)
        assert path == receipts_path
        return storage

    monkeypatch.setattr(workflow, "get_storage", get_storage)

    result = getattr(workflow, function_name)(*args)

    assert result == receipt
    assert storage_requests == [receipts_path]
    assert storage.acquired == []
