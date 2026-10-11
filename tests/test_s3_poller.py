from datetime import datetime, timezone

import pytest

from xtrek.s3_poller import EventLedger, S3EventPoller, event_identity, publish_pending


def _obj(etag="abc", modified=None):
    return {"Key": "Задания/order.json", "ETag": f'"{etag}"', "Size": 12,
            "LastModified": modified or datetime(2026, 1, 1, tzinfo=timezone.utc)}


def test_event_identity_tracks_overwrite_not_tag_only_change():
    obj = _obj()
    first = event_identity("b", obj["Key"], obj, [{"Key": "status", "Value": "new"}])
    assert first != event_identity("b", obj["Key"], _obj("different"), [{"Key": "status", "Value": "new"}])
    assert first == event_identity("b", obj["Key"], obj, [{"Key": "status", "Value": "finished"}])


def test_ledger_recovers_expired_publish_lease(tmp_path):
    ledger = EventLedger(str(tmp_path / "outbox.sqlite3"))
    assert ledger.enqueue("id", "b", "k")
    assert not ledger.enqueue("id", "b", "k")
    assert ledger.claim(now=100, lease_seconds=10) == ("id", '{"bucket": "b", "key": "k"}')
    assert ledger.claim(now=105) is None
    assert ledger.claim(now=111)[0] == "id"
    ledger.close()


class FakePaginator:
    def __init__(self, pages):
        self.pages = pages
        self.calls = []

    def paginate(self, **kwargs):
        self.calls.append(kwargs)
        yield from self.pages


class FakeS3:
    def __init__(self, pages, list_error=None):
        self.paginator = FakePaginator(pages)
        self.list_error = list_error
        self.calls = []
        self.source_objects = {}

    def list_objects_v2(self, **kwargs):
        self.calls.append(kwargs.copy())
        if self.list_error:
            raise self.list_error
        token = kwargs.get("ContinuationToken")
        index = int(token or 0)
        page = dict(self.paginator.pages[index])
        if index + 1 < len(self.paginator.pages):
            page["IsTruncated"] = True
            page["NextContinuationToken"] = str(index + 1)
        else:
            page["IsTruncated"] = False
        return page

    def get_object(self, **kwargs):
        import io
        return {"Body": io.BytesIO(self.source_objects[kwargs["Key"]])}

def test_scan_paginates_and_deduplicates(tmp_path, monkeypatch):
    import xtrek.s3_poller as poller
    monkeypatch.setattr(poller, "ROUTED_PREFIXES", {"b": ("Задания/",)})
    pages = [{"Contents": [_obj()]}, {"Contents": [_obj()]}]
    ledger = EventLedger(str(tmp_path / "ledger.sqlite3"))
    s3 = FakeS3(pages)
    scanner = S3EventPoller(s3, ledger, datetime(2025, 1, 1, tzinfo=timezone.utc))
    assert scanner.scan() == 1
    assert scanner.scan() == 0
    ledger.close()


def test_prefixes_are_scanned_round_robin_with_separate_cursor(tmp_path, monkeypatch):
    import xtrek.s3_poller as poller
    monkeypatch.setattr(poller, "ROUTED_PREFIXES", {"b": ("A/", "B/")})
    pages = [{"Contents": [_obj()]}, {"Contents": [_obj()]}]
    ledger = EventLedger(str(tmp_path / "ledger.sqlite3"))
    s3 = FakeS3(pages)
    scanner = S3EventPoller(s3, ledger, datetime(2025, 1, 1, tzinfo=timezone.utc))
    scanner.scan(max_pages=1)
    scanner.scan(max_pages=1)
    assert [call["Prefix"] for call in s3.calls] == ["A/", "B/"]
    assert ledger.scan_cursor() == 0
    ledger.close()


def test_scan_fails_closed_on_list_error(tmp_path, monkeypatch):
    import xtrek.s3_poller as poller
    monkeypatch.setattr(poller, "ROUTED_PREFIXES", {"b": ("Задания/",)})
    ledger = EventLedger(str(tmp_path / "ledger.sqlite3"))
    scanner = S3EventPoller(FakeS3([{"Contents": [_obj()]}], list_error=RuntimeError("list api down")), ledger,
                             datetime(2025, 1, 1, tzinfo=timezone.utc))
    with pytest.raises(RuntimeError, match="list api down"):
        scanner.scan()
    assert ledger.claim() is None
    ledger.close()


def test_scan_allows_only_configured_local_signer_inn(tmp_path, monkeypatch):
    import json
    import xtrek.s3_poller as poller
    monkeypatch.setattr(poller, "ROUTED_PREFIXES", {poller.INPUT_BUCKET: ("Задания/",)})
    allowed = _obj()
    allowed["Key"] = "Задания/allowed.json"
    denied = dict(allowed, Key="Задания/denied.json", ETag='"other"')
    s3 = FakeS3([{"Contents": [allowed, denied]}])
    s3.source_objects = {
        allowed["Key"]: json.dumps({"PasportData": {"Manufacturer_inn": "7733154124"}}).encode(),
        denied["Key"]: json.dumps({"PasportData": {"Manufacturer_inn": "9723161905"}}).encode(),
    }
    ledger = EventLedger(str(tmp_path / "ledger.sqlite3"))
    scanner = S3EventPoller(s3, ledger, datetime(2025, 1, 1, tzinfo=timezone.utc),
                            allowed_inns=frozenset({"7733154124"}))
    assert scanner.scan() == 1
    row = ledger.claim()
    assert row is not None and json.loads(row[1])["key"] == allowed["Key"]
    ledger.close()


def test_publish_retries_outbox_after_failure(tmp_path):
    ledger = EventLedger(str(tmp_path / "ledger.sqlite3"))
    ledger.enqueue("id", "b", "k")

    class FailingApp:
        def send_task(self, *args, **kwargs):
            raise RuntimeError("broker down")

    with pytest.raises(RuntimeError, match="broker down"):
        publish_pending(ledger, FailingApp())
    row = ledger.db.execute("SELECT state,lease_until,attempts FROM s3_outbox WHERE event_id='id'").fetchone()
    assert row[0] == "pending" and row[1] == 0 and row[2] == 1
    ledger.close()
