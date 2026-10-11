"""Offline safety tests for the handoff notifier. All HTTP/S3 calls are mocked."""
from datetime import datetime, timezone
import io
import json
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch

from xtrek.task_notifier import NotifierSettings, TASK_BUCKET, TaskNotifier


KEY = "equipment-tasks/T-00000000-0000-4000-8000-000000000001.json"
TASK_ID = Path(KEY).stem
CHAT = "-72448994566325"
URL = f"https://storage.yandexcloud.net/{TASK_BUCKET}/{KEY}?X-Amz-Signature=fake"


class Body(io.BytesIO):
    pass


class FakeS3:
    def __init__(self):
        self.etag, self.version = "etag-1", "version-1"
        self.last_modified = datetime.fromtimestamp(1_700_000_000, timezone.utc)

    def head_object(self, **kwargs):
        return {"ETag": f'"{self.etag}"', "VersionId": self.version, "LastModified": self.last_modified}

    def get_object(self, **kwargs):
        return {"ETag": f'"{self.etag}"', "VersionId": self.version,
                "Body": Body(json.dumps({"id": TASK_ID}).encode())}

    def generate_presigned_url(self, *_args, **_kwargs):
        return URL


class Response:
    def __init__(self, status=200, data=None):
        self.status_code, self.data = status, data or {}

    def json(self):
        return self.data

    def close(self):
        pass


class LocalNotifierTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.s3 = FakeS3()
        self.history = []
        self.post_count = 0
        self.settings = NotifierSettings("test-token", CHAT, "test-ca", Path(self.tmp.name) / "ledger.sqlite3")
        self.notifier = TaskNotifier(s3=self.s3, settings=self.settings)

    def get(self, url, **kwargs):
        if url == URL:
            return Response(206)
        if url.endswith("/me"):
            return Response(data={"user_id": "bot-1"})
        if url.endswith("/messages"):
            return Response(data={"messages": list(self.history)})
        self.fail(f"Unexpected GET URL: {url}")

    def post(self, *_args, **_kwargs):
        self.post_count += 1
        self.history = [{"timestamp": 1_800_000_000_000,
            "sender": {"is_bot": True, "user_id": "bot-1"},
            "recipient": {"chat_id": CHAT},
            "body": {"mid": "mid-1", "text": f"{TASK_ID} {URL}"}}]
        return Response(data={"message": {"body": {"mid": "mid-1"}}})

    def invoke(self, notifier=None):
        return (notifier or self.notifier).process_object(bucket_id=TASK_BUCKET, object_key=KEY,
                                                          event_etag=self.s3.etag, event_version_id=self.s3.version)

    @patch("xtrek.task_notifier.requests.get")
    @patch("xtrek.task_notifier.requests.post")
    def test_two_senders_only_one_post(self, post, get):
        get.side_effect, post.side_effect = self.get, self.post
        second = TaskNotifier(s3=self.s3, settings=self.settings)
        results=[]
        threads=[threading.Thread(target=lambda n=n: results.append(self.invoke(n))) for n in (self.notifier, second)]
        [t.start() for t in threads]; [t.join() for t in threads]
        self.assertEqual(self.post_count, 1)
        self.assertEqual(sum(r.state == "delivered" for r in results), 2)

    @patch("xtrek.task_notifier.requests.get")
    @patch("xtrek.task_notifier.requests.post")
    def test_crash_before_post_recovers_get_only(self, post, get):
        get.side_effect, post.side_effect = self.get, self.post
        row=self.notifier._ensure_row(TASK_BUCKET,KEY,self.s3.etag,self.s3.version,1_700_000_000_000)
        self.notifier._transition(TASK_BUCKET,TASK_ID,("discovered",),"sending",detail="simulated crash")
        result=self.invoke()
        self.assertEqual(result.state,"uncertain")
        post.assert_not_called()

    @patch("xtrek.task_notifier.requests.get")
    @patch("xtrek.task_notifier.requests.post")
    def test_crash_after_post_reconciles_without_retry(self, post, get):
        get.side_effect, post.side_effect = self.get, self.post
        self.notifier._ensure_row(TASK_BUCKET,KEY,self.s3.etag,self.s3.version,1_700_000_000_000)
        self.notifier._transition(TASK_BUCKET,TASK_ID,("discovered",),"sending",detail="simulated post completed")
        self.post()
        result=self.invoke()
        self.assertEqual(result.state,"delivered")
        self.assertEqual(result.message_id,"mid-1")
        post.assert_not_called()

    @patch("xtrek.task_notifier.requests.get")
    @patch("xtrek.task_notifier.requests.post")
    def test_incomplete_history_defers_without_post(self, post, get):
        get.side_effect, post.side_effect = self.get, self.post
        self.notifier.settings = NotifierSettings("test-token",CHAT,"test-ca",self.settings.ledger_path,1)
        self.history=[{"timestamp":1_800_000_000_000,"body":{}} for _ in range(100)]
        result=self.invoke()
        self.assertEqual(result.state,"deferred")
        post.assert_not_called()

    @patch("xtrek.task_notifier.requests.get")
    @patch("xtrek.task_notifier.requests.post")
    def test_history_is_complete_at_configured_outage_cutoff(self, post, get):
        get.side_effect, post.side_effect = self.get, self.post
        self.notifier.settings = NotifierSettings(
            "test-token", CHAT, "test-ca", self.settings.ledger_path, 2,
            history_cutoff_ms=1_800_000_000_000)
        self.history = [{"timestamp": 1_800_000_000_000, "body": {"text": "older message"}}
                        for _ in range(100)]
        result = self.invoke()
        self.assertEqual(result.state, "delivered")
        self.assertEqual(self.post_count, 1)

    @patch("xtrek.task_notifier.requests.get")
    @patch("xtrek.task_notifier.requests.post")
    def test_overwrite_keeps_logical_delivery_and_does_not_resend(self, post, get):
        get.side_effect, post.side_effect = self.get, self.post
        self.assertEqual(self.invoke().state,"delivered")
        self.s3.etag, self.s3.version = "etag-2", "version-2"
        result=self.invoke()
        self.assertEqual(result.state,"delivered")
        self.assertEqual(self.post_count,1)

    @patch("xtrek.task_notifier.requests.get")
    @patch("xtrek.task_notifier.requests.post")
    def test_uncertain_history_is_found_after_object_overwrite(self, post, get):
        get.side_effect, post.side_effect = self.get, self.post
        self.notifier._ensure_row(TASK_BUCKET,KEY,self.s3.etag,self.s3.version,1_700_000_000_000)
        self.notifier._transition(TASK_BUCKET,TASK_ID,("discovered",),"sending",detail="prior uncertain POST")
        # The object now has a new physical version; task identity and message
        # history remain the same logical task, so reconcile instead of resend.
        self.s3.etag, self.s3.version = "etag-overwritten", "version-overwritten"
        self.post()
        result=self.invoke()
        self.assertEqual(result.state,"delivered")
        self.assertEqual(result.message_id,"mid-1")
        post.assert_not_called()


if __name__ == "__main__":
    unittest.main()
