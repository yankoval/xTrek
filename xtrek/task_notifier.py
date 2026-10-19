"""Single-owner MAX notifier handoff module; importing it performs no I/O."""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from html import escape, unescape
import fcntl
import json
import logging
import os
from pathlib import Path
import re
import sqlite3
import time
from typing import Any, Iterator
from urllib.parse import unquote, urlparse

try:
    import requests
except ImportError:  # Keep offline tests importable; runtime owner installs requests.
    class _RequestsUnavailable:
        @staticmethod
        def get(*_args, **_kwargs):
            raise RuntimeError("requests package is required by the notifier runtime")
        @staticmethod
        def post(*_args, **_kwargs):
            raise RuntimeError("requests package is required by the notifier runtime")
    requests = _RequestsUnavailable()

TASK_BUCKET = "1bf11148-3595-4a07-a089-d460153b7c7a"
TASK_PREFIX = "equipment-tasks/T-"
MAX_API = "https://platform-api2.max.ru"
LINK_TTL_SECONDS = 48 * 60 * 60
LOG = logging.getLogger("xtrek.task_notifier")


@dataclass(frozen=True)
class NotifierSettings:
    max_bot_token: str = field(repr=False)
    max_chat_id: str
    ca_file: str
    ledger_path: Path = Path("/var/lib/xtrek/max-task-delivery.sqlite3")
    max_history_pages: int = 50
    history_cutoff_ms: int = 0


@dataclass(frozen=True)
class DeliveryResult:
    state: str
    object_key: str
    message_id: str | None = None
    detail: str | None = None


class TaskNotifier:
    """Validate, reconcile and send one equipment-task JSON notification.

    SQLite CAS prevents cross-process duplicate sends. flock serializes calls
    sharing this ledger on one host. A `sending` row is never sent again: the
    next invocation can only reconcile MAX history through GET.
    """

    def __init__(self, *, s3: Any, settings: NotifierSettings):
        self.s3, self.settings = s3, settings
        self._initialize_ledger()

    @staticmethod
    def _now() -> str:
        return datetime.now(timezone.utc).isoformat()

    def _connect(self) -> sqlite3.Connection:
        db = sqlite3.connect(self.settings.ledger_path, timeout=30)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA synchronous=FULL")
        db.execute("PRAGMA busy_timeout=30000")
        return db

    @contextmanager
    def _db(self) -> Iterator[sqlite3.Connection]:
        db = self._connect()
        try:
            yield db
            if db.in_transaction:
                db.commit()
        except Exception:
            if db.in_transaction:
                db.rollback()
            raise
        finally:
            db.close()

    @contextmanager
    def _process_lock(self):
        lock_path = self.settings.ledger_path.with_suffix(self.settings.ledger_path.suffix + ".lock")
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        with lock_path.open("a") as lock:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(lock.fileno(), fcntl.LOCK_UN)

    def _initialize_ledger(self) -> None:
        self.settings.ledger_path.parent.mkdir(parents=True, exist_ok=True)
        with self._db() as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.execute("""CREATE TABLE IF NOT EXISTS logical_deliveries (
                bucket_id TEXT NOT NULL, task_id TEXT NOT NULL, object_key TEXT NOT NULL,
                recipient TEXT NOT NULL, mode TEXT NOT NULL, state TEXT NOT NULL,
                created_at TEXT NOT NULL, updated_at TEXT NOT NULL, first_seen_ms INTEGER NOT NULL,
                etag TEXT, version_id TEXT, message_id TEXT, attempt_count INTEGER NOT NULL DEFAULT 0,
                detail TEXT, PRIMARY KEY(bucket_id,task_id,recipient,mode))""")

    @staticmethod
    def _valid_key(bucket: str, key: str) -> bool:
        if bucket != TASK_BUCKET or not key.startswith(TASK_PREFIX):
            return False
        name = key[len(TASK_PREFIX):]
        return name.endswith(".json") and len(name) > 5 and "/" not in name

    @staticmethod
    def _etag(head: dict[str, Any]) -> str:
        return str(head.get("ETag", "")).strip('"')

    @staticmethod
    def _version_id(head: dict[str, Any]) -> str | None:
        value = head.get("VersionId")
        return str(value) if value not in (None, "", "null") else None

    def _row(self, bucket: str, task_id: str) -> sqlite3.Row | None:
        with self._db() as db:
            return db.execute("SELECT * FROM logical_deliveries WHERE bucket_id=? AND task_id=? AND recipient=? AND mode='json-link'",
                              (bucket, task_id, str(self.settings.max_chat_id))).fetchone()

    def _ensure_row(self, bucket: str, key: str, etag: str, version_id: str | None, first_seen_ms: int) -> sqlite3.Row:
        task_id, now = Path(key).stem, self._now()
        with self._db() as db:
            db.execute("BEGIN IMMEDIATE")
            db.execute("""INSERT OR IGNORE INTO logical_deliveries
                (bucket_id,task_id,object_key,recipient,mode,state,created_at,updated_at,first_seen_ms,etag,version_id)
                VALUES (?,?,?,?,'json-link','discovered',?,?,?,?,?)""",
                (bucket,task_id,key,str(self.settings.max_chat_id),now,now,first_seen_ms,etag,version_id))
            # Keep the first observation time and update provenance on overwrite.
            db.execute("""UPDATE logical_deliveries SET object_key=?, etag=?, version_id=?, updated_at=?
                WHERE bucket_id=? AND task_id=? AND recipient=? AND mode='json-link'
                AND state IN ('discovered','deferred','failed_before_send')""",
                (key,etag,version_id,now,bucket,task_id,str(self.settings.max_chat_id)))
            row=db.execute("SELECT * FROM logical_deliveries WHERE bucket_id=? AND task_id=? AND recipient=? AND mode='json-link'",
                           (bucket,task_id,str(self.settings.max_chat_id))).fetchone()
            db.commit()
        return row

    def _transition(self, bucket: str, task_id: str, expected: tuple[str,...], state: str,
                    *, etag: str | None = None, version_id: str | None = None,
                    message_id: str | None = None, detail: str | None = None,
                    attempted: bool = False) -> bool:
        marks=','.join('?' for _ in expected)
        with self._db() as db:
            db.execute("BEGIN IMMEDIATE")
            cur=db.execute(f"""UPDATE logical_deliveries SET state=?,updated_at=?,etag=COALESCE(?,etag),
                version_id=COALESCE(?,version_id),message_id=COALESCE(?,message_id),detail=?,
                attempt_count=attempt_count+? WHERE bucket_id=? AND task_id=? AND recipient=?
                AND mode='json-link' AND state IN ({marks})""",
                (state,self._now(),etag,version_id,message_id,detail,int(attempted),bucket,task_id,
                 str(self.settings.max_chat_id),*expected))
            db.commit()
            return cur.rowcount == 1

    def _get_json(self, path: str, params: dict[str, Any] | None = None) -> tuple[int,dict[str,Any]]:
        response=requests.get(f"{MAX_API}/{path}",params=params,
            headers={"Authorization":self.settings.max_bot_token},timeout=15,verify=self.settings.ca_file)
        try: body=response.json()
        except ValueError: body={}
        return response.status_code, body if isinstance(body,dict) else {}

    @staticmethod
    def _strings(value: Any):
        if isinstance(value,str): yield unescape(value)
        elif isinstance(value,dict):
            for item in value.values(): yield from TaskNotifier._strings(item)
        elif isinstance(value,list):
            for item in value: yield from TaskNotifier._strings(item)

    @staticmethod
    def _exact_object_link(values: list[str], bucket: str, key: str) -> bool:
        expected_path="/"+key; path_style="/"+bucket+expected_path
        for value in values:
            for link in re.findall(r"https?://[^\s<>\"']+",unquote(value)):
                p=urlparse(link.rstrip(").,;")); host=(p.hostname or "").lower()
                if p.scheme=="https" and ((host=="storage.yandexcloud.net" and p.path==path_style) or
                    (host==f"{bucket}.storage.yandexcloud.net" and p.path==expected_path)):
                    return True
        return False

    def _reconcile(self, *, task_id: str, bucket: str, key: str, created_ms: int) -> tuple[bool,str|None,bool]:
        """Scan all available history; fail closed if the configured page cap is hit."""
        status,me=self._get_json("me"); user_id=me.get("user_id")
        if status!=200 or user_id is None: return False,None,False
        cursor=None; previous_oldest=None
        for _ in range(self.settings.max_history_pages):
            params={"chat_id":self.settings.max_chat_id,"count":100}
            if cursor is not None: params["from"]=cursor
            status,result=self._get_json("messages",params)
            if status!=200: return False,None,False
            messages=result.get("messages",[])
            for message in messages:
                stamp=int(message.get("timestamp",0)); body=message.get("body",{})
                values=list(self._strings(body)); content="\n".join(unquote(v) for v in values)
                if task_id not in content: continue
                sender=message.get("sender",{}); recipient=message.get("recipient",{})
                if not (sender.get("is_bot") and str(sender.get("user_id"))==str(user_id)
                        and str(recipient.get("chat_id"))==str(self.settings.max_chat_id)):
                    continue
                mid=body.get("mid")
                if self._exact_object_link(values,bucket,key): return True,str(mid) if mid else None,not bool(mid)
                return True,None,True
            stamps=[int(m.get("timestamp",0)) for m in messages if m.get("timestamp") is not None]
            if len(messages)<100 or (stamps and min(stamps)<=self.settings.history_cutoff_ms):
                return True,None,False
            if not stamps: return False,None,False
            oldest=min(stamps)
            if oldest==previous_oldest: return False,None,False
            previous_oldest=oldest; cursor=oldest-1
        return False,None,False

    def _verify_link(self, url: str) -> bool:
        # Never log or return the signed URL. A one-byte range proves the URL
        # currently resolves without downloading the full task.
        response=requests.get(url,headers={"Range":"bytes=0-0"},timeout=20,stream=True)
        try: return response.status_code in (200,206)
        finally: response.close()

    def _read_pinned(self, bucket: str, key: str, head: dict[str,Any], version_id: str | None):
        params={"Bucket":bucket,"Key":key}
        if version_id: params["VersionId"]=version_id
        else: params["IfMatch"]=head["ETag"]
        response=self.s3.get_object(**params)
        try:
            meta=response.get("ResponseMetadata",{}).get("HTTPHeaders",{})
            got_etag=str(meta.get("etag",response.get("ETag", ""))).strip('"')
            got_version=response.get("VersionId")
            if not got_etag or got_etag != self._etag(head): raise RuntimeError("GET ETag missing or differs from HEAD")
            if version_id and str(got_version or "") != version_id: raise RuntimeError("GET VersionId missing or differs from requested version")
            return json.loads(response["Body"].read())
        finally: response["Body"].close()

    def process_object(self, *, bucket_id: str, object_key: str, event_etag: str|None=None,
                       event_version_id: str|None=None) -> DeliveryResult:
        if not self._valid_key(bucket_id,object_key): raise ValueError("Object is outside equipment-task JSON route")
        task_id=Path(object_key).stem
        with self._process_lock():
            hp={"Bucket":bucket_id,"Key":object_key}
            if event_version_id: hp["VersionId"]=event_version_id
            head=self.s3.head_object(**hp); etag=self._etag(head); version_id=self._version_id(head)
            if event_etag and event_etag.strip('"')!=etag: return DeliveryResult("stale_event",object_key,detail="event ETag differs from selected object version")
            first_ms=int(head["LastModified"].timestamp()*1000)
            row=self._ensure_row(bucket_id,object_key,etag,version_id,first_ms)
            state=row["state"]
            if state=="delivered": return DeliveryResult("delivered",object_key,row["message_id"],"logical task already delivered")
            if state in ("sending","uncertain"):
                complete,mid,conflict=self._reconcile(task_id=task_id,bucket=bucket_id,key=object_key,created_ms=row["first_seen_ms"])
                if complete and mid:
                    self._transition(bucket_id,task_id,("sending","uncertain"),"delivered",message_id=mid,detail="reconciled by MAX GET")
                    return DeliveryResult("delivered",object_key,mid,"reconciled by GET")
                detail="conflicting task message requires review" if conflict else "uncertain POST; no automatic retry"
                self._transition(bucket_id,task_id,("sending","uncertain"),"uncertain",detail=detail)
                return DeliveryResult("uncertain",object_key,detail=detail)
            try:
                task=self._read_pinned(bucket_id,object_key,head,version_id)
                if not isinstance(task,dict) or task.get("id")!=task_id: raise ValueError("S3 task ID does not match object name")
                complete,mid,conflict=self._reconcile(task_id=task_id,bucket=bucket_id,key=object_key,created_ms=row["first_seen_ms"])
                if not complete:
                    self._transition(bucket_id,task_id,("discovered","deferred","failed_before_send"),"deferred",detail="MAX history incomplete")
                    return DeliveryResult("deferred",object_key,detail="MAX history incomplete")
                if conflict:
                    self._transition(bucket_id,task_id,("discovered","deferred","failed_before_send"),"uncertain",detail="task ID found with different link")
                    return DeliveryResult("uncertain",object_key,detail="task ID found with different link")
                if mid:
                    self._transition(bucket_id,task_id,("discovered","deferred","failed_before_send"),"delivered",message_id=mid,detail="matched history")
                    return DeliveryResult("delivered",object_key,mid,"matched existing MAX message")
                params={"Bucket":bucket_id,"Key":object_key,"ResponseContentDisposition":"attachment"}
                if version_id: params["VersionId"]=version_id
                url=self.s3.generate_presigned_url("get_object",Params=params,ExpiresIn=LINK_TTL_SECONDS)
                if not self._verify_link(url): raise RuntimeError("presigned JSON link failed GET verification")
                # Close the HEAD/POST race for unversioned buckets, and assert
                # the selected typed version/ETag still describes the object.
                latest=self.s3.head_object(Bucket=bucket_id,Key=object_key)
                if self._etag(latest)!=etag or self._version_id(latest)!=version_id:
                    raise RuntimeError("object overwritten before send")
                text=(f'<a href="{escape(url,quote=True)}">'
                      f'Задание: {escape(Path(object_key).name)}</a>')
            except Exception as exc:
                self._transition(bucket_id,task_id,("discovered","deferred","failed_before_send"),"failed_before_send",detail=type(exc).__name__)
                return DeliveryResult("failed_before_send",object_key,detail=type(exc).__name__)
            # Atomic CAS is the authorization to POST. Any losing invocation
            # sees `sending` and performs GET reconciliation only.
            if not self._transition(bucket_id,task_id,("discovered","deferred","failed_before_send"),"sending",
                                    etag=etag,version_id=version_id,detail="POST started",attempted=True):
                current=self._row(bucket_id,task_id)
                return DeliveryResult(current["state"] if current else "uncertain",object_key,detail="CAS denied POST")
            try:
                response=requests.post(f"{MAX_API}/messages",params={"chat_id":self.settings.max_chat_id,"disable_link_preview":"true"},
                    headers={"Authorization":self.settings.max_bot_token},json={"text":text,"notify":True,"format":"html"},
                    timeout=20,verify=self.settings.ca_file)
                try: result=response.json()
                except ValueError: result={}
                message=result.get("message",{}) if isinstance(result,dict) else {}; body=message.get("body",{}) if isinstance(message,dict) else {}
                mid=body.get("mid") if isinstance(body,dict) else None
            except Exception as exc:
                detail=f"POST outcome unknown ({type(exc).__name__}); GET reconciliation required"
            else:
                detail=f"MAX response HTTP {response.status_code}; GET reconciliation required"
            # A 200/message id is not proof of a usable link: confirm exact
            # sender, chat, task ID and object path through MAX GET history.
            complete,matched_mid,conflict=self._reconcile(task_id=task_id,bucket=bucket_id,key=object_key,created_ms=row["first_seen_ms"])
            if complete and matched_mid:
                self._transition(bucket_id,task_id,("sending",),"delivered",message_id=matched_mid,detail="exact link confirmed by MAX GET")
                return DeliveryResult("delivered",object_key,matched_mid)
            if conflict: detail="MAX history contains task ID with a different link; manual review required"
            self._transition(bucket_id,task_id,("sending",),"uncertain",message_id=mid if isinstance(mid,str) else None,detail=detail)
            return DeliveryResult("uncertain",object_key,detail=detail)


def _utc_setting(name: str) -> datetime:
    value=os.environ.get(name)
    if not value:
        raise SystemExit(f"Set {name}; automatic historical replay is disabled")
    result=datetime.fromisoformat(value.replace("Z","+00:00"))
    if result.tzinfo is None:
        raise SystemExit(f"{name} must include a timezone")
    return result.astimezone(timezone.utc)


def scan_and_deliver(notifier: TaskNotifier, *, s3: Any, cutoff_utc: datetime,
                     allowlist: frozenset[str]=frozenset(), max_pages: int=100) -> int:
    """Reconcile only fresh or explicitly allowlisted equipment-task JSONs."""
    token=None; processed=0; already_delivered=0; states: dict[str,int]={}
    for _ in range(max_pages):
        request={"Bucket":TASK_BUCKET,"Prefix":TASK_PREFIX,"MaxKeys":1000}
        if token: request["ContinuationToken"]=token
        page=s3.list_objects_v2(**request)
        for obj in page.get("Contents",[]):
            key=obj.get("Key","")
            if not key.startswith(TASK_PREFIX) or key.endswith("/") or "/" in key[len(TASK_PREFIX):]:
                continue
            modified=obj.get("LastModified")
            if isinstance(modified,str): modified=datetime.fromisoformat(modified.replace("Z","+00:00"))
            if key not in allowlist and (modified is None or modified.astimezone(timezone.utc)<cutoff_utc):
                continue
            try:
                result=notifier.process_object(bucket_id=TASK_BUCKET,object_key=key,
                    event_etag=obj.get("ETag"),event_version_id=obj.get("VersionId"))
            except Exception as exc:
                LOG.error("Notifier failed before result: key=%s error=%s",key,type(exc).__name__)
                continue
            processed+=1
            states[result.state]=states.get(result.state,0)+1
            if result.state=="delivered" and result.detail=="logical task already delivered":
                already_delivered+=1
                LOG.debug("Task notification already delivered: key=%s",key)
            else:
                log=LOG.info if result.state=="delivered" else LOG.warning
                log("Task notification: key=%s state=%s message_id=%s",
                    key,result.state,result.message_id or "")
        if not page.get("IsTruncated"):
            break
        token=page.get("NextContinuationToken")
        if not token:
            raise RuntimeError("S3 returned a truncated page without a continuation token")
    else:
        LOG.warning("Notifier scan page cap reached; remaining objects will be reconciled next cycle")
    LOG.info("Notifier scan complete: candidate_objects=%d already_delivered=%d states=%s",
             processed,already_delivered,json.dumps(states,sort_keys=True))
    return processed


def main():
    if os.environ.get("XTREK_LOCAL_S3_POLLING")!="1" or os.environ.get("XTREK_LOCAL_NOTIFIER_ENABLED")!="1":
        raise SystemExit("Enable both XTREK_LOCAL_S3_POLLING=1 and XTREK_LOCAL_NOTIFIER_ENABLED=1")
    import boto3
    from botocore.config import Config
    endpoint=os.environ.get("XTREK_S3_ENDPOINT_URL","https://storage.yandexcloud.net")
    access=os.environ.get("XTREK_S3_ACCESS_KEY"); secret=os.environ.get("XTREK_S3_SECRET_KEY")
    token=os.environ.get("MAX_BOT_TOKEN"); chat=os.environ.get("MAX_CHAT_ID")
    ca=os.environ.get("MAX_CA_FILE")
    if not all((access,secret,token,chat,ca)):
        raise SystemExit("S3, MAX token/chat and MAX_CA_FILE settings are required")
    task_cutoff=_utc_setting("XTREK_MAX_TASK_CUTOFF_UTC")
    history_cutoff=_utc_setting("XTREK_MAX_HISTORY_CUTOFF_UTC")
    ledger=Path(os.environ.get("XTREK_MAX_TASK_DELIVERY_LEDGER","/var/lib/xtrek/task-notifier/max-task-delivery.sqlite3"))
    settings=NotifierSettings(token,chat,ca,ledger,
        max_history_pages=max(1,int(os.environ.get("XTREK_MAX_HISTORY_PAGES","50"))),
        history_cutoff_ms=int(history_cutoff.timestamp()*1000))
    allowlist_path=os.environ.get("XTREK_MAX_TASK_ALLOWLIST")
    allowlist=frozenset()
    if allowlist_path:
        with open(allowlist_path,encoding="utf-8") as f:
            allowlist=frozenset(line.strip() for line in f if line.strip() and not line.startswith("#"))
    s3=boto3.client("s3",endpoint_url=endpoint,region_name=os.environ.get("XTREK_S3_REGION","ru-central1"),
        aws_access_key_id=access,aws_secret_access_key=secret,config=Config(retries={"max_attempts":2}))
    notifier=TaskNotifier(s3=s3,settings=settings)
    interval=max(10,int(os.environ.get("XTREK_MAX_NOTIFIER_SECONDS","20")))
    while True:
        try:
            scan_and_deliver(notifier,s3=s3,cutoff_utc=task_cutoff,allowlist=allowlist,
                max_pages=max(1,int(os.environ.get("XTREK_MAX_NOTIFIER_SCAN_PAGES","100"))))
        except Exception as exc:
            LOG.error("Notifier scan failed: %s",type(exc).__name__)
        time.sleep(interval)


if __name__=="__main__":
    logging.basicConfig(level=os.environ.get("LOG_LEVEL","INFO"))
    main()
