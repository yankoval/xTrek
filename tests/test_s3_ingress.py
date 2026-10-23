import concurrent.futures
import importlib.util
import json
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch
import pytest
from xtrek.s3_ingress import Intake, authenticate, signature
from xtrek.s3_poller import EventLedger, INTERNAL_BUCKET, INPUT_BUCKET, S3EventPoller, event_identity

NOW = datetime(2026, 10, 23, 12, 1, 0, 123000, tzinfo=timezone.utc)
CUTOFF = datetime(2026, 10, 23, 12, tzinfo=timezone.utc)
KEY = 'equipment-reports/Отчёт +%20.json'
ITEM = {'bucket': INTERNAL_BUCKET, 'key': KEY}

class S3:
    def __init__(self):
        self.obj = dict(Key=KEY, ETag='"abc"', Size=17, LastModified=NOW)
        self.calls = []
    def list_objects_v2(self, **kw):
        self.calls.append(kw)
        return {'Contents': [dict(self.obj)], 'IsTruncated': False}

@pytest.fixture
def intake(tmp_path):
    path = str(tmp_path / 'outbox.sqlite3')
    EventLedger(path).close()
    env = {'XTREK_S3_POLL_LEDGER': path, 'XTREK_LOCAL_ALLOWED_INNS': '7733154124',
           'XTREK_S3_CUTOFF_UTC': CUTOFF.isoformat(),
           'XTREK_S3_INGRESS_ROUTES_JSON': json.dumps({INTERNAL_BUCKET: ['equipment-reports/']})}
    return Intake(S3(), env)

def test_signed_request_cannot_be_modified_or_replayed_after_expiry():
    secret, body, ts = b'x' * 40, b'{}', '1792760000'
    sig = signature(secret, ts, body)
    assert authenticate(secret, ts, sig, body, int(ts))
    for timestamp, signature_value, payload, now in [(ts,sig,body+b' ',int(ts)),(ts,sig,body,int(ts)+301),(ts,sig,body,int(ts)-301),('bad',sig,body,int(ts)),(ts,'bad',body,int(ts))]:
        assert not authenticate(secret, timestamp, signature_value, payload, now)

def test_response_loss_and_scanner_share_one_published_row(intake):
    assert intake.receive([ITEM])['registered'] == 1
    ledger = EventLedger(intake.db_path)
    eid = event_identity(INTERNAL_BUCKET, KEY, intake.s3.obj, ())
    assert ledger.claim()[0] == eid
    ledger.published(eid)
    assert intake.receive([ITEM])['known'] == 1
    scanner = S3EventPoller(intake.s3, ledger, CUTOFF, routed_prefixes={INTERNAL_BUCKET: ('equipment-reports/',)})
    assert scanner.scan(max_pages=1) == 0
    assert list(ledger.db.execute('SELECT state FROM s3_outbox')) == [('published',)]
    ledger.close()

def test_scanner_first_then_overwrite(intake):
    ledger = EventLedger(intake.db_path)
    scanner = S3EventPoller(intake.s3, ledger, CUTOFF, routed_prefixes={INTERNAL_BUCKET: ('equipment-reports/',)})
    assert scanner.scan(max_pages=1) == 1
    assert intake.receive([ITEM])['known'] == 1
    intake.s3.obj['ETag'] = '"changed"'
    assert intake.receive([ITEM])['registered'] == 1
    assert ledger.db.execute('SELECT count(*) FROM s3_outbox').fetchone()[0] == 2
    ledger.close()

def test_parallel_intakes_commit_once(intake):
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(lambda _: intake.receive([ITEM]), range(8)))
    assert sum(r['registered'] for r in results) == 1

def test_routes_and_exact_keys_fail_closed(intake):
    assert intake.receive([dict(ITEM, bucket='outside')])['ignored'] == 1
    assert not intake.s3.calls
    intake.s3.obj['Key'] += '.sibling'
    with pytest.raises(RuntimeError): intake.receive([ITEM])

def test_partial_batch_is_durable_when_second_object_unavailable(intake):
    with pytest.raises(RuntimeError):
        intake.receive([ITEM, dict(ITEM, key='equipment-reports/missing.json')])
    assert intake.receive([ITEM])['known'] == 1

def test_dynamic_cutoff_and_explicit_backfill(intake, tmp_path):
    policy = tmp_path / 'policy.env'
    policy.write_text('XTREK_S3_CUTOFF_UTC="2026-10-24T00:00:00Z"\n')
    intake.env['XTREK_S3_ADMISSION_ENV_FILE'] = str(policy)
    assert intake.receive([ITEM])['ignored'] == 1
    allow = tmp_path / 'allow.jsonl'; allow.write_text(json.dumps(ITEM)+'\n')
    intake.env['XTREK_S3_BACKFILL_ALLOWLIST'] = str(allow)
    assert intake.receive([ITEM])['registered'] == 1
    policy.write_text('XTREK_S3_CUTOFF_UTC=2026-10-24T00:00:00\n')
    with pytest.raises(ValueError): intake.receive([ITEM])

def test_input_signer_admission_is_shared(intake):
    intake.routes = {INPUT_BUCKET: ['Задания/']}
    intake.s3.obj['Key'] = 'Задания/test.json'
    with patch('xtrek.s3_ingress.source_inn_allowed', return_value=False) as admit:
        assert intake.receive([{'bucket':INPUT_BUCKET,'key':'Задания/test.json'}])['ignored'] == 1
        assert admit.call_args.kwargs['allowed_inns'] == frozenset({'7733154124'})

def adapter():
    path = Path(__file__).resolve().parents[1] / 'deploy/cloud/s3-notifier/index.py'
    spec = importlib.util.spec_from_file_location('notifier', path)
    mod = importlib.util.module_from_spec(spec); spec.loader.exec_module(mod)
    return mod

def test_adapter_preserves_key_and_rejects_ambiguous_ack(monkeypatch):
    mod = adapter()
    for k,v in {'XTREK_S3_INGRESS_URL':'https://example.invalid/xtrek/s3-events','XTREK_S3_INGRESS_SECRET':'x'*40,'XTREK_S3_INGRESS_ROUTES_JSON':json.dumps({INTERNAL_BUCKET:['equipment-reports/']})}.items():monkeypatch.setenv(k,v)
    requests=[]
    class Response:
        status=202
        def __enter__(self):return self
        def __exit__(self,*a):pass
        def read(self,n):return b'{"registered":1,"known":0,"ignored":0}'
    class Opener:
        def open(self,request,timeout):requests.append(request);return Response()
    monkeypatch.setattr(mod.urllib.request,'build_opener',lambda *a:Opener())
    event={'messages':[{'event_metadata':{'event_type':'yandex.cloud.events.storage.ObjectCreate'},'details':{'bucket_id':INTERNAL_BUCKET,'object_id':KEY}}]}
    assert mod.handler(event,None)['statusCode']==200
    req=requests[0]
    assert json.loads(req.data)['events']==[ITEM]
    assert authenticate(b'x'*40,req.get_header('X-xtrek-timestamp'),req.get_header('X-xtrek-signature'),req.data)
    Response.status=200
    with pytest.raises(RuntimeError):mod.handler(event,None)


def test_http_registration_ack_and_authentication(intake):
    import threading
    import urllib.error
    import urllib.request
    from http.server import ThreadingHTTPServer
    from xtrek.s3_ingress import create_handler
    body=json.dumps({'events':[ITEM]},ensure_ascii=False).encode()
    secret=b'x'*40
    server=ThreadingHTTPServer(('127.0.0.1',0),create_handler(intake,secret))
    thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
    try:
        url='http://127.0.0.1:'+str(server.server_port)+'/xtrek/s3-events'
        stamp=str(int(__import__('time').time()))
        headers={'Content-Type':'application/json','X-Xtrek-Timestamp':stamp,'X-Xtrek-Signature':signature(secret,stamp,body)}
        request=urllib.request.Request(url,body,headers=headers)
        with urllib.request.urlopen(request,timeout=3) as response:
            assert response.status==202
            assert json.load(response)['registered']==1
        with urllib.request.urlopen(request,timeout=3) as response:
            assert json.load(response)['known']==1
        bad=urllib.request.Request(url,body+b' ',headers=headers)
        with pytest.raises(urllib.error.HTTPError) as error:
            urllib.request.urlopen(bad,timeout=3)
        assert error.value.code==401
        intake.s3.obj['Key']+='missing'
        with pytest.raises(urllib.error.HTTPError) as error:
            urllib.request.urlopen(request,timeout=3)
        assert error.value.code==503
    finally:
        server.shutdown();server.server_close();thread.join(timeout=3)
