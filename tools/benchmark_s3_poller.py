"""Offline bootstrap/known-page benchmark; no network or business events.

Run with the project venv and --baseline-dir pointing to a clean main export.
RSS numbers are peak bootstrap values, not production sizing predictions.
"""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
python = sys.executable
snippet = r'''
import io,json,os,resource,statistics,sys,tempfile,time
from datetime import datetime,timezone
sys.path.insert(0,sys.argv[1])
from xtrek import s3_poller as p
mode=sys.argv[2]
if mode == 'publish':
    os.environ['XTREK_LOCAL_BROKER_URL']='amqp://fake:fake@127.0.0.1/test-only'
    app=p.create_publisher_app()
    print(json.dumps({'mode':mode,'peak_rss_mib':resource.getrusage(resource.RUSAGE_SELF).ru_maxrss/(1024**2 if sys.platform == 'darwin' else 1024)}))
    raise SystemExit
import boto3
from botocore.config import Config
client=boto3.client('s3',endpoint_url='https://synthetic.invalid',aws_access_key_id='fake',aws_secret_access_key='fake',config=Config(retries={'total_max_attempts':1}))
body=b'{"Quantity":"1","PasportData":{"Manufacturer_inn":"allowed"}}'
objects=[{'Key':f'Задания/{i}.json','ETag':'"etag"','Size':len(body),'LastModified':datetime(2026,1,1,tzinfo=timezone.utc)} for i in range(581)]
class Fake:
    def __init__(self): self.gets=0; self.lists=0
    def list_objects_v2(self,**kw):
        self.lists+=1
        return {'Contents':objects}
    def get_object(self,**kw):
        self.gets+=1
        return {'Body':io.BytesIO(body),'ETag':'"etag"','ContentLength':len(body),'LastModified':objects[0]['LastModified']}
p.ROUTED_PREFIXES={p.INPUT_BUCKET:('Задания/',)}
with tempfile.TemporaryDirectory() as directory:
    ledger=p.EventLedger(directory+'/outbox.db')
    for o in objects: ledger.enqueue(p.event_identity(p.INPUT_BUCKET,o['Key'],o,()),p.INPUT_BUCKET,o['Key'])
    fake=Fake(); scanner=p.S3EventPoller(fake,ledger,datetime(2025,1,1,tzinfo=timezone.utc),allowed_inns=frozenset({'allowed'}))
    times=[]
    for _ in range(20):
        start=time.perf_counter(); scanner.scan(max_pages=1); times.append(time.perf_counter()-start)
    print(json.dumps({'mode':mode,'peak_rss_mib':resource.getrusage(resource.RUSAGE_SELF).ru_maxrss/(1024**2 if sys.platform == 'darwin' else 1024),'median_ms':statistics.median(times)*1000,'get_per_cycle':fake.gets/20,'list_per_cycle':fake.lists/20,'db_bytes':os.path.getsize(str(ledger.path))}))
'''
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--baseline-dir', required=True)
args = parser.parse_args()
root = str(Path(__file__).resolve().parents[1])
for path,mode in [(args.baseline_dir,'baseline-module-only'),(root,'scan'),(root,'publish')]:
    print(subprocess.check_output([python,'-c',snippet,path,mode],text=True).strip())
