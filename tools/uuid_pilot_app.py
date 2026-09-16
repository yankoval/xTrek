"""Isolated read-only Celery task for the approved one-organization UUID pilot."""
import hashlib
import os
from pathlib import Path
import requests
from celery import Celery

ROOT = Path(__file__).resolve().parent
os.environ['TOKENS_CONFIG'] = str(ROOT / 'tokens_config.json')
# Import actual task-boundary handlers; the production broker is never started.
os.environ['YMQ_ACCESS_KEY'] = 'isolated-unused'
os.environ['YMQ_SECRET_KEY'] = 'isolated-unused'
os.environ['YMQ_QUEUE_URL'] = 'https://example.invalid/unused'
from xtrek.tasks import _start_token_snapshot, _finish_token_snapshot
from xtrek import tokens
from xtrek.org_manager import OrganizationManager

tokens.home_dir = ROOT / 'cache'
app = Celery('uuid-isolated-pilot', broker='filesystem://', backend='file://' + str(ROOT / 'results'))
app.conf.update(
    broker_transport_options={'data_folder_in': str(ROOT / 'messages'),
                              'data_folder_out': str(ROOT / 'messages'),
                              'control_folder': str(ROOT / 'control')},
    task_default_queue=ROOT.name, task_serializer='json', result_serializer='json',
    accept_content=['json'], worker_prefetch_multiplier=1, worker_enable_remote_control=False,
)


@app.task(name='uuid_pilot.read_balance')
def read_balance():
    manager = OrganizationManager(str(ROOT / 'xtrek' / 'my_orgs'))
    processor = tokens.TokenProcessor(org_manager=manager)
    inn = '9723161905'
    org = manager.find(inn=inn)
    record = processor.get_token_record(inn, 'true_api')
    if not record or record.format != 'UUID':
        raise RuntimeError('Pilot requires a live True API UUID')
    suz = processor.get_token_record(inn, 'suz', org.connection_id, org.oms_id)
    with requests.Session() as session:
        session.trust_env = False
        response = session.get(record.issuer_base_url + '/elk/product-groups/balance/all',
                               headers={'Authorization': 'Bearer ' + record.token},
                               timeout=(10, 30), allow_redirects=False)
    if response.status_code != 200:
        raise RuntimeError('Pilot balance read failed with HTTP ' + str(response.status_code))
    return {'pid': os.getpid(), 'http': response.status_code, 'purpose': record.purpose,
            'format': record.format, 'expireDate': record.expireDate,
            'token_sha256': hashlib.sha256(record.token.encode()).hexdigest(),
            'suz_sha256': hashlib.sha256(suz.token.encode()).hexdigest(),
            'balance_sha256': hashlib.sha256(response.content).hexdigest()}
