"""Run only from a freshly isolated copy on yc-worker, never from a production release.

Approved scope: True API UUID for 9723161905, read-only API checks, private registry,
filesystem Celery queue. No SUZ issuance, business documents, or service changes.
"""
import base64
import hashlib
import json
import math
import os
from pathlib import Path
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parent
INN = '9723161905'
BUCKET = '20ab2a0c-2726-4ba1-9c7c-7deae82941ff'
SOURCE = Path('/home/ivankiselev1975/Scripts/Testing/tokens_config.json')
CON = '7018ae41-2e60-4105-9b30-b1f2502a92c8'
OMS = '3cf9c20a-74ed-4463-b480-332e7c0caed4'
BASE = 'https://markirovka.crpt.ru/api/v3/true-api'


def emit(stage, **data):
    print(json.dumps(dict(stage=stage, **data)), flush=True)


def main():
    assert ROOT.parent == Path('/tmp') and ROOT.name.startswith('xtrek-uuid-pilot-')
    os.umask(0o077)
    os.chdir(ROOT)
    # Credentials stay on VPS. Do not inherit an unrelated production broker/config.
    for key in list(os.environ):
        if key.startswith('YMQ_') or key in {'TOKENS_CONFIG', 'suz_worker_config', 'token_config',
                                           'CELERY_BROKER_URL', 'CELERY_RESULT_BACKEND'}:
            os.environ.pop(key, None)
    import boto3
    import requests
    from xtrek.token_registry import TokenRegistry, migrate_legacy_record
    source = json.loads(SOURCE.read_text())
    assert source['tokens_path'] == f's3://{BUCKET}/tokens.json'
    assert source['sign'] == f's3://{BUCKET}/sign/'
    client = boto3.client('s3', **source['s3_config'])
    prefix = 'token-pilots/' + ROOT.name + '/'
    key = prefix + 'registry-v2.json'
    config = {'s3_config': source['s3_config'], 'sign': source['sign'], 'SIGNING_TIMEOUT': 120,
              'tokens_registry_path': f's3://{BUCKET}/{key}',
              'tokens_master_lock_path': f's3://{BUCKET}/{prefix}master.lock',
              'tokens_environment': 'production', 'tokens_allowed_inns': [INN],
              'tokens_purposes': ['true_api'], 'true_api_token_format': 'UUID',
              'tokens_refresh_before_expiry_seconds': 1800, 'suz_ping_sign': True,
              'tokens_read_only': True}
    for directory in ['cache', 'messages', 'control', 'results', 'xtrek/my_orgs']:
        (ROOT / directory).mkdir(parents=True, exist_ok=True)
    config_path = ROOT / 'tokens_config.json'
    config_path.write_text(json.dumps(config))
    os.environ['TOKENS_CONFIG'] = str(config_path)
    orgs = []

    def walk(value):
        if isinstance(value, dict):
            if str(value.get('inn')) == INN:
                orgs.append(value)
            else:
                for item in value.values(): walk(item)
        elif isinstance(value, list):
            for item in value: walk(item)

    for page in client.get_paginator('list_objects_v2').paginate(Bucket=BUCKET, Prefix='firm/'):
        for obj in page.get('Contents', []):
            if obj['Key'].endswith('.json'):
                walk(json.loads(client.get_object(Bucket=BUCKET, Key=obj['Key'])['Body'].read()))
    assert len(orgs) == 1 and orgs[0]['connection_id'] == CON and orgs[0]['oms_id'] == OMS
    (ROOT / 'xtrek/my_orgs/organization.json').write_text(json.dumps(orgs[0]))
    response = client.get_object(Bucket=BUCKET, Key='tokens.json')
    source_etag = response['ETag']
    legacy = json.loads(response['Body'].read())
    jwt_candidates = []
    suz_candidates = []
    for raw in legacy:
        value = raw.get('Токен', '')
        if value.count('.') == 2:
            claims = json.loads(base64.urlsafe_b64decode(value.split('.')[1] + '==='))
            if str(claims.get('inn')) == INN:
                jwt_candidates.append(migrate_legacy_record(raw, purpose='true_api', environment='production',
                                      inn=INN, issuer_base_url=BASE))
        elif raw.get('Идентификатор') == CON:
            suz_candidates.append(migrate_legacy_record(raw, purpose='suz', environment='production',
                                  inn=INN, issuer_base_url=BASE, connection_id=CON, oms_id=OMS,
                                  legacy_timezone='Europe/Moscow'))
    jwt = max(jwt_candidates, key=lambda r: r.remaining_seconds())
    suz = max(suz_candidates, key=lambda r: r.remaining_seconds())
    assert min(jwt.remaining_seconds(), suz.remaining_seconds()) > 1800
    from xtrek.crpt_auth import verify_token
    from xtrek import tokens, token_worker
    from uuid_pilot_app import app, read_balance
    assert 'IfNoneMatch' in client.meta.service_model.operation_model('PutObject').input_shape.members
    tokens.home_dir = ROOT / 'cache'
    original_issue = token_worker.issue_token

    def guarded_issue(inn, **kwargs):
        assert inn == INN and kwargs['purpose'] == 'true_api' and kwargs['token_format'] == 'UUID'
        return original_issue(inn, **kwargs)

    token_worker.issue_token = guarded_issue
    result = {'inn': INN, 'run_dir': str(ROOT), 'registry_key': key, 'source_etag_before': source_etag,
              'production_pid_before': subprocess.check_output(['systemctl', 'show', 'celery.service', '-p', 'MainPID', '--value'], text=True).strip()}
    worker = None
    seeded = False
    log = None
    try:
        emit('baseline_checks')
        with requests.Session() as session:
            session.trust_env = False
            verify_token(jwt, session, config)
            result['old_jwt_before'] = 200
            verify_token(suz, session, config)
            result['signed_suz_ping_before'] = 200
        emit('baseline_ok')
        # Conditional create: never replace an existing pilot or shared object.
        client.put_object(Bucket=BUCKET, Key=key, Body=json.dumps(TokenRegistry([jwt, suz]).to_dict()).encode(), IfNoneMatch='*')
        seeded = True
        emit('issuing_first_uuid')
        assert token_worker.TokenRefreshWorker().check_and_refresh(), 'First master cycle failed'
        log = (ROOT / 'worker.log').open('w')
        worker = subprocess.Popen([sys.executable, '-m', 'celery', '-A', 'uuid_pilot_app:app', 'worker',
                                   '--pool=solo', '--concurrency=1', '--without-gossip', '--without-mingle',
                                   '--without-heartbeat', '--loglevel=WARNING', '-Q', ROOT.name,
                                   '--hostname=' + ROOT.name + '@%h'], cwd=ROOT, stdout=log, stderr=subprocess.STDOUT)
        result['worker_pid'] = worker.pid
        first = read_balance.delay().get(timeout=75, interval=1)
        result['first'] = first
        emit('first_worker_read', **first)
        selected = tokens.TokenProcessor(org_manager=token_worker.TokenRefreshWorker().org_manager).get_token_record(INN, 'true_api')
        assert selected.token and selected.expireDate == first['expireDate']
        # Isolated master threshold only; server expireDate is never edited.
        config['tokens_refresh_before_expiry_seconds'] = math.ceil(selected.remaining_seconds()) + 30
        result['forced_refresh_threshold_seconds'] = config['tokens_refresh_before_expiry_seconds']
        config_path.write_text(json.dumps(config))
        emit('refreshing_uuid', threshold=result['forced_refresh_threshold_seconds'])
        assert token_worker.TokenRefreshWorker().check_and_refresh(), 'Refresh cycle failed'
        assert worker.poll() is None
        second = read_balance.delay().get(timeout=75, interval=1)
        result['second'] = second
        emit('second_worker_read', **second)
        assert first['pid'] == second['pid'] == worker.pid
        assert first['token_sha256'] != second['token_sha256']
        assert first['suz_sha256'] == second['suz_sha256']
        config['tokens_refresh_before_expiry_seconds'] = 1800
        config_path.write_text(json.dumps(config))
        before = client.head_object(Bucket=BUCKET, Key=key)['ETag']
        assert token_worker.TokenRefreshWorker().check_and_refresh()
        result['fresh_cycle_no_write'] = before == client.head_object(Bucket=BUCKET, Key=key)['ETag']
        assert result['fresh_cycle_no_write']
        with requests.Session() as session:
            session.trust_env = False
            verify_token(jwt, session, config)
            result['old_jwt_after'] = 200
            verify_token(suz, session, dict(config, suz_ping_sign=False))
            result['suz_ping_after'] = 200
        result['success'] = True
    except Exception as exc:
        result['success'] = False
        result['error_type'] = type(exc).__name__
        emit('failed', error_type=type(exc).__name__)
    finally:
        if worker is not None:
            worker.terminate()
            try: worker.wait(timeout=20)
            except subprocess.TimeoutExpired:
                worker.kill(); worker.wait(timeout=10)
            result['isolated_worker_stopped'] = worker.poll() is not None
        if log: log.close()
        if seeded:
            obj = client.get_object(Bucket=BUCKET, Key=key)
            (ROOT / 'final-registry-private.json').write_bytes(obj['Body'].read())
            client.delete_object(Bucket=BUCKET, Key=key)
            result['pilot_registry_removed'] = True
        result['source_etag_after'] = client.head_object(Bucket=BUCKET, Key='tokens.json')['ETag']
        result['shared_tokens_unchanged'] = result['source_etag_after'] == source_etag
        result['production_pid_after'] = subprocess.check_output(['systemctl', 'show', 'celery.service', '-p', 'MainPID', '--value'], text=True).strip()
        result['production_pid_unchanged'] = result['production_pid_before'] == result['production_pid_after']
        (ROOT / 'result.json').write_text(json.dumps(result, indent=2))
        emit('result', **result)
    return 0 if result.get('success') else 1


if __name__ == '__main__':
    raise SystemExit(main())
