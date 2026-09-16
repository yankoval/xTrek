"""Offline contract checks against a supplied cf checkout; never invoke live functions."""
import argparse
import base64
import importlib.util
import io
import json
import os
from pathlib import Path
import socket
import sys
from unittest.mock import MagicMock, patch


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--cf-root', type=Path, required=True)
    parser.add_argument('--triggers', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    checks = []

    def blocked(*a, **kw):
        raise AssertionError('Network prohibited in contract checks')

    def check(name, condition):
        assert condition, name
        checks.append(name)

    with patch.object(socket.socket, 'connect', blocked), patch.object(socket.socket, 'connect_ex', blocked):
        bridge = load('cf_bridge_contract', args.cf_root / 'bucket-to-queue-message-trigger/index.py')
        sqs = MagicMock()
        bridge.sqs_client = sqs
        os.environ.update(QUEUE_URL='https://example.test/queue', CELERY_TASK_NAME='tasks.process_s3_event',
                          CELERY_ROUTING_KEY='queue_task_create_1C', YMQ_ACCESS_KEY='synthetic',
                          YMQ_SECRET_KEY='synthetic', YMQ_QUEUE_URL='https://example.test/queue')
        from xtrek import config_loader, tokens
        with patch.object(config_loader, 'load_config', return_value={'input_bucket':'input', 'internal_bucket':'internal'}):
            tasks = load('xtrek._cf_contract_tasks', Path(tokens.__file__).with_name('tasks.py'))
        dispatch = {
            'emissionOrders/': 'logic_sign_emission', 'emissionReceipts/': 'logic_update_emission',
            'emissions/': 'logic_get_emission_kodes', 'kodes/': 'logic_kodes',
            'utilisationReceipts/': 'logic_utilisationReceipt', 'introduceReceipts/': 'logic_update_introduce',
            'equipment-reports/': 'logic_start_equipment_reports', 'productionOrders/': 'logic_start_virtualProdTask_emission',
            'aggReceipts/': 'logic_update_agg', 'aggSetReceipts/': 'logic_update_agg_set',
        }
        mocks = {name: MagicMock(return_value='contract-ok') for name in dispatch.values()}
        for name, mock in mocks.items():
            assert hasattr(tasks, name), name
            setattr(tasks, name, mock)
        envelopes = []
        for prefix, function in dispatch.items():
            key = prefix + 'synthetic.json'
            bridge.handler({'messages':[{'details':{'bucket_id':'internal','object_id':key}}]}, None)
            envelope = json.loads(sqs.send_message.call_args.kwargs['MessageBody'])
            arguments, kwargs, embed = json.loads(base64.b64decode(envelope['body']))
            check('celery_v2_' + prefix, envelope['headers']['task'] == tasks.process_s3_event.name
                  and envelope['properties']['delivery_info']['routing_key'] == 'queue_task_create_1C')
            tasks.process_s3_event.run(*arguments, **kwargs)
            mocks[function].assert_called_once_with('internal/' + key)
            envelopes.append(envelope)
        for key in ['tokens-v2.json', 'token-pilots/run/registry-v2.json', 'tokens.json.master.lock',
                    'sign/1234567890_random_dataToSign.txt', 'sign/1234567890_random_dataToSign.json.sig']:
            before = sum(m.call_count for m in mocks.values())
            result = tasks.process_s3_event.run({'bucket':'internal','key':key})
            check('router_ignores_' + key, result == 'Skipped: No match' and sum(m.call_count for m in mocks.values()) == before)
        triggers = json.loads(args.triggers.read_text())
        active = [t['rule']['object_storage'] for t in triggers if t.get('status') == 'ACTIVE' and 'object_storage' in t.get('rule',{})]
        bucket = '20ab2a0c-2726-4ba1-9c7c-7deae82941ff'
        def matching(key):
            return [r for r in active if r['bucket_id']==bucket and key.startswith(r.get('prefix','')) and key.endswith(r.get('suffix',''))]
        for extension in ['txt','json']:
            key = 'sign/1234567890_random_dataToSign.' + extension
            check('one_sign_trigger_' + extension, len(matching(key)) == 1)
            check('no_signature_recursion_' + extension, not matching(key+'.sig'))
        for key in ['tokens-v2.json','token-pilots/run/registry-v2.json','tokens.json.master.lock']:
            check('no_live_trigger_' + key, not matching(key))
        # Demonstrate the existing delivery failure, without sending a queue message.
        sqs.send_message.side_effect = RuntimeError('synthetic queue unavailable')
        failure = bridge.handler({'messages':[{'details':{'bucket_id':'internal','object_id':'emissions/test.json'}}]}, None)
        agg = load('cf_agg_contract', args.cf_root / 'task-agg-worker/index.py')
        storage = MagicMock()
        storage.get_object_tagging.return_value = {'TagSet':[]}
        payload = {'id':'test','gtin':'04600000000000','numРacksInBox':2,'boxLabelFields':{'title':'Test'}}
        storage.get_object.return_value = {'Body':io.BytesIO(json.dumps(payload).encode())}
        storage.generate_presigned_url.return_value = 'https://example.test/put'
        with patch.object(agg,'get_s3_client',return_value=storage), patch.dict(os.environ,{'BUCKET':'output','FILTER_PREFIX':'equipment-tasks/'}):
            agg.handler({'messages':[{'details':{'bucket_id':'input','object_id':'equipment-tasks/test.json'}}]},None)
        exported=json.loads(storage.put_object.call_args.kwargs['Body'])
        check('aggregation_fields_preserved',all(exported[k]==v for k,v in payload.items()))
        check('report_upload_link_added',exported['task-export-signed-link']=='https://example.test/put')
        storage.reset_mock()
        with patch.object(agg,'get_s3_client',return_value=storage), patch.dict(os.environ,{'FILTER_PREFIX':'T-GB'}):
            agg.handler({'messages':[{'details':{'bucket_id':'input','object_id':'upAggXtrakTasks/T-GB-1.json'}}]},None)
        findings={'ingress_queue_error_returns_200':failure['statusCode']==200,
                  'paused_aggregation_prefix_mismatch':not storage.get_object.called}
        from celery.signals import task_prerun,task_postrun
        task_prerun.disconnect(tasks._start_token_snapshot)
        task_postrun.disconnect(tasks._finish_token_snapshot)
        tasks.app.close()
    args.output.write_text(json.dumps({'passed':checks,'findings':findings,'sample_celery_envelope':envelopes[0]},indent=2))
    print(json.dumps({'passed':len(checks),'findings':findings}))


if __name__=='__main__': main()
