from datetime import datetime, timezone
from pathlib import Path
import pytest
from xtrek.pallet_label_celery_runtime import SnapshotStorage, read_snapshot, validate_source, deliver_pallet_report

UID = '079e8478-6667-4e8d-a3be-179a9de423eb'
RID = 'T-test-' + UID


def settings():
    return {'backlog_report_ids': [], 'backlog_source_uuids': [UID], 'source_cutoff_utc': '2026-10-11T13:43:46+00:00', 'allowed_inns': ['7733154124']}


def source(quantity=5):
    return {'Quantity': quantity, 'PasportData': {'Manufacturer_inn': '7733154124'}}


def test_disabled_route_has_no_network(monkeypatch):
    monkeypatch.delenv('XTREK_PALLET_DELIVERY_CONFIG', raising=False)
    assert deliver_pallet_report(report_key='equipment-reports/x.json', runtime_config={}) == {'state': 'DISABLED'}


def test_exact_backlog_source_allowed():
    validate_source(RID, source(), {'LastModified': datetime(2020, 1, 1, tzinfo=timezone.utc)}, settings())


@pytest.mark.parametrize('quantity', ['', 0, None])
def test_blank_or_nonpositive_never_prints(quantity):
    with pytest.raises(ValueError, match='notification only'):
        validate_source(RID, source(quantity), {}, settings())


def test_protected_uuid_never_delivered():
    with pytest.raises(ValueError, match='Protected'):
        validate_source('T-623c20fa-f90c-4478-aeae-9c6635cedc9f', source(), {}, settings())


def test_old_source_outside_backlog_is_held():
    s=settings();s['backlog_source_uuids']=[]
    with pytest.raises(ValueError, match='Historical'):
        validate_source(RID, source(), {'LastModified': datetime(2020, 1, 1, tzinfo=timezone.utc)}, s)


def test_fresh_source_allowed():
    s=settings();s['backlog_source_uuids']=[]
    validate_source(RID, source(), {'LastModified': datetime(2026, 10, 11, 14, tzinfo=timezone.utc)}, s)


def test_unconfigured_inn_held():
    x=source();x['PasportData']['Manufacturer_inn']='unknown'
    with pytest.raises(ValueError, match='INN'):
        validate_source(RID, x, {}, settings())


def test_snapshot_read_uses_etag():
    import io
    class Client:
        def head_object(self, **kw):return {'ETag': 'exact'}
        def get_object(self, **kw):
            assert kw['IfMatch']=='exact'
            return {'Body':io.BytesIO(b'{"id":"exact"}')}
    assert read_snapshot(Client(), 'bucket', 'key')[0]['id']=='exact'


def test_snapshot_storage_has_only_frozen_join():
    x=SnapshotStorage({'a':{'id':'a'}})
    assert x.exists('a') and not x.exists('b')
    assert '"a"' in x.read_text('a')


def test_celery_delivery_calls_notifier_and_caches_png(tmp_path, monkeypatch):
    import json
    import boto3
    import requests
    import xtrek.pallet_label_celery_runtime as runtime
    from xtrek.pallet_label_max_notifier import LabelMessage, DeliveryLedger
    cfg=settings();cfg.update(enabled=True, chat_id='-72448994566325', bot_user_id='259171708',
        task_bucket='tasks', report_bucket='reports', max_environment_file=str(tmp_path/'max.env'),
        ledger_path=str(tmp_path/'ledger.sqlite3'), png_root=str(tmp_path/'png'),history_start_ms=0,
        previously_delivered_report_ids=[])
    (tmp_path/'max.env').write_text('MAX_CHAT_ID=-72448994566325\nMAX_BOT_TOKEN=fixture\nMAX_CA_FILE=fixture.pem\n')
    (tmp_path/'config.json').write_text(json.dumps(cfg))
    DeliveryLedger.initialize(cfg['ledger_path'])
    monkeypatch.setattr(boto3,'client',lambda *a,**kw:object())
    monkeypatch.setattr(requests,'Session',lambda:type('Session',(),{})())
    monkeypatch.setattr(runtime,'read_snapshot',lambda client,bucket,key:(source() if key.startswith('Задания/') else {},{}))
    item=LabelMessage(RID,'346070517900000057','pallet_1_346070517900000057.png','fixture',b'\x89PNG\r\n\x1a\nfixture','source-hash')
    monkeypatch.setattr(runtime,'prepare_report_label_messages',lambda **kw:[item])
    calls=[]
    class Notifier:
        def __init__(self,**kw):
            assert kw['chat_id']=='-72448994566325'
            assert kw['expected_bot_user_id']=='259171708'
        def _verify_bot(self):pass
        def _history(self):return []
        def deliver(self,label):calls.append(label);return {'state':'CONFIRMED','message_id':'mid.fixture'}
    monkeypatch.setattr(runtime,'MaxPalletLabelNotifier',Notifier)
    out=runtime.deliver_pallet_report(report_key='equipment-reports/'+RID+'.json',runtime_config={'s3_config':{}},delivery_config_path=str(tmp_path/'config.json'))
    assert out['state']=='DELIVERED' and len(calls)==1
    assert (tmp_path/'png'/RID/item.file_name).read_bytes()==item.png_bytes
