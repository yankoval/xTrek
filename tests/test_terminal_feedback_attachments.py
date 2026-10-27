import copy
import hashlib
import json
from unittest.mock import MagicMock

import pytest

from xtrek import terminal_feedback as feedback
from xtrek.terminal_feedback_messages import make_attachment, compact_message, frozen_attachment
from xtrek.terminal_feedback_max import MaxTerminalTransport, deliver, DeliveryUncertain, MAX_API
from xtrek.terminal_feedback_s3 import S3TerminalLedger
from test_terminal_feedback import settings
from test_terminal_feedback_s3 import Store


def facts_for(count):
    return {'version':1,'scan_case':None,'source_sha256':'0'*64,
        'products_total':count*6,'products_checked':count*6,'products_ok':True,
        'issues':[{'kind':'alreadyregistered','count':count,'items':[
            {'kind':'box','id':f'{i:018d}','position':i,'state':'INTRODUCED','contents':{'EMITTED':6}}
            for i in range(1,count+1)]}]}


def preview(count):
    return {'event_id':'e'*64,'production_order_id':'T-test','category':'report_aggregate_conflict',
            'facts_json':json.dumps(facts_for(count))}


@pytest.mark.parametrize('count,attached', [(1,False),(10,False),(11,True),(3000,True)])
def test_strict_threshold_and_every_number_in_file(count, attached):
    row=preview(count)
    file=make_attachment(row)
    assert bool(file)==attached
    if file:
        assert file['box_count']==count
        assert hashlib.sha256(file['text'].encode()).hexdigest()==file['sha256']
        assert len(file['text'].encode())==file['size']
        for i in range(1,count+1):
            assert f'{i:018d}' in file['text']
        assert 'Полный список — в приложенном файле' in compact_message(row)
    else:
        assert 'приложенном файле' not in compact_message(row)


def test_count_physical_boxes_not_error_occurrences_or_ready_boxes():
    facts=facts_for(10)
    facts['issues'].append(dict(copy.deepcopy(facts['issues'][0]),kind='duplicateaggregation'))
    row=preview(10);row['facts_json']=json.dumps(facts)
    assert make_attachment(row) is None
    facts['products_ok']=False
    facts['issues'].append({'kind':'readybox','count':20,'items':[
        {'kind':'box','id':f'{i:018d}','position':i} for i in range(11,31)]})
    row['facts_json']=json.dumps(facts)
    assert make_attachment(row) is None
    facts['problem_boxes']=[{'kind':'box','id':f'{i:018d}','position':i} for i in range(1,12)]
    row['facts_json']=json.dumps(facts)
    assert make_attachment(row)['box_count']==11


def prepared(tmp_path,settings,backend='sqlite'):
    ledger=(feedback.TerminalLedger(settings['ledger_path']) if backend=='sqlite'
            else S3TerminalLedger(Store(str(tmp_path/'s3.db')),'s3://bucket/events'))
    row=ledger.record('bucket','equipment-reports/T.json','report-validation','report_aggregate_conflict',facts=facts_for(11))
    return ledger,ledger.bind_route(row['event_id'],'T','Alice',settings['routes']['line-a'])


@pytest.mark.parametrize('backend',['sqlite','s3'])
def test_attachment_and_body_freeze_together(tmp_path,settings,backend):
    ledger,row=prepared(tmp_path,settings,backend)
    file=frozen_attachment(row)
    assert file['box_count']==11
    ledger.record('bucket','equipment-reports/T.json','report-validation','invalid_report',facts=facts_for(12))
    changed=ledger.bind_route(row['event_id'],'changed','Bob',settings['routes']['line-b'])
    assert changed==row
    assert ledger.claim_post(row['event_id'])
    saved=ledger.get(row['event_id'])
    assert saved['attachment_json']==row['attachment_json']
    with pytest.raises(ValueError,match='checksum'):
        frozen_attachment(dict(saved,attachment_json=json.dumps(dict(file,text=file['text']+'changed'))))


@pytest.mark.parametrize('failure',[None,'upload','message'])
def test_one_message_with_file_and_no_retry_after_uncertain_upload_or_post(tmp_path,settings,monkeypatch,failure):
    ledger,row=prepared(tmp_path,settings)
    session=MagicMock();calls=[]
    def post(url,**kwargs):
        assert ledger.get(row['event_id'])['delivery_state']=='POST_STARTED'
        calls.append((url,kwargs))
        response=MagicMock()
        if url==MAX_API+'/uploads':
            assert kwargs['params']=={'type':'file'}
            response.json.return_value={'url':'https://fu.oneme.ru/upload'}
        elif url=='https://fu.oneme.ru/upload':
            assert kwargs['headers']=={'Authorization':None}
            name,data,mime=kwargs['files']['data'];file=frozen_attachment(row)
            assert name==file['filename'] and hashlib.sha256(data).hexdigest()==file['sha256']
            if failure=='upload': raise TimeoutError('secret')
            response.json.return_value={'token':'file-token'}
        else:
            assert url==MAX_API+'/messages'
            assert kwargs['json']=={'text':row['message_text'],'notify':True,
                'attachments':[{'type':'file','payload':{'token':'file-token'}}]}
            if failure=='message': raise TimeoutError('secret')
        return response
    session.post.side_effect=post
    transport=MaxTerminalTransport(session=session,token='bot-secret',route=settings['routes']['line-a'],created_at=row['created_at'])
    transport.find=MagicMock(return_value=None)
    monkeypatch.setattr('xtrek.terminal_feedback_max.time.sleep',lambda _:None)
    assert deliver(ledger,row,transport)=='GET_RECONCILIATION_REQUIRED'
    first=len(calls)
    assert deliver(ledger,row,transport)=='GET_RECONCILIATION_REQUIRED'
    assert len(calls)==first
    assert sum(url==MAX_API+'/messages' for url,_ in calls)==(0 if failure=='upload' else 1)
    transport.find.return_value='verified-file-mid'
    assert deliver(ledger,row,transport)=='CONFIRMED'


@pytest.mark.parametrize('wrong',[None,'absent','name','bytes','url','truncated','oversize'])
def test_get_requires_downloadable_exact_file_not_just_caption(tmp_path,settings,wrong):
    ledger,row=prepared(tmp_path,settings)
    file=frozen_attachment(row)
    item={'type':'file','filename':file['filename'],'payload':{'url':'https://fu.oneme.ru/file'}}
    message={'sender':{'user_id':'bot'},'recipient':{'chat_id':'chat-a'},
             'body':{'text':row['message_text'],'mid':'mid','attachments':[item]}}
    if wrong=='absent':message['body']['attachments']=[]
    if wrong=='name':item['filename']='different.txt'
    if wrong=='url':item['payload']['url']='https://example.org/file'
    data=file['text'].encode()
    if wrong=='bytes':data=b'x'*len(data)
    if wrong=='truncated':data=data[:-1]
    if wrong=='oversize':data+=b'x'
    session=MagicMock();response=MagicMock(status_code=200)
    response.iter_content.return_value=[data];session.get.return_value=response
    transport=MaxTerminalTransport(session=session,token='secret',route=settings['routes']['line-a'],created_at=row['created_at'])
    transport.reader._verify_bot=MagicMock();transport.reader._history=MagicMock(return_value=iter([message]))
    if wrong:
        with pytest.raises(DeliveryUncertain):transport.find(row)
    else:
        assert transport.find(row)=='mid'
        assert session.get.call_args.kwargs['headers']=={'Authorization':None}
    session.post.assert_not_called()


def test_rejected_product_codes_are_mapped_to_all_problem_boxes(tmp_path):
    from xtrek.utils import AggregationAnalyzer
    codes=[f'010461011762137921{i:013d}' for i in range(11)]
    boxes=[{'boxNumber':f'{i+1:018d}','productNumbersFull':[c]} for i,c in enumerate(codes)]
    path=tmp_path/'report.json';path.write_text(json.dumps({'readyBox':boxes}))
    api,nk=MagicMock(),MagicMock();nk.product_info.return_value={'isSet':True}
    api.get_list_cis_info.return_value=[{'cisInfo':{'cis':c,'status':'RETIRED'}} for c in codes]
    result=AggregationAnalyzer(api,nk).check_report(str(path))
    row=dict(preview(11),category='report_status_rejected',facts_json=json.dumps(result.feedback_facts))
    file=make_attachment(row)
    assert file['box_count']==11
    for i,c in enumerate(codes):
        assert c in file['text'] and f'{i+1:018d}' in file['text']


@pytest.mark.parametrize('backend', ['sqlite', 's3'])
def test_old_frozen_message_does_not_gain_attachment(tmp_path, settings, backend):
    ledger, row = prepared(tmp_path, settings, backend)
    legacy_text = 'Previously frozen message'
    if backend == 'sqlite':
        with ledger.connect() as db:
            db.execute('UPDATE terminal_events SET message_text=?', (legacy_text,))
            db.execute('ALTER TABLE terminal_events DROP COLUMN attachment_json')
        ledger = feedback.TerminalLedger(settings['ledger_path'])
    else:
        def old_format(event):
            event.pop('attachment_json')
            event['message_text'] = legacy_text
            return event
        ledger._update(row['event_id'], old_format)
    rebound = ledger.bind_route(row['event_id'], 'T', 'Alice', settings['routes']['line-a'])
    assert rebound['message_text'] == legacy_text
    assert frozen_attachment(rebound) is None


def test_duplicate_sscc_at_eleven_positions_means_eleven_problem_boxes():
    row = preview(11)
    facts = facts_for(11)
    for item in facts['issues'][0]['items']:
        item['id'] = '000000000000000001'
    row['facts_json'] = json.dumps(facts)
    assert make_attachment(row)['box_count'] == 11
