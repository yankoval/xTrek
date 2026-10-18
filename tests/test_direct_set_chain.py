"""In-process synthetic E2E: real preparation/generator/storage, no network/signing."""
import json
import socket
from pathlib import Path
from unittest.mock import MagicMock
from uuid import uuid4
import pytest
from test_tasks_unit_set_flow import import_tasks


@pytest.mark.parametrize("parent_first", [True, False])
def test_direct_set_chain_from_emission(monkeypatch, tmp_path, parent_first):
    from xtrek import create_emission_task_sample as flow, utils, storage
    from xtrek.operation_state import publish_once
    tasks = import_tasks(monkeypatch)
    def deny(*a, **kw):
        raise AssertionError('Real network/signing forbidden in synthetic test')
    monkeypatch.setattr(socket.socket, 'connect', deny)
    monkeypatch.setattr(socket, 'getaddrinfo', deny)
    monkeypatch.chdir(tmp_path)
    config = {k:str(tmp_path/k) for k in ['production_orders_path','kodes','emissions_path',
        'emission_orders_path','emission_receipts','utilisation_tasks_path','utilisation_receipts',
        'utilisation_reports','introduce-tasks','introduce-receipts','introduces',
        'equipment_set_reports','agg_set_tasks','agg_set_receipts','agg_sets','operation_state_path']}
    config.update(product_group_settings={'chemistry':{'alcoholVolume':'0'}}, s3_config=None)
    monkeypatch.setattr(flow, 'load_config', lambda *_:config)
    monkeypatch.setattr(tasks, 'config', config)
    local = storage.LocalStorage()
    class InputStorage:
        def read_text(self, path):return (tmp_path/Path(path).name).read_text()
        def get_tags(self, path):return {}
        def exists(self,path):return (tmp_path/Path(path).name).exists()
    factory=lambda p,*_:InputStorage() if p.startswith('s3://input/') else local
    monkeypatch.setattr('xtrek.operation_state.get_storage',factory);monkeypatch.setattr(storage,'get_storage',factory);monkeypatch.setattr(flow,'get_storage',factory);monkeypatch.setattr(tasks,'get_storage',factory)
    set_gtin='04670404500000';components=[('04670404500017',2),('04670404500024',1)]
    nk=MagicMock()
    def product(g,*a,**kw):
        return {'result':[{'is_set':g==set_gtin,'good_name':'SYNTHETIC '+g,
            'owner_inn':'0000000000','good_attrs':[{'attr_id':13933,'attr_value':'3305900009'}]}]}
    nk.feedProduct.side_effect=product
    nk.get_set_by_gtin.return_value={'result':[{'is_set':True,'good_name':'SYNTHETIC SET',
        'set_gtins':[{'gtin':g,'quantity':q} for g,q in components]}]}
    nk.get_active_permit_documents_by_gtin.return_value=[{'number':'SYNTHETIC','date':'2026-01-01','type':'CONFORMITY_DECLARATION'}]
    monkeypatch.setattr(flow,'NK',lambda **_:nk)
    monkeypatch.setattr(flow,'get_inn_by_gtin',lambda *a,**k:'0000000000')
    monkeypatch.setattr(flow,'OrganizationManager',MagicMock())
    monkeypatch.setattr(flow,'TokenProcessor',MagicMock())
    monkeypatch.setattr(tasks,'set_ready_check',lambda path:utils.set_ready_check(path,api=MagicMock(),nk=nk,config=config))
    monkeypatch.setattr(tasks,'generate_prn_files',lambda *_:True)
    def put(key,identity,data):
        path=config[key]+'/'+identity+'.json';publish_once(local,path,json.dumps(data));return data
    def read(key,identity):return json.loads(Path(config[key],identity+'.json').read_text())
    calls=[]
    def send_util(order,*a,**k):
        read('utilisation_tasks_path',order);calls.append(('utilisation',order));return True
    def status_util(order):
        pid=flow._find_production_order_id_by_suz_order_id(order)
        value=tasks.UtilisationReportStatus(omsId='synthetic',reportId=order,reportStatus='SUCCESS',productionOrderId=pid)
        put('utilisation_reports',order,value.__dict__);return value
    def send_intro(order,*a,**k):
        read('introduce-tasks',order);calls.append(('introduce',order));return True
    def status_intro(order,*a):return put('introduces',order,{'status':'CHECKED_OK','productionOrderId':flow._find_production_order_id_by_suz_order_id(order)})
    def send_set(pid,*a,**k):
        read('agg_set_tasks',pid);calls.append(('set',pid));return True
    for name,fn in [('sign_and_send_utilisation',send_util),('update_utilisation_report_status',status_util),
                    ('sign_and_send_introduce',send_intro),('update_introduce_status',status_intro),
                    ('sign_and_send_aggregation_set',send_set)]:monkeypatch.setattr(tasks,name,fn)
    monkeypatch.setattr(tasks,'update_aggregation_set_status',lambda pid,*_:put('agg_sets',pid,{'status':'CHECKED_OK'}))
    for name in ['create_equipment_aggregation_task','create_virtual_tasks_from_equipment_report',
                 'create_equipment_set_report_from_report','create_aggregation_report','sign_and_send_aggregation']:
        monkeypatch.setattr(tasks,name,deny)
    order=str(uuid4());tid=str(uuid4());pid='T-direct-'+tid
    task={'Gtin':set_gtin,'Quantity':3,'directMode':True,'emissionDocumentId':order,
          'PasportData':{'Manufacturer_inn':'0000000000','Batch_date_production':'2026-10-01','Batch_date_expired':'2029-10-01'}}
    codes=lambda gtin,n:['01'+gtin+'21'+f'SYN{i:010d}'+'\x1d93TEST' for i in range(n)]
    (tmp_path/(tid+'.json')).write_text(json.dumps(task))
    (tmp_path/(order+'.json')).write_text(json.dumps({'orderId':order,'codes':codes(set_gtin,3)}))
    tasks.logic_create_order('input/Задания/'+tid+'.json',task)
    assert read('production_orders_path',pid)['GtinType']=='SET'
    from xtrek.direct_mode import validate_direct_document
    from xtrek.operation_state import OperationConflict
    with pytest.raises(OperationConflict, match='component introduction'):
        validate_direct_document(config,order,{'productionOrderId':pid},'introduce')
    assert not Path(config['emission_receipts'],pid+'.json').exists()
    tasks.logic_kodes('kodes/'+order+'.json')
    if parent_first:
        with pytest.raises(tasks.WorkflowPending):
            tasks.logic_utilisationReceipt('utilisationReceipts/'+order+'.json')
    assert not any(x[0]=='set' for x in calls), 'Must await components'
    for component_index, (gtin,qty) in enumerate(components):
        child='V-'+pid+'-'+gtin;value=read('production_orders_path',child)
        assert int(value['Quantity'])==3*qty and value['virtual'] is True
        assert value['PasportData']['Manufacturer_inn']==task['PasportData']['Manufacturer_inn']
        tasks.logic_start_virtualProdTask_emission('productionOrders/'+child+'.json')
        emission = read('emission_orders_path', child)
        assert emission['products'][0]['cisType'] == 'UNIT'
        assert emission['products'][0]['quantity'] == 3*qty
        # Synthetic SUZ emission response for the standard virtual component route.
        child_order=str(uuid4())
        put('emission_receipts',child,{'orderId':child_order})
        put('emissions_path',child_order,{'productionOrderId':child})
        put('kodes',child_order,{'orderId':child_order,'codes':codes(gtin,3*qty)})
        tasks.logic_kodes('kodes/'+child_order+'.json')
        tasks.logic_utilisationReceipt('utilisationReceipts/'+child_order+'.json')
        if not parent_first or component_index < len(components) - 1:
            with pytest.raises(tasks.WorkflowPending):
                tasks.logic_update_introduce('introduceReceipts/'+child_order+'.json')
        else:
            tasks.logic_update_introduce('introduceReceipts/'+child_order+'.json')
    if not parent_first:
        assert not any(x[0]=='set' for x in calls)
        tasks.logic_utilisationReceipt('utilisationReceipts/'+order+'.json')
    report=read('equipment_set_reports',pid)
    assert len(report['readyBox'])==3
    parents=[b['boxNumber'].split('\x1d')[0] for b in report['readyBox']]
    assert set(parents)=={c.split('\x1d')[0] for c in codes(set_gtin,3)}
    used=[]
    for box in report['readyBox']:
        items=box['productNumbersFull'];assert len(items)==3
        assert [c[2:16] for c in items].count(components[0][0])==2
        used.extend(items)
    assert len(used)==len(set(used))==9
    assert ('introduce',order) not in calls
    assert calls.count(('set',pid))==1
    assert 'completed' in tasks.logic_update_agg_set('aggSetReceipts/'+pid+'.json')
    before=list(calls)
    tasks.logic_create_order('input/Задания/'+tid+'.json',task)
    tasks.logic_kodes('kodes/'+order+'.json')
    tasks.trigger_set_aggregation_if_ready(pid)
    assert calls==before

@pytest.mark.parametrize('flag,source', [(True,'feed'),(False,'feed'),(True,'fallback'),(False,'fallback'),(None,'feed'),('false','feed')])
def test_direct_type_uses_shared_catalog_resolution(monkeypatch, flag, source):
    from xtrek import create_emission_task_sample as flow
    data={'Gtin':'04670404500000','Quantity':1,'directMode':True,'GtinType':'FORGED',
          'emissionDocumentId':str(uuid4()),'PasportData':{'Manufacturer_inn':'0000000000',
          'Batch_date_production':'2026-10-01','Batch_date_expired':'2029-10-01'}}
    config={'production_orders_path':'synthetic-production'}
    monkeypatch.setattr(flow,'load_config',lambda *_:config)
    store=MagicMock();store.get_tags.return_value={};store.read_text.return_value=json.dumps(data)
    monkeypatch.setattr(flow,'get_storage',lambda *a:store)
    monkeypatch.setattr(flow,'get_inn_by_gtin',lambda *a,**k:'0000000000')
    monkeypatch.setattr(flow,'OrganizationManager',MagicMock());monkeypatch.setattr(flow,'TokenProcessor',MagicMock())
    nk=MagicMock();nk.feedProduct.return_value={'result':[{'is_set':flag}]} if source=='feed' else None
    nk.product_info.return_value={'isSet':flag,'gtin':data['Gtin']}
    monkeypatch.setattr(flow,'NK',lambda **_:nk)
    published=MagicMock();monkeypatch.setattr(flow,'publish_once',published)
    if type(flag) is not bool:
        with pytest.raises(ValueError,match='confirm UNIT/SET'):flow.process_incoming_task('input/'+str(uuid4())+'.json')
        published.assert_not_called()
    else:
        flow.process_incoming_task('input/'+str(uuid4())+'.json')
        assert json.loads(published.call_args.args[2])['GtinType']==('SET' if flag else 'UNIT')
        nk.feedProduct.assert_called_once_with(data['Gtin'])
        if source=='fallback':nk.product_info.assert_called_once_with(data['Gtin'],rd_info=False)


def test_direct_readiness_resolves_participant_without_equipment(monkeypatch, tmp_path):
    from xtrek import utils
    path=tmp_path/'parent.json'
    path.write_text(json.dumps({'directMode':True,'Gtin':'04670404500000','PasportData':{'Manufacturer_inn':'0000000000'}}))
    config={}
    monkeypatch.setattr(utils,'_environment_true_api_token',lambda _:None)
    processor=MagicMock();processor.get_token_for.return_value={'Токен':'synthetic'}
    monkeypatch.setattr(utils,'TokenProcessor',lambda **_:processor)
    api=MagicMock();nk=MagicMock()
    monkeypatch.setattr(utils,'HonestSignAPI',lambda **_:api)
    monkeypatch.setattr(utils,'NK',lambda **_:nk)
    assert utils._ensure_resources(str(path),config=config)[1:3]==(api,nk)
    processor.get_token_for.assert_called_once_with('0000000000',purpose='true_api')
