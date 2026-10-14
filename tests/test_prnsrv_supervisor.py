import hashlib
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import Mock

import pytest
from botocore.exceptions import ClientError, EndpointConnectionError, ReadTimeoutError

from xtrek.prnsrv_supervisor import (
    Supervisor, Evidence, FatalSupervisorError, atomic_json, transient_read,
)
from xtrek.s3_poller import configure_poller_logging

MAIN = 'a'*40
UID = '01954888-8b83-466b-9f6d-c425b20364d5'
UID2 = '0208e112-8344-4e01-80ad-42331dbef32a'
PROTECTED = '623c20fa-f90c-4478-aeae-9c6635cedc9f'
NOW = datetime(2026,10,14,tzinfo=timezone.utc)


class Clock:
    def __init__(self): self.value = 1000.0
    def now(self): return self.value
    def sleep(self, seconds): self.value += seconds


class Adapter:
    def __init__(self):
        self.objects=[];self.sources={};self.fail_list=[];self.fail_get=[]
        self.generated=[];self.staged=[];self.allocations={};self.after_allocate=None
    def add(self, uid=UID, count=50):
        key='Задания/'+uid+'.json';self.objects.append({'Key':key,'LastModified':NOW})
        self.sources[key]={'count':count,'secret':'PRIVATE-CODE-SIGNATURE'}
    def list_sources(self, deadline):
        if self.fail_list: raise self.fail_list.pop(0)
        return self.objects
    def read_source(self, key, deadline):
        if self.fail_get: raise self.fail_get.pop(0)
        return self.sources[key]
    def describe(self, source):
        return hashlib.sha256(json.dumps(source).encode()).hexdigest(),'7733154124',source['count']
    def check_history(self, uid, source_hash, inn, deadline):
        if uid in self.allocations:raise ValueError('Prior allocation')
    def generate(self,key,uid,source_hash):
        self.generated.append(uid);self.allocations[uid]=self.allocations.get(uid,0)+1
        if self.after_allocate:raise self.after_allocate
    def stage(self,uid):self.staged.append(uid);return 'receipt/'+uid


def state(tmp_path, jobs=None):
    value={'main':MAIN,'cutoff':'2026-10-11T13:43:46.364465+00:00',
           'baseline_keys':['Задания/historical.json'],'jobs':jobs or {}}
    atomic_json(tmp_path/'state.json',value)
    return value


def supervisor(tmp_path, adapter, clock=None):
    clock=clock or Clock()
    return Supervisor(tmp_path,MAIN,adapter,clock=clock.now,monotonic=clock.now,
                      sleep=clock.sleep,jitter=lambda:.5)


def error(status):
    return ClientError({'ResponseMetadata':{'HTTPStatusCode':status},
                        'Error':{'Code':'InternalError','Message':'PRIVATE-TOKEN'}},'ListObjectsV2')


@pytest.mark.parametrize('failure', [EndpointConnectionError(endpoint_url='PRIVATE-TOKEN'),
                                    ReadTimeoutError(endpoint_url='PRIVATE-TOKEN'),
                                    error(429),error(500),error(502),error(503),error(504)])
def test_transient_reads_recover_without_repeat_business(tmp_path,failure):
    before=state(tmp_path);a=Adapter();a.add();a.fail_list=[failure,failure]
    s=supervisor(tmp_path,a);s.cycle();s.cycle()
    assert a.generated==[UID]and a.staged==[UID]and a.allocations=={UID:1}
    assert s.state['jobs'][UID]['state']=='STAGED'
    assert s.state['cutoff']==before['cutoff']and s.state['baseline_keys']==before['baseline_keys']
    combined=''.join(p.read_text()for p in tmp_path.glob('*.json*'))
    assert 'PRIVATE-TOKEN'not in combined and'PRIVATE-CODE-SIGNATURE'not in combined
    assert s.evidence.value['last_full_scan_success']is not None


def test_outage_exhaustion_keeps_process_degraded_and_backoff_bounded(tmp_path):
    state(tmp_path);a=Adapter();a.add();s=supervisor(tmp_path,a)
    delays=[]
    for _ in range(10):
        a.fail_list=[error(503)]*5;delays.append(s.cycle())
        assert s.evidence.value['phase']=='DEGRADED'
        assert s.state['jobs']=={}and not a.generated
    assert all(0<d<=300 for d in delays)
    assert delays[:4]==[27,54,108,216]and delays[-1]==270
    s.cycle();assert a.generated==[UID]
    assert s.evidence.value['failures']==0


def test_pagination_failure_does_not_admit_partial_sources(tmp_path):
    state(tmp_path);a=Adapter();a.add()
    def partial(deadline):
        # Real adapter gathers complete pagination before returning it.
        _first_page=a.objects
        raise error(503)
    a.list_sources=partial;s=supervisor(tmp_path,a);s.cycle()
    assert not a.generated and not s.state['jobs']


@pytest.mark.parametrize('status',[400,401,403])
def test_nonretryable_scan_fails_closed(tmp_path,status):
    state(tmp_path);a=Adapter();a.add();a.fail_list=[error(status)];s=supervisor(tmp_path,a)
    with pytest.raises(FatalSupervisorError):s.cycle()
    assert not a.generated and s.evidence.value['phase']=='FATAL'


def test_get_failure_before_admission_does_not_poison_job_or_previous_hash(tmp_path):
    state(tmp_path);a=Adapter();a.add(UID);a.add(UID2);s=supervisor(tmp_path,a)
    a.fail_get=[error(503)]*5;s.cycle()
    assert UID not in s.state['jobs']
    assert not a.generated and s.evidence.value['phase']=='DEGRADED'
    s.cycle();assert a.allocations=={UID:1,UID2:1}


def test_crash_after_allocation_restart_skips_admitted_without_replay(tmp_path):
    state(tmp_path);a=Adapter();a.add();a.after_allocate=SystemExit('simulated process crash')
    s=supervisor(tmp_path,a)
    with pytest.raises(SystemExit):s.cycle()
    saved=json.loads((tmp_path/'state.json').read_text())
    assert saved['jobs'][UID]['state']=='ADMITTED'and a.allocations[UID]==1
    a.after_allocate=None;new=supervisor(tmp_path,a);new.cycle()
    assert a.generated==[UID]and a.allocations[UID]==1 and not a.staged
    assert new.state==saved and new.evidence.value['quarantined_admitted']==1


def test_post_admission_timeout_blocks_not_retries(tmp_path):
    state(tmp_path);a=Adapter();a.add();a.after_allocate=ReadTimeoutError(endpoint_url='PRIVATE-TOKEN')
    s=supervisor(tmp_path,a);s.cycle();s.cycle();supervisor(tmp_path,a).cycle()
    assert a.generated==[UID]and a.allocations[UID]==1
    assert s.state['jobs'][UID]['state']=='BLOCKED'


def test_protected_completed_blocked_and_empty_quantity_preserved(tmp_path):
    jobs={UID:{'state':'STAGED','source_hash':'saved'},UID2:{'state':'BLOCKED','source_hash':'saved2'}}
    before=state(tmp_path,jobs);a=Adapter();a.add(UID);a.add(UID2);a.add(PROTECTED)
    empty='036b25d4-59db-43b0-82b3-47aa246e5e74';a.add(empty,None)
    s=supervisor(tmp_path,a);s.cycle()
    assert not a.generated and all(s.state['jobs'][k]==v for k,v in jobs.items())
    assert PROTECTED not in s.state['jobs']and s.state['jobs'][empty]['state']=='SKIPPED'
    assert s.state['baseline_keys']==before['baseline_keys']


def test_checkpoint_failure_prevents_business_call(tmp_path,monkeypatch):
    state(tmp_path);a=Adapter();a.add();s=supervisor(tmp_path,a)
    monkeypatch.setattr(s,'save',Mock(side_effect=FatalSupervisorError('disk full')))
    with pytest.raises(FatalSupervisorError):s.cycle()
    assert not a.generated


def test_sigterm_finishes_current_job_then_stops_admission(tmp_path):
    state(tmp_path);a=Adapter();a.add(UID);a.add(UID2);s=supervisor(tmp_path,a)
    original=a.generate
    def generate(*args):original(*args);s.stop()
    a.generate=generate;s.run()
    assert a.generated==[UID]and a.staged==[UID]
    assert UID2 not in s.state['jobs']and s.evidence.value['phase']=='STOPPED_SAFE'


def test_last_success_survives_process_restart_and_error_rotation(tmp_path):
    state(tmp_path);a=Adapter();s=supervisor(tmp_path,a);s.cycle()
    success=s.evidence.value['last_full_scan_success'];e=Evidence(tmp_path,MAIN,clock=lambda:1200,rotate_bytes=100)
    assert e.value['last_full_scan_success']==success
    for _ in range(20):
        try:raise RuntimeError('PRIVATE-TOKEN PRIVATE-CODE-SIGNATURE')
        except RuntimeError as err:e.error(err,'LIST')
    assert len(list(tmp_path.glob('events.jsonl.*')))<=5
    assert any('traceback'in p.read_text()for p in tmp_path.glob('events.jsonl*'))
    assert all('PRIVATE-'not in p.read_text()for p in tmp_path.glob('events.jsonl*'))


def test_sdk_debug_headers_not_emitted_after_import_side_effects(caplog):
    sdk=logging.getLogger('botocore.endpoint');root=logging.getLogger()
    previous=sdk.level;previous_root=root.level
    try:
        sdk.setLevel(logging.DEBUG);root.setLevel(logging.DEBUG)
        configure_poller_logging();caplog.set_level(logging.INFO)
        sdk.debug('PRIVATE-TOKEN response body PRIVATE-CODE')
        logging.getLogger('xtrek.s3_poller').info('S3 poll complete: discovered=1 published=1')
        assert 'PRIVATE-'not in caplog.text and'S3 poll complete'in caplog.text
    finally:sdk.setLevel(previous);root.setLevel(previous_root)


def test_invalid_state_is_fatal(tmp_path):
    (tmp_path/'state.json').write_text('{broken')
    with pytest.raises(FatalSupervisorError):supervisor(tmp_path,Adapter())


def test_transient_get_exhaustion_increases_global_cooldown(tmp_path):
    state(tmp_path);a=Adapter();a.add();s=supervisor(tmp_path,a)
    for expected in (27,54,108):
        a.fail_get=[ReadTimeoutError(endpoint_url='private')]*5
        assert s.cycle()==expected
    assert not a.generated and not s.state['jobs']
    s.cycle();assert a.generated==[UID]and s.evidence.value['failures']==0


def test_deadline_completed_read_cannot_admit_late_result(tmp_path):
    state(tmp_path);a=Adapter();a.add();c=Clock();s=supervisor(tmp_path,a,c)
    def late(deadline):c.value=deadline+1;return a.objects
    a.list_sources=late
    assert s.cycle()<=300 and not a.generated


def test_existing_allocation_is_blocked_without_generate(tmp_path):
    state(tmp_path);a=Adapter();a.add();a.allocations[UID]=1
    s=supervisor(tmp_path,a);s.cycle();s.cycle()
    assert not a.generated and a.allocations[UID]==1
    assert s.state['jobs'][UID]['state']=='BLOCKED'


def test_old_success_heartbeat_replaced_after_empty_successful_scan(tmp_path):
    state(tmp_path);a=Adapter();c=Clock();s=supervisor(tmp_path,a,c);s.cycle()
    c.value+=300;s.cycle()
    assert s.evidence.value['last_full_scan_success']==1300
    assert s.evidence.value['last_cycle_success']==1300


def test_service_has_bounded_restart_and_fatal_prevention():
    body=(Path(__file__).parents[1]/'deploy/systemd/xtrek-prnsrv-temporary-auto.service').read_text()
    for field in ('Restart=on-failure','RestartSec=30s','StartLimitIntervalSec=600',
                  'StartLimitBurst=5','RestartPreventExitStatus=78','TimeoutStopSec=infinity',
                  '-m xtrek.prnsrv_supervisor'):
        assert field in body
    assert '[Install]'not in body
