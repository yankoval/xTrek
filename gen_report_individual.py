#!/usr/bin/env python3
"""
Usage:
  python3 gen_report_individual.py <name> [<name> ...]
  python3 gen_report_individual.py                              # scan all non-finished
Requires: suz_worker_config env var or ~/python-projects/suz_worker_config.json
"""
import boto3, json, sys, os, re, io, xlsxwriter
from datetime import datetime, timezone

cfg_path = os.environ.get('suz_worker_config', os.path.expanduser('~/python-projects/suz_worker_config.json'))
with open(cfg_path) as f: cfg = json.load(f)
print(f'Config: {cfg_path}')

sc = cfg.get('s3_config', {})
B = ''
for v in cfg.values():
    m = re.match(r's3://([^/]+)', str(v))
    if m: B = m.group(1); break
if not B: print('No bucket'); sys.exit(1)

EP = sc.get('endpoint_url', 'https://storage.yandexcloud.net')
U = f'{EP}/{B}'
s3 = boto3.client('s3', endpoint_url=EP,
    aws_access_key_id=sc.get('aws_access_key_id',''),
    aws_secret_access_key=sc.get('aws_secret_access_key',''),
    region_name=sc.get('region_name','ru-central1'))
p = s3.get_paginator('list_objects_v2')

def pf(key):
    v = cfg.get(key, '')
    m = re.match(r's3://[^/]+/(.+)', str(v))
    return m.group(1) if m else ''

P = {
    'eq':   pf('equipment-reports'),      'ut': pf('utilisation_tasks_path'),
    'ur':   pf('utilisation_receipts'),     'up': pf('utilisation_reports'),
    'em':   pf('emission_receipts'),        'po': pf('production_orders_path'),
    'ir':   pf('introduce-receipts'),       'iv': pf('introduces'),
    'esr':  pf('equipment_set_reports'),    'ast': pf('agg_set_tasks'),
    'asr':  pf('agg_set_receipts'),         'ass': pf('agg_sets'),
    'at':   pf('agg-tasks'),                'ar': pf('agg-receipts'),
    'ag':   pf('aggs'),                     'kd': pf('kodes'),
    'out':  'Report/',
}

CSS = '''<style>
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;max-width:1100px;margin:14px auto;padding:0 12px;background:#f0f2f5;color:#212529}
h1{font-size:1em;border-bottom:2px solid #0d6efd;padding-bottom:5px;word-break:break-all;margin:0 0 8px 0}
details{margin:8px 0;border:1px solid #dee2e6;border-radius:8px;background:#fff;overflow:hidden}
summary{padding:8px 12px;font-weight:600;font-size:.9em;cursor:pointer;background:#f8f9fa;display:flex;align-items:center;gap:8px;user-select:none}
summary:hover{background:#e9ecef}
summary .dot{width:9px;height:9px;border-radius:50%;display:inline-block;flex-shrink:0}
.dot.ok{background:#198754}.dot.w{background:#ffc107}.dot.e{background:#dc3545}
summary .cnt{font-weight:400;font-size:.8em;color:#6c757d;margin-left:auto}
.db{padding:4px 10px 8px 10px}
.st{display:flex;align-items:flex-start;margin:2px 0;padding:3px 6px;border-radius:3px;font-size:.82em}
.st.ok{border-left:3px solid #198754;background:#effaf4}
.st.w{border-left:3px solid #ffc107;background:#fffef3}
.st.e{border-left:3px solid #dc3545;background:#ffeef0}
.st .lb{font-weight:600;color:#6c757d;min-width:120px;font-size:.78em}
.st .mt{color:#495057;margin-left:6px;font-size:.8em}
a{color:#0d6efd;text-decoration:none}a:hover{text-decoration:underline}
.l0{margin-left:0}.l1{margin-left:18px}.l2{margin-left:36px}
.vh{font-size:.8em;color:#495057;margin:6px 0 1px 0;font-weight:600}
.kl{font-size:.8em;margin-left:6px;color:#0d6efd}
.kd-inline{font-size:.78em;margin-left:6px;color:#198754;font-weight:500}
.sok{border-color:#198754}.sw{border-color:#ffc107}.se{border-color:#dc3545}
.bnr{margin-top:14px;padding:10px 16px;border-radius:8px;color:#fff;text-align:center;font-weight:600;font-size:.95em}
.bnr.ok{background:#198754}.bnr.w{background:linear-gradient(135deg,#856404,#b8860b)}.bnr.e{background:#dc3545}
.bnr .sub{font-size:.72em;font-weight:400;opacity:.85;margin-top:2px}
.ft{text-align:center;color:#adb5bd;margin-top:8px;font-size:.75em}
</style>'''

# XLSX generation is done server-side in generate_kodes_xlsx() — no JS conversion needed.

def ex(key):
    try: s3.head_object(Bucket=B, Key=key); return True
    except: return False

def rj(key):
    return json.loads(s3.get_object(Bucket=B, Key=key)['Body'].read())

def ts(key):
    try: return s3.head_object(Bucket=B, Key=key)['LastModified'].astimezone().strftime('%d.%m %H:%M')
    except: return ''

def st_html(lb,key,cls,meta,lvl='l0'):
    lk = f'<a href="{U}/{key}" target="_blank">{key.split("/")[-1]}</a>' if key else '<em>no file</em>'
    tm = ''
    if key:
        try: tm = s3.head_object(Bucket=B, Key=key)['LastModified'].astimezone().strftime('%d.%m %H:%M')
        except: pass
    ts_html = f' <span style="font-size:.7em;color:#adb5bd">{tm}</span>' if tm else ''
    return f'<div class="st {cls} {lvl}"><span class="lb">{lb}</span><span>{lk}{ts_html}</span><span class="mt">{meta}</span></div>'

def kd_link(key,label):
    return f'<a href="{U}/{key}" target="_blank" class="kd-inline">\U0001F4E5 {label}</a>'

def kd_xls_link(xlsx_key, label):
    """Direct download link to pre-generated XLSX on S3 (via xlsxwriter, same as jsontoxlsx.py)."""
    return f'<a href="{U}/{xlsx_key}" class="kd-inline" style="color:#0d6efd;margin-left:4px">\U0001F4CA {label}.xls</a>'

def generate_kodes_xlsx(kd_data, bucket, xlsx_key):
    """Read codes from kodes JSON, write XLSX via xlsxwriter, upload to S3."""
    codes_raw = kd_data.get('codes', kd_data.get('productNumbersFull', []))
    if not codes_raw:
        return
    output = io.BytesIO()
    wb = xlsxwriter.Workbook(output, {'in_memory': True})
    ws = wb.add_worksheet('Codes')
    fmt_header = wb.add_format({'bold': True})
    ws.write(0, 0, 'Номер КИ', fmt_header)
    import re as _re
    for i, raw in enumerate(codes_raw):
        if not isinstance(raw, str):
            continue
        m = _re.search(r"01(\d{14})21([^\u001d]+)", raw)
        if m:
            code = f"01{m.group(1)}21{m.group(2)}"
        else:
            code = raw.replace('\u001d', '').strip()
        ws.write(i + 1, 0, code)
    wb.close()
    output.seek(0)
    try:
        s3.put_object(Bucket=bucket, Key=xlsx_key,
                      Body=output.getvalue(),
                      ContentType='application/vnd.ms-excel')
        print(f'  XLSX uploaded: {xlsx_key}')
    except Exception as e:
        print(f'  XLSX upload failed: {e}')

def norm_name(raw):
    n = raw.strip().replace(P['eq'],'').replace('.json','')
    if not n.startswith('T-'):
        for pg in p.paginate(Bucket=B, Prefix=f'{P["eq"]}T-{n}'):
            for o in pg.get('Contents',[]):
                k = o['Key']
                if k == P['eq']: continue
                s2 = k.replace(P['eq'],'').replace('.json','')
                if s2.endswith(n) or n in s2: return s2
    return n

r = []
if len(sys.argv) > 1:
    for raw in sys.argv[1:]:
        n = norm_name(raw)
        k = f'{P["eq"]}{n}.json'
        try:
            s3.head_object(Bucket=B, Key=k)
            t = {t['Key']:t['Value'] for t in s3.get_object_tagging(Bucket=B,Key=k).get('TagSet',[])}
            ch = t.get('check','?')
            r.append((n,k,ch))
            print(f'+ {n} (check:{ch})')
        except Exception as e:
            print(f'- {n}: NOT FOUND ({e})')
else:
    for pg in p.paginate(Bucket=B, Prefix=f'{P["eq"]}T-'):
        for o in pg.get('Contents',[]):
            k = o['Key']
            if k == P['eq']: continue
            t = {t['Key']:t['Value'] for t in s3.get_object_tagging(Bucket=B,Key=k).get('TagSet',[])}
            if t.get('check') != 'finished':
                n = k.replace(P['eq'],'').replace('.json','')
                r.append((n,k,t.get('check','?')))
    print(f'Files: {len(r)} (check!=finished)')

if not r:
    print('No reports to process.')
    sys.exit(0)

for n,k,ch in r:
    secs = []; all_ok = []

    # ====== 0. Production order ======
    s0 = ''; oks0 = []
    po_found = False
    if n.startswith('T-'):
        for pg2 in p.paginate(Bucket=B, Prefix=f'{P["po"]}V-{n}'):
            for o2 in pg2.get('Contents',[]):
                vk = o2['Key']
                if vk.endswith('/'): continue
                vn = vk.split('/')[-1].replace('.json','')
                s0 += st_html('Production order',vk,'ok',vn,'l1'); oks0.append(True); po_found = True
    if not po_found:
        s0 += st_html('Production order',None,'e','not found','l1'); oks0.append(False)
    s0s = 'ok' if all(oks0) else ('w' if any(oks0) else 'e')
    secs.append(('0. Production order',s0s,s0)); all_ok.extend(oks0)

    # ====== 1. Equipment report & Virtual attachments ======
    s1 = ''; oks1 = []
    # Equipment report stats
    try:
        eq = rj(k)
        bx = len(eq.get('readyBox',[]))
        cds = sum(len(b.get('productNumbersFull',[])) for b in eq.get('readyBox',[]))
        g = eq.get('readyBox',[{}])[0].get('productNumbersFull',[''])[0]
        gt = g[2:16] if g.startswith('01') and len(g)>=16 else '?'
        s1 += f'<span style="background:#e9ecef;padding:3px 8px;border-radius:4px;margin:0 6px 6px 0;font-size:.85em">\U0001F4E6 {bx}</span>'
        s1 += f'<span style="background:#e9ecef;padding:3px 8px;border-radius:4px;margin:0 6px 6px 0;font-size:.85em">\U0001F3F7 {cds}</span>'
        s1 += f'<span style="background:#e9ecef;padding:3px 8px;border-radius:4px;font-size:.85em">\U0001F522 GTIN {gt}</span>'
        s1 += f'<br><a href="{U}/{k}" target="_blank" class="kl">\U0001F4C4 {k.split("/")[-1]}</a>'
        oks1.append(True)
    except:
        s1 += '<span class="st e"><span class="lb">ERROR</span>read failed</span>'
        oks1.append(False)

    # V-files (sub-stage of equipment report)
    vfiles = []
    first_oid = None
    if n.startswith('T-'):
        for pg2 in p.paginate(Bucket=B, Prefix=f'{P["em"]}V-{n}'):
            for o2 in pg2.get('Contents',[]):
                vk = o2['Key']
                if vk.endswith('/'): continue
                try:
                    dd = rj(vk); oid_v = dd.get('orderId','')
                    vfiles.append((vk, oid_v))
                    if not first_oid: first_oid = oid_v
                except: pass

    if vfiles:
        for vi,(vk,oid_v) in enumerate(vfiles):
            vname = vk.split('/')[-1].replace('.json','')
            s1 += f'<div class="vh l1">\u2500\u2500 Attachment {vi+1}: {vname}</div>'
            # Emission receipt
            s1 += st_html('Emission receipt',vk,'ok',f'orderId {oid_v[:16]}' if oid_v else 'no','l2'); oks1.append(True)
            if oid_v:
                kk = f'{P["kd"]}{oid_v}.json'
                if ex(kk):
                    xlsx_key = f'{P["out"]}{n}_kodes_V{vi+1}.xls'
                    if not ex(xlsx_key):
                        try: generate_kodes_xlsx(rj(kk), B, xlsx_key)
                        except Exception as xe: print(f'  XLSX gen failed: {xe}')
                    s1 += f'<div class="st ok l2"><span class="lb">Kodes</span><span>{kd_link(kk, f"V{vi+1} codes")} {kd_xls_link(xlsx_key, f"kodes_V{vi+1}")}</span></div>'
                for lb,sf in [('Util task','ut'),('Util receipt','ur'),('Util report','up'),('Intro receipt','ir'),('Intro doc','iv')]:
                    kv = f'{P[sf]}{oid_v}.json'
                    if ex(kv):
                        okf = True
                        if sf == 'up':
                            try:
                                ud = rj(kv); st = ud.get('reportStatus','?'); msg = ud.get('reportStatusMessage','')
                                m = st; okf = (st == 'SUCCESS')
                                if msg: m += f' | {msg[:60]}'
                            except: m='read error'; okf=False
                        elif sf == 'iv':
                            try:
                                ivd = rj(kv)
                                if isinstance(ivd,list) and len(ivd) > 0:
                                    d0 = ivd[0]; st = d0.get('status','?'); tp = d0.get('type','')
                                    inn = d0.get('senderInn','')
                                    m = st; okf = (st == 'CHECKED_OK')
                                    if tp: m += f' type:{tp}'
                                    if inn: m += f' INN:{inn}'
                                else: m='empty'
                            except: m='read error'; okf=False
                        elif 'receipt' in sf:
                            try:
                                rd = rj(kv)
                                m = f'ID {str(rd.get(list(rd.keys())[0],"?"))[:16]}'
                            except: m='read error'
                        else: m='created'
                        s1 += st_html(lb,kv,'ok' if okf else 'w',m,'l2'); oks1.append(okf)
                    else:
                        s1 += st_html(lb,None,'e','not found','l2'); oks1.append(False)
    else:
        s1 += st_html('Attachments',None,'e','no V-files found','l1'); oks1.append(False)

    if first_oid:
        kk = f'{P["kd"]}{first_oid}.json'
        if ex(kk):
            xlsx_key = f'{P["out"]}{n}_kodes_T-level.xls'
            if not ex(xlsx_key):
                try: generate_kodes_xlsx(rj(kk), B, xlsx_key)
                except Exception as xe: print(f'  XLSX gen failed: {xe}')
            s1 += f'<div class="st ok l1"><span class="lb">T Kodes</span><span>{kd_link(kk, "T-level codes")} {kd_xls_link(xlsx_key, "kodes_T-level")}</span></div>'

    s1s = 'ok' if all(oks1) else ('w' if any(oks1) else 'e')
    secs.append(('1. Equipment report & Attachments',s1s,s1)); all_ok.extend(oks1)

    # ====== 2. T-level utilisation ======
    s2 = ''; oks2 = []
    for lb,sf in [('Util task','ut'),('T-Util receipt','ur'),('T-Util report','up')]:
        k2 = f'{P[sf]}{n}.json'
        if ex(k2):
            okf = True
            if sf == 'up':
                try:
                    ud = rj(k2); st = ud.get('reportStatus','?'); msg = ud.get('reportStatusMessage','')
                    m = st; okf = (st == 'SUCCESS')
                    if msg: m += f' | {msg[:60]}'
                except: m='read error'; okf=False
            elif sf == 'ur':
                try: rd = rj(k2); m = f'reportId {str(rd.get("reportId","?"))[:12]}'
                except: m='read error'
            else: m='created'
            s2 += st_html(lb,k2,'ok' if okf else 'w',m); oks2.append(okf)
        else:
            s2 += st_html(lb,None,'e','not found'); oks2.append(False)
    s2s = 'ok' if all(oks2) else ('w' if any(oks2) else 'e')
    secs.append(('2. T-level utilisation',s2s,s2)); all_ok.extend(oks2)

    # ====== 3. SET aggregation ======
    s4 = ''; oks4 = []
    if n.startswith('T-'):
        for lb,sf in [('Set report','esr'),('Set task','ast'),('Set receipt','asr'),('Set status','ass')]:
            ks = f'{P[sf]}{n}.json'
            if ex(ks):
                okf = True
                if sf == 'ast':
                    try: td = rj(ks); inn = td.get('participantId','?'); au = td.get('aggregationUnits',[]); nu = len(au) if isinstance(au,list) else 0; m = f'INN {inn} {nu}units'
                    except: m='read error'; okf=False
                elif sf == 'asr':
                    try: rd = rj(ks); m = f'doc_id {str(rd.get("document_id","?"))[:20]}'
                    except: m='read error'
                elif sf == 'ass':
                    try:
                        ad = rj(ks)
                        if isinstance(ad,list) and len(ad) > 0:
                            d0 = ad[0]; st = d0.get('status','?'); msg = d0.get('statusMessage',''); errs = d0.get('errors',[]); m = st; okf = (st == 'CHECKED_OK')
                            if msg: m += f' | {msg[:80]}'
                            if errs: m += f' | errors:{len(errs)}'
                        elif isinstance(ad,dict):
                            sc4 = ad.get('status_code',''); err4 = ad.get('error','')
                            if sc4 == 404 and '\u0414\u043e\u043a\u0443\u043c\u0435\u043d\u0442 \u043d\u0435 \u043d\u0430\u0439\u0434\u0435\u043d' in str(err4):
                                has_r = ex(f'{P["asr"]}{n}.json')
                                m = '\u043e\u0442\u0432\u0435\u0442 \u0441\u0435\u0440\u0432\u0435\u0440\u0430: "\u0414\u043e\u043a\u0443\u043c\u0435\u043d\u0442 \u043d\u0435 \u043d\u0430\u0439\u0434\u0435\u043d \u0432 \u0413\u0418\u0421 \u041c\u0422" \u2014 \u043e\u0436\u0438\u0434\u0430\u0435\u043c \u0441\u0438\u043d\u0445\u0440\u043e\u043d\u0438\u0437\u0430\u0446\u0438\u0438' if has_r else '\u0434\u043e\u043a\u0443\u043c\u0435\u043d\u0442 \u043d\u0435 \u0431\u044b\u043b \u043f\u0435\u0440\u0435\u0434\u0430\u043d \u0432 \u0441\u0438\u0441\u0442\u0435\u043c\u0443'
                                okf = False
                            else: m = f'status_code:{sc4} {str(err4)[:80]}'; okf=False
                        else: m='empty'; okf=False
                    except: m='read error'; okf=False
                elif sf == 'esr': m='created'
                s4 += st_html(lb,ks,'ok' if okf else 'w',m); oks4.append(okf)
            else:
                s4 += st_html(lb,None,'e','not found'); oks4.append(False)
    s4s = 'ok' if all(oks4) else ('w' if any(oks4) else 'e')
    secs.append(('3. SET aggregation',s4s,s4)); all_ok.extend(oks4)

    # ====== 4. Transport aggregation ======
    s5 = ''; oks5 = []
    if n.startswith('T-'):
        for lb,sf in [('Agg task','at'),('Agg receipt','ar'),('Agg status','ag')]:
            ks = f'{P[sf]}{n}.json'
            if ex(ks):
                okf = True
                if sf == 'ag':
                    try: td = rj(ks); st = td[0].get('status','?') if (isinstance(td,list) and td) else '?'; m = st; okf = (st == 'CHECKED_OK')
                    except: m='read error'; okf=False
                elif sf == 'ar':
                    try: rd = rj(ks); m = f'ID {str(rd.get(list(rd.keys())[0],"?"))[:20]}'
                    except: m='read error'
                else: m='created'
                s5 += st_html(lb,ks,'ok' if okf else 'w',m); oks5.append(okf)
            else:
                s5 += st_html(lb,None,'e','not found'); oks5.append(False)
    s5s = 'ok' if all(oks5) else ('w' if any(oks5) else 'e')
    secs.append(('4. Transport aggregation',s5s,s5)); all_ok.extend(oks5)

    # ====== 5. Tag ======
    s6 = st_html('Tag',k,'ok' if ch=='finished' else 'w',f'check: <b>{ch}</b>')
    s6s = 'ok' if ch == 'finished' else 'w'
    secs.append(('5. Tag',s6s,s6)); all_ok.append(ch == 'finished')

    # ====== Render ======
    sh = ''
    for title,status,body in secs:
        dc = 'ok' if status == 'ok' else ('w' if status == 'w' else 'e')
        sc = 'sok' if status == 'ok' else ('sw' if status == 'w' else 'se')
        okn = body.count('st ok'); tn = body.count('class="st')
        cnt = f'{okn}/{tn} OK' if tn else ''
        opn = ' open' if status == 'e' or tn <= 2 else ''
        sh += f'<details class="{sc}"{opn}><summary><span class="dot {dc}"></span>{title}<span class="cnt">{cnt}</span></summary><div class="db">{body}</div></details>\n'

    overall = all(all_ok)
    bcl = 'ok' if overall else ('w' if any(all_ok) else 'e')
    btx = '\u2705 COMPLETE' if overall else ('\u26a0\ufe0f PARTIAL' if any(all_ok) else '\u274c INCOMPLETE')
    now = datetime.now().strftime('%Y-%m-%d %H:%M')

    html = f'<!DOCTYPE html><html lang="ru"><head><meta charset="UTF-8"><title>{n}</title>{CSS}</head><body><h1>{n}</h1>{sh}<div class="bnr {bcl}">{btx}<div class="sub">Generated {now}</div></div><div class="ft">{now} \u00b7 DeepSeek TUI</div></body></html>'
    s3.put_object(Bucket=B, Key=f'{P["out"]}{n}_report.html', Body=html.encode(), ContentType='text/html; charset=utf-8')

    # CLI summary with colors
    C={'ok':'\033[32m','w':'\033[33m','e':'\033[31m','R':'\033[0m'}
    def dot(s): return f'{C.get(s,"")}\u25cf{C["R"]}'
    ts=''
    try: ts=s3.get_object(Bucket=B,Key=k)['LastModified'].astimezone().strftime('%Y-%m-%d %H:%M')
    except:pass
    parts=[f'{ts}']
    for title,status,_ in secs:
        sn=title.split()[0].rstrip('.')
        parts.append(f'{sn}:{dot(status)}')
    report_url = f'{U}/{P["out"]}{n}_report.html'
    link = f'\033]8;;{report_url}\a{n[:80]}\033]8;;\a'
    parts.append(f'{C["ok"]}{link}{C["R"]}')
    print('  '+'  '.join(parts))

print(f'Done: {len(r)} reports')
