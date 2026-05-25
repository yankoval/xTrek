#!/usr/bin/env python3
"""
kin_info.py — запрос информации о КИН-коде через True API.

Поиск токена: HONEST_SIGN_TOKEN → ~/tokens.json → tokens_config.json
Вывод: тип, статус, история, док-ты, вложения для SET.

Использование:
  python3 kin_info.py <KIN-код>
  python3 kin_info.py --token eyJ... <KIN-код>
"""

import sys, os, json, argparse, requests, urllib3, base64, re
from pathlib import Path

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
HOST = os.getenv('TRUE_API_HOST', 'https://markirovka.crpt.ru')
TARGET_INN = '7733154124'

_type_map = {'UNIT':'Товар','PACK':'Набор','SET':'Набор','CONSUMER_PACK':'Потребительская упаковка'}
_status_map = {'EMITTED':'Эмитирован','INTRODUCED':'В обороте','APPLIED':'Нанесён',
               'WRITTEN_OFF':'Списан','RETIRED':'Выбыл','UTILIZED':'Утилизирован'}


def _jwt_inn(token):
    try:
        p = token.split('.')[1]
        p += '=' * (4 - len(p) % 4)
        return json.loads(base64.urlsafe_b64decode(p)).get('inn','')
    except: return ''


def find_token():
    t = os.getenv('HONEST_SIGN_TOKEN','')
    if t: print(f'Токен из HONEST_SIGN_TOKEN (ИНН {_jwt_inn(t)})'); return t

    tk = Path.home() / 'tokens.json'
    if tk.exists():
        try:
            with open(tk) as f: d = json.load(f)
            if isinstance(d,list):
                for i in d:
                    tok = i.get('Токен','')
                    if tok and _jwt_inn(tok)==TARGET_INN: print(f'Токен из ~/tokens.json (ИНН {TARGET_INN})'); return tok
                if d: tok = d[0].get('Токен','')
                if tok: print(f'Токен из ~/tokens.json ({_jwt_inn(tok)})'); return tok
            elif isinstance(d,dict):
                tok = d.get('Токен','')
                if tok: print(f'Токен из ~/tokens.json ({_jwt_inn(tok)})'); return tok
        except: pass

    cfg = Path(__file__).parent / 'tokens_config.json'
    if cfg.exists():
        try:
            with open(cfg) as f: c = json.load(f)
            sc = c.get('s3_config',{})
            if sc.get('endpoint_url') and sc.get('aws_access_key_id'):
                import boto3
                s3 = boto3.client('s3',endpoint_url=sc['endpoint_url'],
                    aws_access_key_id=sc['aws_access_key_id'],
                    aws_secret_access_key=sc.get('aws_secret_access_key',''),
                    region_name=sc.get('region_name','ru-central1'))
                m = re.match(r's3://([^/]+)/(.+)',c.get('tokens_path',''))
                if m:
                    try:
                        tokens = json.loads(s3.get_object(Bucket=m.group(1),Key=m.group(2))['Body'].read())
                        if isinstance(tokens,list):
                            for i in tokens:
                                tok = i.get('Токен','')
                                if tok and _jwt_inn(tok)==TARGET_INN: print(f'Токен из S3'); return tok
                    except: pass
        except: pass
    return ''


def get_cis_info(code, token):
    h = {'Authorization':f'Bearer {token}','Content-Type':'application/json'}
    try:
        r = requests.post(f'{HOST}/api/v3/true-api/cises/info',json=[code],headers=h,verify=False,timeout=30)
        r.raise_for_status(); return r.json()
    except Exception as e: print(f'Ошибка cises/info: {e}'); return []


def get_cis_history(code, token):
    h = {'Authorization':f'Bearer {token}','Accept':'*/*'}
    try:
        r = requests.post(f'{HOST}/api/v3/true-api/cises/history?cis={code}',headers=h,verify=False,timeout=30)
        r.raise_for_status(); return r.json()
    except Exception as e: print(f'Ошибка cises/history: {e}'); return []


def format(info_data, history_data):
    c = info_data.get('cisInfo',info_data) if isinstance(info_data,dict) else (info_data[0].get('cisInfo',info_data[0]) if info_data else {})
    events = history_data if isinstance(history_data,list) else []; cur = events[0] if events else {}
    cis = c.get('cis',cur.get('cis','?'))
    gtin = c.get('gtin',cur.get('gtin','-'))
    pt = cur.get('packageType',c.get('cisType',''))
    td = _type_map.get(pt,pt) if pt else '?'
    st = _status_map.get(c.get('status',cur.get('status','?')),c.get('status',cur.get('status','?')))
    print(f'КИН    : {cis}'); print(f'GTIN   : {gtin}')
    if cur.get('productName'): print(f'Товар  : {cur["productName"][:80]}')
    print(f'Тип    : {td}'+(f' ({pt})' if pt else '')); print(f'Статус : {st}')
    if cur.get('producerInn'): print(f'Произв.: {cur["producerInn"]}')
    if events:
        print(f'\nИстория движения ({len(events)}):')
        for e in events:
            s = _status_map.get(e.get('status','?'),e.get('status','?'))
            ts = e.get('timestamp','')[:19].replace('T',' '); d = e.get('docId','-'); x=''
            if e.get('parent'): x+=f' parent:{e["parent"]}'
            if e.get('child'): x+=f' children:{len(e["child"])}'
            print(f'  {ts}  {s:<12}  doc:{d}{x}')
    em = [e for e in events if e.get('status')=='EMITTED']
    ap = [e for e in events if e.get('status')=='APPLIED']
    intr = [e for e in events if e.get('status')=='INTRODUCED']
    if em: e=em[0]; print(f'\nЭмиссия: {e["docId"]}\n  Дата: {e.get("emissionDate","")[:19].replace("T"," ")}')
    if ap: print(f'\nНанесение ({len(ap)}):\n'+'\n'.join(f'  {e.get("operationDate","")[:19].replace("T"," ")}  doc:{e.get("docId","-")}' for e in ap))
    if intr: print(f'\nВведение в оборот ({len(intr)}):\n'+'\n'.join(f'  {e.get("operationDate","")[:19].replace("T"," ")}  doc:{e.get("docId","-")}' for e in intr))
    if pt=='SET':
        cc=set()
        for e in events:
            for ch in e.get('child',[]): cc.add(ch)
        if cc: print(f'\nВложения ({len(cc)}):\n'+'\n'.join(f'  - {ch}' for ch in sorted(cc)))


def main():
    p = argparse.ArgumentParser()
    p.add_argument('code',nargs='?'); p.add_argument('--token','-t'); p.add_argument('--json','-j',action='store_true')
    args = p.parse_args()
    token = args.token or find_token()
    if not token: print('Токен не найден'); sys.exit(1)
    if not args.code: print('Укажите КИН-код'); sys.exit(1)
    info = get_cis_info(args.code.strip(),token)
    history = get_cis_history(args.code.strip(),token)
    if not info and not history: print('Нет данных'); sys.exit(1)
    if args.json: print(json.dumps({'info':info,'history':history},ensure_ascii=False,indent=2))
    else: format(info,history)

if __name__=='__main__': main()
