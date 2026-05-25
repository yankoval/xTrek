#!/usr/bin/env python3
"""
create_test_eqrep.py — создание тестового отчёта оборудования.

Берёт задание на производство (production_order_id), получает:
  - T-коды агрегатов SET из kodes/{order_id_t}.json
  - V-коды вложений из kodes/{order_id_v}.json
  - SSCC для коробок через SSCC_Utils.get_sscc_from_service()

Формирует JSON в формате equipment-report и сохраняет локально.

Usage:
  python3 create_test_eqrep.py <production_order_id> [--boxes N] [--output FILE] [--sscc-url URL] [--sscc-prefix PREFIX]

Пример:
  python3 create_test_eqrep.py T-7148-77-002-2546-C-05-d7654023-a6cc-4095-b8bc-cd14b41f9b4c --boxes 3
"""

import sys, os, json, re, argparse
from datetime import datetime, timezone, timedelta
import random
from pathlib import Path
from uuid import uuid4

# --- Пути проектов ---
XTREK_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(XTREK_DIR / 'xtrek'))

cfg_path = os.environ.get('suz_worker_config',
                          os.path.expanduser('~/python-projects/suz_worker_config.json'))

# ────────────────────────────── CONFIG & S3 ──────────────────────────────

def load_cfg():
    with open(cfg_path) as f:
        return json.load(f)

def get_s3_client(cfg):
    import boto3
    sc = cfg.get('s3_config', {})
    return boto3.client('s3',
        endpoint_url=sc.get('endpoint_url', 'https://storage.yandexcloud.net'),
        aws_access_key_id=sc.get('aws_access_key_id', ''),
        aws_secret_access_key=sc.get('aws_secret_access_key', ''),
        region_name=sc.get('region_name', 'ru-central1'))

def get_bucket(cfg):
    for v in cfg.values():
        m = re.match(r's3://([^/]+)', str(v))
        if m:
            return m.group(1)
    return ''

# ──────────────────────────── КОДЫ ИЗ S3 ─────────────────────────────

def s3_read(s3, bucket, key):
    try:
        resp = s3.get_object(Bucket=bucket, Key=key)
        return json.loads(resp['Body'].read())
    except Exception as e:
        return None

def get_v_codes(s3, bucket, pid):
    """
    V-коды: emissionReceipts/V-{pid}*.json → orderId → kodes/{orderId}.json
    """
    v_prefix = f'emissionReceipts/V-{pid}'
    paginator = s3.get_paginator('list_objects_v2')
    v_files = []
    for page in paginator.paginate(Bucket=bucket, Prefix=v_prefix):
        for obj in page.get('Contents', []):
            key = obj['Key']
            if key.endswith('/'):
                continue
            dd = s3_read(s3, bucket, key)
            if dd:
                oid = dd.get('orderId', '')
                if oid:
                    v_files.append((key, oid))

    if not v_files:
        return [], None

    all_codes = []
    first_oid = v_files[0][1] if v_files else None
    for vk, oid in v_files:
        kd_key = f'kodes/{oid}.json'
        kd_data = s3_read(s3, bucket, kd_key)
        if kd_data:
            codes = kd_data.get('codes', [])
            if codes:
                print(f'  V-эмиссия {vk.split("/")[-1]}: orderId={oid}, кодов={len(codes)}')
                all_codes.extend(codes)

    return all_codes, first_oid


def get_t_codes(s3, bucket, pid, first_oid):
    """
    T-коды: сначала kodes/{first_oid}.json, затем kodes/T-{pid}.json
    """
    candidates = [f'kodes/{first_oid}.json'] if first_oid else []
    if pid.startswith('T-'):
        candidates.append(f'kodes/T-{pid[2:]}.json')
    else:
        candidates.append(f'kodes/T-{pid}.json')
    candidates.append(f'kodes/{pid}.json')

    seen = set()
    for kd_key in candidates:
        if kd_key in seen:
            continue
        seen.add(kd_key)
        kd_data = s3_read(s3, bucket, kd_key)
        if kd_data:
            codes = kd_data.get('codes', [])
            if codes:
                print(f'  T-коды: {len(codes)} кодов из {kd_key}')
                return codes
            else:
                print(f'  T-коды пусты в {kd_key}')

    print('  T-коды не найдены')
    return []


def clean_code(raw):
    if not isinstance(raw, str):
        return ''
    return raw.split('\x1d')[0].strip()


# ─────────────────────── ФОРМИРОВАНИЕ JSON ──────────────────────

def build_report(v_codes_raw, t_codes_raw, sscc_list, pid):
    v_clean = [clean_code(c) for c in v_codes_raw if clean_code(c)]
    t_clean = [clean_code(c) for c in t_codes_raw if clean_code(c)]

    num_boxes = len(sscc_list)
    items_per_box = max(len(v_clean) // num_boxes, 1) if v_clean else 0
    ready_boxes = []
    start_time = datetime.now(timezone.utc)

    for i in range(num_boxes):
        start_idx = i * items_per_box
        if i < num_boxes - 1:
            end_idx = start_idx + items_per_box
        else:
            end_idx = len(v_clean)

        box_codes = v_clean[start_idx:end_idx]
        # productNumbersFull — оригинальные (с криптохвостом), если хватает
        original = v_codes_raw[start_idx:end_idx] if start_idx < len(v_codes_raw) else []
        sscc = sscc_list[i]
        box_time = (start_time + timedelta(minutes=random.randint(2, 30))).isoformat()

        ready_boxes.append({
            "Number": i,
            "boxNumber": sscc,
            "boxAgregate": True,
            "boxTime": box_time,
            "productNumbers": box_codes,
            "productNumbersFull": original,
        })
        print(f'  Коробка {i+1}: SSCC={sscc}, кодов={len(box_codes)}')

    report = {
        "id": str(uuid4()),
        "startTime": start_time.isoformat(),
        "endTime": datetime.now(timezone.utc).isoformat(),
        "operators": [],
        "readyBox": ready_boxes,
        "sampleNumbers": [],
        "sampleNumbersFull": None,
        "defectiveCodes": None,
        "defectiveCodesFull": None,
        "emptyNumbers": None,
    }

    report["build"] = "001"

    return report


# ────────────────────────── MAIN ──────────────────────────────

def main():
    p = argparse.ArgumentParser(
        description='Создание тестового отчёта оборудования')
    p.add_argument('production_order_id')
    p.add_argument('--boxes', '-b', type=int, default=1)
    p.add_argument('--output', '-o')
    p.add_argument('--sscc-url',
        default='https://functions.yandexcloud.net/d4et2pvmtgp0oo5pk0bh')
    p.add_argument('--sscc-prefix', default='460705179')
    p.add_argument('--sscc-extension', default='0')
    args = p.parse_args()

    pid = args.production_order_id.replace('.json', '').strip()
    num_boxes = max(args.boxes, 1)

    print(f'Задание на производство: {pid}')
    print(f'Коробок: {num_boxes}')

    cfg = load_cfg()
    s3 = get_s3_client(cfg)
    bucket = get_bucket(cfg)
    print(f'Bucket: {bucket}')

    # 1. V-коды
    print('\n=== V-коды (вложения) ===')
    v_codes, first_oid = get_v_codes(s3, bucket, pid)
    if not v_codes:
        print('V-коды не найдены')
        sys.exit(1)
    print(f'Всего V-кодов: {len(v_codes)}')

    # 2. T-коды (SET)
    print('\n=== T-коды (SET-агрегаты) ===')
    t_codes = get_t_codes(s3, bucket, pid, first_oid)

    # 3. SSCC
    print('\n=== SSCC ===')
    sscc_list = []
    try:
        from xtrek.SSCC_Utils import get_sscc_from_service
        sscc_list = get_sscc_from_service(
            args.sscc_url, args.sscc_prefix, num_boxes,
            args.sscc_extension if args.sscc_extension else None)
    except Exception as e:
        print(f'  [WARN] SSCC сервис: {e}')

    if not sscc_list:
        print('  [WARN] Создаём тестовые SSCC')
        sscc_list = [f'{args.sscc_prefix}{str(i+1).zfill(12)}' for i in range(num_boxes)]
    print(f'SSCC: {len(sscc_list)}')

    # 4. Формируем
    print('\n=== Формирование ===')
    report = build_report(v_codes, t_codes, sscc_list, pid)

    # 5. Сохраняем
    out_path = args.output or os.path.join(
        os.path.expanduser('~/Downloads'),
        f'{pid[:60].replace("/", "_")}_eqrep.json')

    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    total_kiz = sum(len(b.get('productNumbers', [])) for b in report['readyBox'])
    print(f'\nСохранено: {out_path}')
    print(f'Коробок: {len(report["readyBox"])}, КИЗ: {total_kiz}')


if __name__ == '__main__':
    main()
