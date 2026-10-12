import json
from collections import Counter

import pytest
from xtrek.kinGenerator import generate_kin_report_from_files, KinReportGenerator

SET, A, B = '04670404500602', '04670404501272', '04670404501289'


def make_inputs(tmp_path, *, counts=(3, 12, 9), packs=None, total=7):
    recipe = {'Hierarchy': [
        {'LevelType': 'Kit', 'Packs': [{'GTIN': g} for g in (packs or [A]*4 + [B]*3)]},
        {'LevelType': 'Kigu', 'Packs': [{'GTIN': SET, 'CountPacksInside': total}]}]}
    paths = []
    for name, data in [('recipe', recipe)] + [
        (g, {'codes': ['01'+g+'21'+str(i).zfill(6)+'\x1d93TEST' for i in range(n)]})
        for g, n in zip((SET, A, B), counts)
    ]:
        p = tmp_path / (name+'.json')
        p.write_text(json.dumps(data))
        paths.append(str(p))
    return paths


def test_four_plus_three_unique_children_and_reserve(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    paths = make_inputs(tmp_path, counts=(3001, 12000, 9000))
    out = generate_kin_report_from_files(paths, 3000, strict_quantity=True)
    boxes = json.loads((tmp_path / out).read_text())['readyBox']
    assert len(boxes) == 3000
    assert all(Counter(c[2:16] for c in box['productNumbersFull']) == {A: 4, B: 3} for box in boxes)
    children = [c.split('\x1d')[0] for box in boxes for c in box['productNumbersFull']]
    assert len(children) == len(set(children)) == 21000
    assert boxes[-1]['boxNumber'][18:24] == '002999'


@pytest.mark.parametrize('quantity,strict', [(3000, False), (None, True)])
def test_shortage_does_not_silently_truncate(tmp_path, monkeypatch, quantity, strict):
    monkeypatch.chdir(tmp_path)
    paths = make_inputs(tmp_path, counts=(3001, 3001, 3001))
    assert generate_kin_report_from_files(paths, quantity, strict_quantity=strict) is None
    assert not (tmp_path / (SET+'_kin_report.json')).exists()


def test_legacy_maximum_remains_available(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    out = generate_kin_report_from_files(make_inputs(tmp_path, counts=(10, 8, 9)))
    assert len(json.loads((tmp_path / out).read_text())['readyBox']) == 2


def test_rejects_incomplete_recipe(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    assert generate_kin_report_from_files(make_inputs(tmp_path, packs=[A, B])) is None


@pytest.mark.parametrize('corruption', ['same_code', 'different_crypto', 'invalid_code'])
def test_rejects_duplicate_or_invalid_children(tmp_path, monkeypatch, corruption):
    monkeypatch.chdir(tmp_path)
    paths = make_inputs(tmp_path)
    p = tmp_path / (A+'.json')
    data = json.loads(p.read_text())
    data['codes'][1] = {'same_code': data['codes'][0],
                        'different_crypto': data['codes'][0].split('\x1d')[0]+'\x1d93OTHER',
                        'invalid_code': 'broken'}[corruption]
    p.write_text(json.dumps(data))
    assert generate_kin_report_from_files(paths, 3) is None


def test_zero_capacity_without_name_error(tmp_path, monkeypatch, caplog):
    monkeypatch.chdir(tmp_path)
    assert generate_kin_report_from_files(make_inputs(tmp_path, counts=(3, 1, 1))) is None
    assert 'all_kit_codes' not in caplog.text


def test_ai21_inside_gtin():
    assert KinReportGenerator().extract_short_code('010462100000000121ABCDEF\x1d93TEST') == 'ABCDEF'


def test_xlsx_preserves_live_bom_and_codes_support(tmp_path):
    from xtrek.jsontoxlsx import convert_json_to_xlsx
    from openpyxl import load_workbook
    p = tmp_path / 'codes.json'
    p.write_text(json.dumps({'codes': ['01'+A+'21ABCDEF\x1d93TEST']}), encoding='utf-8-sig')
    book = load_workbook(convert_json_to_xlsx(p))
    assert book.active['A2'].value == '01'+A+'21ABCDEF'
