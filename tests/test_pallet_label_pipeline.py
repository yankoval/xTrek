import json

import pytest

from xtrek.pallet_label_pipeline import (
    PalletLabelValidationError,
    process_equipment_report_event,
    prepare_report_label_messages,
    validate_report_task,
)


REPORT_ID = 'report-0001'
PALLET_1 = '346070517900000057'
PALLET_2 = '346070517900000064'
BOX_1 = '000000000000000024'


class FakeStorage:
    def __init__(self, values):
        self.values = values

    def exists(self, key):
        return key in self.values

    def read_text(self, key):
        return json.dumps(self.values[key], ensure_ascii=False)


def report_v2():
    return {
        'schemaVersion': 2,
        'id': REPORT_ID,
        'operator': 'Оператор 1',
        'readyPallet': [
            {'palletAggregate': True, 'palletNumber': PALLET_1,
             'readyBox': [{'boxAgregate': True, 'boxNumber': BOX_1,
                           'productNumbers': ['01CODE'],
                           'productNumbersFull': ['01CODE\x1d93TAIL']}]},
            {'palletAggregate': True, 'palletNumber': PALLET_2,
             'readyBox': [{'boxAgregate': True, 'boxNumber': '000000000000000031',
                           'productNumbersFull': ['02CODE\x1d93TAIL', '03CODE\x1d93TAIL']}]},
        ],
    }


def task_v2():
    return {'id': REPORT_ID, 'reportSchemaVersion': 2,
            'palletNumbers': [PALLET_1, PALLET_2]}


def test_entrypoint_joins_report_and_task_renders_each_assigned_pallet():
    reports = FakeStorage({'equipment-reports/report-0001.json': report_v2()})
    tasks = FakeStorage({'equipment-tasks/report-0001.json': task_v2()})
    rendered = []

    def renderer(**kwargs):
        rendered.append(kwargs)
        return b'\x89PNG\r\n\x1a\nfixture'

    labels = prepare_report_label_messages(
        report_storage=reports, task_storage=tasks,
        report_key='equipment-reports/report-0001.json', renderer=renderer,
    )
    assert [label.file_name for label in labels] == [
        f'pallet_1_{PALLET_1}.png', f'pallet_2_{PALLET_2}.png']
    assert [call['products_count'] for call in rendered] == [1, 2]
    assert len({label.source_hash for label in labels}) == 1
    assert all(label.report_id == REPORT_ID for label in labels)


@pytest.mark.parametrize('change, message', [
    (lambda report, task: task.update(id='other'), 'task ID'),
    (lambda report, task: report['readyPallet'][0].update(palletNumber='346070517900000064'), 'duplicate or not assigned'),
    (lambda report, task: report['readyPallet'][0]['readyBox'][0].update(boxAgregate=False), 'boxAgregate'),
    (lambda report, task: task.update(palletNumbers=[PALLET_1, PALLET_1]), 'contains duplicates'),
])
def test_validator_blocks_mismatched_or_open_data(change, message):
    report, task = report_v2(), task_v2()
    change(report, task)
    with pytest.raises(PalletLabelValidationError, match=message):
        validate_report_task('equipment-reports/report-0001.json', report,
                             'equipment-tasks/report-0001.json', task)


def test_renderer_output_must_be_png():
    reports = FakeStorage({'equipment-reports/report-0001.json': report_v2()})
    tasks = FakeStorage({'equipment-tasks/report-0001.json': task_v2()})
    with pytest.raises(PalletLabelValidationError, match='PNG bytes'):
        prepare_report_label_messages(
            report_storage=reports, task_storage=tasks,
            report_key='equipment-reports/report-0001.json', renderer=lambda **_: b'bad',
        )


def test_missing_task_blocks_before_renderer():
    reports = FakeStorage({'equipment-reports/report-0001.json': report_v2()})
    with pytest.raises(PalletLabelValidationError, match='missing'):
        prepare_report_label_messages(
            report_storage=reports, task_storage=FakeStorage({}),
            report_key='equipment-reports/report-0001.json', renderer=lambda **_: pytest.fail(),
        )


def test_event_entrypoint_passes_only_validated_rendered_messages_to_notifier():
    reports = FakeStorage({'equipment-reports/report-0001.json': report_v2()})
    tasks = FakeStorage({'equipment-tasks/report-0001.json': task_v2()})

    class Notifier:
        def __init__(self):
            self.items = []

        def deliver(self, item):
            self.items.append(item)
            return {'state': 'GET_ONLY_FIXTURE', 'sscc': item.sscc}

    notifier = Notifier()
    results = process_equipment_report_event(
        report_storage=reports, task_storage=tasks,
        report_key='equipment-reports/report-0001.json', notifier=notifier,
        renderer=lambda **_: b'\x89PNG\r\n\x1a\nfixture',
    )
    assert [result['sscc'] for result in results] == [PALLET_1, PALLET_2]
    assert all(item.file_name.endswith('.png') for item in notifier.items)


def make_sscc(serial, namespace='0460705179'):
    body = namespace + f'{serial:07d}'
    check = (10 - sum(int(d) * (3 if i % 2 == 0 else 1) for i, d in enumerate(reversed(body))) % 10) % 10
    return body + str(check)


def test_compact_ranges_match_cloud_with_leading_zeros_gaps_and_ai00():
    from xtrek.pallet_label_pipeline import compact_box_numbers
    codes = [make_sscc(n) for n in [5, 2, 1, 2, 8, 9]]
    assert compact_box_numbers([{'boxNumber': '00' + c} for c in codes]) == '0000001-0000002, 0000005, 0000008-0000009'


def test_serial_ranges_reject_ambiguous_namespaces_and_bad_check_digits():
    from xtrek.pallet_label_pipeline import compact_box_numbers
    with pytest.raises(PalletLabelValidationError, match='namespaces'):
        compact_box_numbers([{'boxNumber': make_sscc(1)}, {'boxNumber': make_sscc(2, '1460705179')}])
    with pytest.raises(PalletLabelValidationError, match='check digit'):
        compact_box_numbers([{'boxNumber': make_sscc(1)[:-1] + '0'}])


def test_each_pallet_caption_and_renderer_get_only_its_boxes_and_source_files():
    calls = []
    labels = prepare_report_label_messages(
        report_storage=FakeStorage({'equipment-reports/report-0001.json': report_v2()}),
        task_storage=FakeStorage({'equipment-tasks/report-0001.json': task_v2()}),
        report_key='equipment-reports/report-0001.json',
        renderer=lambda **kw: calls.append(kw) or b'\x89PNG\r\n\x1a\nfixture')
    assert [c['box_numbers'] for c in calls] == ['0000002', '0000003']
    for i, label in enumerate(labels):
        assert f"Номера коробов: {calls[i]['box_numbers']}" in label.text
        assert 'Отчёт-файл: equipment-reports/report-0001.json' in label.text
        assert 'Задание-файл: equipment-tasks/report-0001.json' in label.text
        assert '93TAIL' not in label.text


def test_v1_boxes_are_included_in_single_assigned_pallet():
    report = {'id': REPORT_ID, 'readyBox': [{'boxNumber': make_sscc(n), 'productNumbersFull': ['fixture']} for n in (1, 2, 5)]}
    task = {'id': REPORT_ID, 'reportSchemaVersion': 1, 'palletNumbers': [PALLET_1]}
    _, specs = validate_report_task(f'equipment-reports/{REPORT_ID}.json', report, '', task)
    assert specs[0].box_numbers == '0000001-0000002, 0000005'


@pytest.mark.parametrize('ranges', ['0000001-0000010', ', '.join(f'{n:07d}' for n in range(0, 700, 2))])
def test_rendered_label_contains_complete_wrapped_ranges_and_decodable_barcode(monkeypatch, ranges):
    from io import BytesIO
    from PIL import Image, ImageDraw
    from xtrek.pallet_label_pipeline import render_pallet_label_png
    import zxingcpp
    drawn = []
    original = ImageDraw.ImageDraw.text
    def text(self, xy, value, *args, **kw):
        if isinstance(value, str): drawn.append((xy, value, kw.get('font')))
        return original(self, xy, value, *args, **kw)
    monkeypatch.setattr(ImageDraw.ImageDraw, 'text', text)
    data = render_pallet_label_png(report_id='Синтетический отчёт', operator='Оператор',
        pallet_number=PALLET_1, pallet_index=1, pallet_count=1, boxes_count=350,
        products_count=700, box_numbers=ranges, font_path='/Library/Fonts/Arial Unicode.ttf')
    image = Image.open(BytesIO(data))
    assert image.width <= 7680 and image.height <= 7680
    assert any('Номера коробов:' in v for _, v, _ in drawn)
    # Whitespace may be removed only at wrap boundaries; every serial survives.
    all_text = ''.join(v.replace(' ', '') for _, v, _ in drawn)
    assert ranges.replace(' ', '') in all_text
    for (x, y), value, font in drawn:
        if font:
            assert x >= 50
            assert x + ImageDraw.Draw(image).textlength(value, font=font) <= image.width - 50
            assert y < image.height - 40
    result = zxingcpp.read_barcode(image)
    assert result is not None
    assert result.text.replace('(', '').replace(')', '').endswith('00' + PALLET_1)


def test_label_rejects_unbounded_ranges_instead_of_clipping():
    from xtrek.pallet_label_pipeline import render_pallet_label_png
    with pytest.raises(PalletLabelValidationError, match='dimensions'):
        render_pallet_label_png(report_id='fixture', operator='fixture', pallet_number=PALLET_1,
            pallet_index=1, pallet_count=1, boxes_count=10000, products_count=10000,
            box_numbers=', '.join(f'{n:07d}' for n in range(10000)), font_path='/Library/Fonts/Arial Unicode.ttf')
