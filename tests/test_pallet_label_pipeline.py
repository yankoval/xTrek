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
