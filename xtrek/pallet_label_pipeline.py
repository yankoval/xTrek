"""Validate equipment report/task identity and build immutable pallet PNGs."""
from __future__ import annotations

import hashlib
import json
import os
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path
from typing import Any, Callable

from .pallet_label_max_notifier import LabelMessage


class PalletLabelValidationError(ValueError):
    pass


@dataclass(frozen=True)
class PalletLabelSpec:
    report_id: str
    operator: str
    sscc: str
    index: int
    count: int
    boxes: int
    products: int


def normalize_sscc(value: Any, field: str) -> str:
    code = str(value or '').strip()
    if len(code) == 20 and code.startswith('00'):
        code = code[2:]
    if not re.fullmatch(r'\d{18}', code):
        raise PalletLabelValidationError(f'{field} must be an 18-digit SSCC')
    expected = (10 - sum(int(d) * (3 if i % 2 == 0 else 1)
                         for i, d in enumerate(reversed(code[:-1]))) % 10) % 10
    if int(code[-1]) != expected:
        raise PalletLabelValidationError(f'{field} has an invalid SSCC check digit')
    return code


def _object(storage: Any, key: str) -> dict:
    try:
        value = json.loads(storage.read_text(key))
    except Exception as exc:
        raise PalletLabelValidationError(f'Cannot read JSON object {key}') from exc
    if not isinstance(value, Mapping):
        raise PalletLabelValidationError(f'{key} must contain a JSON object')
    return dict(value)


def _array(value: Any, field: str, *, nonempty: bool = False) -> list:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise PalletLabelValidationError(f'{field} must be an array')
    result = list(value)
    if nonempty and not result:
        raise PalletLabelValidationError(f'{field} must not be empty')
    if any(not isinstance(item, Mapping) for item in result):
        raise PalletLabelValidationError(f'{field} must contain objects')
    return result


def _product_count(box: Mapping, field: str) -> int:
    values = box.get('productNumbersFull')
    if not isinstance(values, list) or not values or any(not isinstance(v, str) or not v for v in values):
        raise PalletLabelValidationError(f'{field}.productNumbersFull must be a non-empty string array')
    short = box.get('productNumbers')
    if short is not None:
        if not isinstance(short, list) or len(short) != len(values):
            raise PalletLabelValidationError(f'{field}.productNumbers differs from productNumbersFull')
        expected = [v.split('\x1d', 1)[0] for v in values]
        if short != expected:
            raise PalletLabelValidationError(f'{field}.productNumbers differs from productNumbersFull')
    return len(values)


def validate_report_task(report_key: str, report: Mapping, task_key: str,
                         task: Mapping) -> tuple[dict, list[PalletLabelSpec]]:
    """Validate the task/report join and return per-pallet label dimensions."""
    report_id = str(report.get('id') or '')
    task_id = str(task.get('id') or '')
    basename = report_key.rsplit('/', 1)[-1]
    if not report_id or not basename.endswith('.json') or basename[:-5] != report_id:
        raise PalletLabelValidationError('Report ID must equal the equipment-reports object filename')
    if task_id != report_id:
        raise PalletLabelValidationError('Equipment task ID must equal report ID')
    version = report.get('schemaVersion', 1)
    if version not in (1, 2) or task.get('reportSchemaVersion') != version:
        raise PalletLabelValidationError('Report and equipment task schema versions must match (1 or 2)')

    raw_assigned = task.get('palletNumbers')
    if not isinstance(raw_assigned, list) or not raw_assigned:
        raise PalletLabelValidationError('Equipment task palletNumbers must be a non-empty array')
    assigned = [normalize_sscc(v, f'{task_key}.palletNumbers') for v in raw_assigned]
    if len(set(assigned)) != len(assigned):
        raise PalletLabelValidationError('Equipment task palletNumbers contains duplicates')

    specs: list[PalletLabelSpec] = []
    operator = str(report.get('operator') or 'N/A')
    if version == 1:
        if len(assigned) != 1:
            raise PalletLabelValidationError('Schema v1 requires exactly one assigned pallet SSCC')
        boxes = _array(report.get('readyBox', []), 'readyBox', nonempty=True)
        product_count = sum(_product_count(box, f'readyBox[{i}]') for i, box in enumerate(boxes))
        specs.append(PalletLabelSpec(report_id, operator, assigned[0], 1, 1, len(boxes), product_count))
        return dict(report), specs

    pallets = _array(report.get('readyPallet'), 'readyPallet', nonempty=True)
    seen_pallets: set[str] = set()
    seen_boxes: set[str] = set()
    for i, pallet in enumerate(pallets):
        field = f'readyPallet[{i}]'
        if pallet.get('palletAggregate') is not True:
            raise PalletLabelValidationError(f'{field}.palletAggregate must be true')
        sscc = normalize_sscc(pallet.get('palletNumber'), f'{field}.palletNumber')
        if sscc in seen_pallets or sscc not in assigned:
            raise PalletLabelValidationError(f'{field}.palletNumber is duplicate or not assigned in task')
        seen_pallets.add(sscc)
        boxes = _array(pallet.get('readyBox'), f'{field}.readyBox', nonempty=True)
        product_count = 0
        for j, box in enumerate(boxes):
            box_field = f'{field}.readyBox[{j}]'
            if box.get('boxAgregate') is not True:
                raise PalletLabelValidationError(f'{box_field}.boxAgregate must be true')
            box_sscc = normalize_sscc(box.get('boxNumber'), f'{box_field}.boxNumber')
            if box_sscc in seen_boxes or box_sscc in assigned or box_sscc in seen_pallets:
                raise PalletLabelValidationError(f'{box_field}.boxNumber is duplicated or collides with pallet SSCC')
            seen_boxes.add(box_sscc)
            product_count += _product_count(box, box_field)
        specs.append(PalletLabelSpec(report_id, operator, sscc, i + 1, len(pallets), len(boxes), product_count))
    return dict(report), specs


def render_pallet_label_png(*, report_id: str, operator: str, pallet_number: str,
                            pallet_index: int, pallet_count: int,
                            boxes_count: int, products_count: int,
                            font_path: str | None = None) -> bytes:
    """Render the established 300-dpi GS1-128 AI 00 label locally."""
    from barcode import get_barcode_class
    from barcode.writer import ImageWriter
    from PIL import Image, ImageDraw, ImageFont

    sscc = normalize_sscc(pallet_number, 'palletNumber')
    font_candidates = [font_path, os.environ.get('XTREK_LABEL_FONT'),
                       str(Path(__file__).with_name('DejaVuSans.ttf')),
                       '/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf',
                       '/usr/share/fonts/truetype/liberation2/LiberationSans-Regular.ttf',
                       r'C:\Windows\Fonts\arial.ttf']
    font_file = next((p for p in font_candidates if p and Path(p).is_file()), None)
    if not font_file:
        raise RuntimeError('A Cyrillic-capable font is required; set XTREK_LABEL_FONT')
    fonts = {size: ImageFont.truetype(font_file, size) for size in (40, 32, 28)}

    barcode_buffer = BytesIO()
    get_barcode_class('gs1_128')(f'00{sscc}', writer=ImageWriter()).write(
        barcode_buffer, options={'format': 'PNG', 'dpi': 300, 'module_width': 0.5,
                                 'module_height': 28, 'quiet_zone': 5,
                                 'write_text': False, 'background': 'white', 'foreground': 'black'})
    barcode_image = Image.open(barcode_buffer).convert('RGB')
    width, padding = max(1200, barcode_image.width + 120), 60
    probe = ImageDraw.Draw(Image.new('RGB', (1, 1)))
    details = []
    for line in (f'Отчёт: {report_id}', f'Оператор: {operator}'):
        if probe.textbbox((0, 0), line, font=fonts[28])[2] > width - 2 * padding:
            details.extend([line[i:i + 45] for i in range(0, len(line), 45)])
        else:
            details.append(line)
    count_lines = [f'Коробов: {boxes_count}', f'Штук: {products_count}']
    height = padding + 55 + 25 + len(details) * 38 + 25 + len(count_lines) * 38 + 15 + barcode_image.height + 25 + 45 + padding
    image = Image.new('RGB', (width, height), 'white')
    draw = ImageDraw.Draw(image)
    draw.rectangle([0, 0, width - 1, height - 1], outline='black', width=5)
    title = f'ПАЛЛЕТА {pallet_index} ИЗ {pallet_count}'
    bbox = draw.textbbox((0, 0), title, font=fonts[40])
    draw.text(((width - bbox[2] + bbox[0]) / 2, padding), title, font=fonts[40], fill='black')
    y = padding + 80
    for line in details:
        bbox = draw.textbbox((0, 0), line, font=fonts[28])
        draw.text(((width - bbox[2] + bbox[0]) / 2, y), line, font=fonts[28], fill='black')
        y += 38
    y += 10
    for line in count_lines:
        bbox = draw.textbbox((0, 0), line, font=fonts[28])
        draw.text(((width - bbox[2] + bbox[0]) / 2, y), line, font=fonts[28], fill='black')
        y += 38
    y += 10
    image.paste(barcode_image, ((width - barcode_image.width) // 2, y))
    y += barcode_image.height + 20
    value = f'(00) {sscc}'
    bbox = draw.textbbox((0, 0), value, font=fonts[32])
    draw.text(((width - bbox[2] + bbox[0]) / 2, y), value, font=fonts[32], fill='black')
    output = BytesIO()
    image.save(output, format='PNG', dpi=(300, 300))
    return output.getvalue()


def prepare_report_label_messages(*, report_storage: Any, task_storage: Any,
                                  report_key: str, reports_prefix: str = 'equipment-reports',
                                  tasks_prefix: str = 'equipment-tasks',
                                  renderer: Callable[..., bytes] = render_pallet_label_png) -> list[LabelMessage]:
    """Entrypoint: read, join, validate, and render labels for one report."""
    reports_prefix = reports_prefix.strip('/')
    tasks_prefix = tasks_prefix.strip('/')
    if not report_key.startswith(reports_prefix + '/'):
        raise PalletLabelValidationError('Report key is outside equipment-reports prefix')
    report = _object(report_storage, report_key)
    report_id = str(report.get('id') or '')
    if not report_id:
        raise PalletLabelValidationError('Report id is required')
    task_key = f'{tasks_prefix}/{report_id}.json'
    if not task_storage.exists(task_key):
        raise PalletLabelValidationError(f'Equipment task is missing: {task_key}')
    task = _object(task_storage, task_key)
    report, specs = validate_report_task(report_key, report, task_key, task)
    canonical = json.dumps({'report': report, 'task': task}, ensure_ascii=False,
                           sort_keys=True, separators=(',', ':')).encode('utf-8')
    source_hash = hashlib.sha256(canonical).hexdigest()
    result = []
    for spec in specs:
        file_name = f'pallet_{spec.index}_{spec.sscc}.png'
        png = renderer(report_id=spec.report_id, operator=spec.operator,
                       pallet_number=spec.sscc, pallet_index=spec.index,
                       pallet_count=spec.count, boxes_count=spec.boxes,
                       products_count=spec.products)
        if not isinstance(png, bytes) or not png.startswith(b'\x89PNG\r\n\x1a\n'):
            raise PalletLabelValidationError('Renderer must return PNG bytes')
        text = (f'Ярлык паллета {spec.index}/{spec.count}\nОтчёт: {spec.report_id}'
                f'\nОператор: {spec.operator}\nSSCC: {spec.sscc}'
                f'\nКоробов: {spec.boxes}\nШтук: {spec.products}')
        result.append(LabelMessage(spec.report_id, spec.sscc, file_name, text, png, source_hash))
    return result


def process_equipment_report_event(*, report_storage: Any, task_storage: Any,
                                  report_key: str, notifier: Any,
                                  reports_prefix: str = 'equipment-reports',
                                  tasks_prefix: str = 'equipment-tasks',
                                  renderer: Callable[..., bytes] = render_pallet_label_png) -> list[dict]:
    """Full local event entrypoint: validate/report-task join, render, then MAX delivery."""
    labels = prepare_report_label_messages(
        report_storage=report_storage,
        task_storage=task_storage,
        report_key=report_key,
        reports_prefix=reports_prefix,
        tasks_prefix=tasks_prefix,
        renderer=renderer,
    )
    return [notifier.deliver(label) for label in labels]
