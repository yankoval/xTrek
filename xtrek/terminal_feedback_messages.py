"""Pure deterministic operator text and complete, separate support details."""
import argparse
import hashlib
import json
import re
import sqlite3
from urllib.parse import quote
from pathlib import Path

from .terminal_feedback import ACTIONS, display_value
from .terminal_feedback_facts import ISSUES, STATE_LABELS, validated_facts

MAX_MESSAGE_LENGTH = 3500
RESCAN_BOX = 'Замените паспорт короба и сканируйте весь паллет заново'
VERIFY_CONTENTS = 'Передайте отчёт ответственному для сверки кодов и состава упаковки.'
RESCAN_REMAINDER = 'Проверьте короба в приложении «ЧЗ Бизнес» и повторно сканируйте только короба не в обороте.'
TEMPLATES = {
    'report_repeat_scan': ('Повторное сканирование по ошибке: все SSCC и коды уже в обороте.', 'Не сканируйте эти короба повторно.'),
    'report_partial_repeat': ('Частичное повторное сканирование.', RESCAN_REMAINDER),
    'report_scan_conflict': ('Состояния или состав SSCC и вложений различаются.', VERIFY_CONTENTS),
    'suz_expiration_date_rejected': ('СУЗ отклонил нанесение: срок годности раньше текущей даты.', 'Согласуйте исправление срока годности в задании.'),
    'suz_production_date_range_rejected': ('СУЗ отклонил нанесение: дата производства вне диапазона 1970-01-01 — 2100-01-01.', 'Согласуйте исправление фактической даты производства в задании.'),
    'suz_production_date_future_rejected': ('СУЗ отклонил нанесение: дата производства позже текущей даты.', 'Согласуйте исправление фактической даты производства в задании.'),
    'report_status_rejected': ('Недопустимое состояние кодов вложений.', VERIFY_CONTENTS),
    'report_aggregate_conflict': ('SSCC уже использован; состояние вложений не подтверждено.', VERIFY_CONTENTS),
    'report_access_denied': ('Нет доступа к проверке отчёта в Честном ЗНАКЕ.', 'Передайте отчёт администратору для проверки полномочий организации.'),
    'introduction_rejected': ('Честный ЗНАК отклонил ввод в оборот.', 'Передайте отчёт ответственному для проверки отказа документа.'),
    'utilisation_rejected': ('СУЗ отклонил отчёт о нанесении.', 'Передайте отчёт ответственному для проверки отказа документа.'),
    'cis_rejected': ('Честный ЗНАК отклонил корректировку кодов.', 'Передайте отчёт ответственному для проверки отказа корректировки.'),
    'aggregation_rejected': ('Честный ЗНАК отклонил агрегацию.', VERIFY_CONTENTS),
    'invalid_report': ('Некорректные данные сканирования.', VERIFY_CONTENTS),
    'empty_report': ('Отчёт пуст или содержит неполные данные сканирования.', 'Повторно выгрузите задание и отсканируйте продукцию заново.'),
    'retry_exhausted': ('Обработка требует проверки ресурсов и документа.', 'Передайте отчёт ответственному для согласования продолжения.'),
    'reconciliation_required': ('Результат обработки требует сверки.', 'Передайте отчёт ответственному для сверки журнала операций.'),
}


def _facts(row):
    try:
        return validated_facts(json.loads(row.get('facts_json') or 'null'))
    except (ValueError, TypeError):
        return None


def _contents_text(counts):
    total = sum(counts.values())
    if not total:
        return 'состояние вложений не подтверждено'
    if set(counts) == {'EMITTED'}:
        return f'вложения из отчёта ещё не в обороте: {total} из {total} (эмитированы)'
    introduced = counts.get('INTRODUCED', 0)
    parts = []
    if introduced:
        parts.append(f'вложения из отчёта уже в обороте: {introduced} из {total}')
    for state, label in [('EMITTED', 'ещё не в обороте (эмитированы)'),
                         ('APPLIED', 'нанесены, ввод не подтверждён'),
                         ('WRITTEN_OFF', 'списаны'), ('RETIRED', 'выведены из оборота'),
                         ('DISAGGREGATED', 'расформированы'), ('NOT_FOUND', 'не найдены'),
                         ('UNKNOWN', 'статус не подтверждён')]:
        if counts.get(state):
            parts.append(f'{label}: {counts[state]}')
    return '; '.join(parts)


def _item(item):
    label = {'box': 'короб', 'pallet': 'паллета', 'aggregate': 'SSCC', 'product': 'КИ', 'gtin': 'GTIN'}[item['kind']]
    position = f" №{item['position']}" if 'position' in item else ''
    state = f" ({STATE_LABELS[item['state']]})" if item.get('state') else ''
    text = f"{label}{position}: {item['id']}{state}" if item['id'] else f'{label}{position}: номер скрыт (неизвестный формат)'
    if item['kind'] == 'box' and 'contents' in item:
        if not item.get('state'):
            text += ' (статус короба не подтверждён)'
        text += '; ' + _contents_text(item['contents'])
    return text


def issue_text(issue, *, limit=None):
    items = issue['items'] if limit is None else issue['items'][:limit]
    result = f"{ISSUES.get(issue['kind'], 'Дополнительная ошибка проверки')} — {issue['count']}"
    if items:
        result += '; ' + '; '.join(_item(item) for item in items)
    remaining = issue['count'] - len(items)
    if remaining:
        result += f'; ещё {remaining} без номеров' if limit is None else f'; ещё {remaining} — в деталях'
    return result


def compact_message(row):
    context = json.loads(row.get('context_json') or '{}')
    report_id = context.get('report_id', row['production_order_id'])
    reason, action = TEMPLATES[row['category']]
    facts = _facts(row)
    if facts:
        issues = facts['issues']
        reason = ' / '.join(issue_text(issue, limit=2) for issue in issues[:2])
        if len(issues) > 2:
            reason += f" / Ещё видов ошибок: {len(issues) - 2} — в деталях"
        only_boxes = all(issue['items'] and len(issue['items']) == issue['count']
                         and all(item['kind'] == 'box' and item['id'] for item in issue['items'])
                         for issue in issues)
        if facts['products_ok'] and only_boxes and {i['kind'] for i in issues} <= {'alreadyregistered', 'duplicateaggregation'}:
            reason += f". Во всём отчёте ещё не в обороте: {facts['products_checked']} из {facts['products_total']} кодов (эмитированы)."
            action = RESCAN_BOX
        elif facts['scan_case'] == 'full_repeat' and row['category'] == 'report_repeat_scan':
            reason = 'Повторное сканирование по ошибке. ' + reason
        elif facts['scan_case'] == 'partial_repeat' and row['category'] == 'report_partial_repeat':
            reason = 'Частичное повторное сканирование. ' + reason
        else:
            action = VERIFY_CONTENTS
    attachment = make_attachment(row)
    details_line = (f"Проблемных коробов: {attachment['box_count']}. Полный список — в приложенном файле."
                    if attachment else f"Детали и полный список: у поддержки по обращению {row['event_id'][:16]}.")
    lines = [
        f"xTrek • Артикул: {display_value(context.get('article'), 80)} • Партия: {display_value(context.get('batch'), 80)}",
        reason,
        action,
        'Обработка остановлена до сверки.',
        f"ID отчёта: {display_value(report_id, 1500)}",
        details_line,
    ]
    text = '\n'.join(lines)
    if len(text) > MAX_MESSAGE_LENGTH:
        raise ValueError('Feedback exceeds safe message limit')
    return text


def details_text(row):
    """Full lists from the frozen event only; never reread mutable production data."""
    context = json.loads(row.get('context_json') or '{}')
    lines = [f"ID отчёта: {display_value(context.get('report_id', row.get('production_order_id')), 1500)}",
             f"Обращение: {row['event_id']}", ACTIONS[row['category']]]
    facts = _facts(row)
    if facts:
        lines.extend([f"SHA256 отчёта: {facts['source_sha256']}",
                      f"Вложений: {facts['products_total']}; статусов получено: {facts['products_checked']}"])
        for issue in facts['issues']:
            lines.append(f"{ISSUES.get(issue['kind'], 'Дополнительная ошибка проверки')} — {issue['count']}")
            lines.extend(_item(item) for item in issue['items'])
            missing = issue['count'] - len(issue['items'])
            if missing:
                lines.append(f'Без номеров: {missing}; исходные значения не включены в безопасный снимок.')
    else:
        lines.append('Подробные факты в этом событии не сохранены; нужна отдельная диагностика.')
    return '\n'.join(lines) + '\n'


def problem_boxes(facts):
    boxes = {}
    items = list(facts.get('problem_boxes', []))
    for issue in facts['issues']:
        if issue['kind'] != 'readybox':
            items.extend(item for item in issue['items'] if item['kind'] == 'box')
    for item in items:
        # A duplicate SSCC at two physical positions means two problem boxes.
        key = ('position', item['position']) if 'position' in item else ('sscc', item['id'])
        boxes.setdefault(key, item)
    return list(boxes.values())


def make_attachment(row):
    facts = _facts(row)
    boxes = problem_boxes(facts) if facts else []
    if len(boxes) <= 10:
        return None
    text = (details_text(row) + f"\nПолный список проблемных коробов: {len(boxes)}\n"
            + '\n'.join(_item(item) for item in boxes) + '\n')
    data = text.encode('utf-8')
    digest = hashlib.sha256(data).hexdigest()
    return {'version': 1, 'filename': f"xtrek-boxes-{row['event_id'][:16]}-{digest[:16]}.txt",
            'text': text, 'sha256': digest, 'size': len(data), 'box_count': len(boxes)}


def frozen_attachment(row):
    value = json.loads(row.get('attachment_json') or 'null')
    if value is None:
        return None
    if (not isinstance(value, dict) or value.get('version') != 1
            or not isinstance(value.get('text'), str)
            or not re.fullmatch(r'xtrek-boxes-[0-9a-f]{16}-[0-9a-f]{16}\.txt', value.get('filename', ''))):
        raise ValueError('Invalid frozen attachment')
    data = value['text'].encode('utf-8')
    digest = hashlib.sha256(data).hexdigest()
    if len(data) != value.get('size') or digest != value.get('sha256') or not value['filename'].endswith(digest[:16] + '.txt'):
        raise ValueError('Frozen attachment checksum mismatch')
    return value


def read_sqlite_event(path, reference):
    if not re.fullmatch(r'[0-9a-f]{16,64}', reference):
        raise ValueError('Use the support reference or full event ID')
    with sqlite3.connect('file:' + quote(str(Path(path).resolve())) + '?mode=ro', uri=True) as db:
        db.row_factory = sqlite3.Row
        rows = db.execute('SELECT * FROM terminal_events WHERE event_id LIKE ? LIMIT 2', (reference + '%',)).fetchall()
    if len(rows) != 1:
        raise ValueError('Reference is absent or ambiguous; use the full event ID')
    return dict(rows[0])


def main():
    parser = argparse.ArgumentParser(description='Read frozen terminal evidence; print complete safe details')
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument('--event-json', type=Path, help='Exported S3 terminal event')
    source.add_argument('--sqlite', type=Path, help='Read-only access; no migrations or initialization')
    parser.add_argument('--reference', help='Support reference or full event ID for SQLite')
    args = parser.parse_args()
    if args.sqlite and not args.reference:
        parser.error('--sqlite requires --reference')
    row = (read_sqlite_event(args.sqlite, args.reference) if args.sqlite
           else json.loads(args.event_json.read_text()))
    print(details_text(row), end='')


if __name__ == '__main__':
    main()
