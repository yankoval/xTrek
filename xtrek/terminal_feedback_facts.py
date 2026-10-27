"""Allowlisted precheck evidence. No API payloads, exceptions or crypto tails."""
import hashlib
import re


ISSUES = {
    'repeatedpallet': 'Паллеты уже в обороте, состав совпадает',
    'repeatedbox': 'Короб и все вложения в обороте; состав совпадает — повторное сканирование',
    'readybox': 'Короба не в обороте, коды готовы',
    'mixedbox': 'Состояния SSCC и вложений требуют сверки',
    'introducedcontents': 'Вложения в обороте, SSCC не подтверждён в обороте',
    'compositionmismatch': 'Зарегистрирован другой состав упаковки',
    'unconfirmedbox': 'Состав или состояние упаковки не подтверждены',
    'alreadyregistered': 'SSCC уже использован',
    'duplicateaggregation': 'Повтор SSCC в отчёте',
    'duplicateattachment': 'Повтор кода вложения',
    'wrongsetstatus': 'Недопустимое состояние кода набора',
    'wrongunitstatus': 'Недопустимое состояние кода товара',
    'waitingstatus': 'Состояние вложения ещё не подтверждено',
    'gtinnotfound': 'Не определён вид продукции',
    'invalidreport': 'Неверная структура отчёта',
    'minssccinaggrep': 'Недостаточно коробов в отчёте',
}
STATE_LABELS = {'EMITTED': 'эмитирован', 'INTRODUCED': 'в обороте', 'APPLIED': 'нанесён',
                'WRITTEN_OFF': 'списан', 'RETIRED': 'выведен из оборота',
                'DISAGGREGATED': 'расформирован', 'NOT_FOUND': 'не найден'}


class ReportValidationResult(dict):
    """Keep the public error mapping unchanged; evidence is in-process metadata."""
    def __init__(self, errors, facts):
        super().__init__(errors)
        self.feedback_facts = facts


def safe_identifier(value, kind):
    if not isinstance(value, str):
        return None
    value = value.split('\x1d', 1)[0]
    if kind in {'box', 'pallet', 'aggregate'}:
        if re.fullmatch(r'00[0-9]{18}', value):
            value = value[2:]
        return value if re.fullmatch(r'[0-9]{18}', value) else None
    if kind == 'gtin':
        return value if re.fullmatch(r'[0-9]{14}', value) else None
    # GS1 AI 21 is at most 20 printable ASCII characters. Longer/unknown
    # representations are explicitly withheld, never truncated into another ID.
    return value if re.fullmatch(r'01[0-9]{14}21[!-~]{1,20}', value) else None


def content_counts(codes, statuses, *, consistent=True):
    """Counts for this box's current report contents, never its registered children."""
    counts = {}
    for code in codes:
        state = statuses.get(code.split('\x1d', 1)[0], {}).get('status') if consistent else None
        state = state if state in STATE_LABELS else 'UNKNOWN'
        counts[state] = counts.get(state, 0) + 1
    return counts


def report_facts(errors, boxes, pallets, statuses, children, aggregates, content, scan=None, statuses_consistent=True):
    locations = {}
    for kind, items, field in [('box', boxes, 'boxNumber'), ('pallet', pallets, 'palletNumber')]:
        for index, item in enumerate(items, 1):
            code = item.get(field, '').split('\x1d', 1)[0]
            if len(code) == 18 and code.isdigit():
                code = '00' + code
            locations.setdefault(code, (kind, index))

    problem_positions = set()
    for issue in (scan or {}).get('issues', []):
        if issue['kind'] != 'readybox':
            problem_positions.update(item['position'] for item in issue['items']
                                     if item['kind'] == 'box' and 'position' in item)

    code_positions, gtin_positions = {}, {}
    for position, box in enumerate(boxes, 1):
        code = (box.get('boxNumber') or '').split('\x1d', 1)[0]
        if len(code) == 18 and code.isdigit():
            code = '00' + code
        if code:
            code_positions.setdefault(code, set()).add(position)
        for child in box.get('productNumbersFull') or []:
            child = child.split('\x1d', 1)[0]
            code_positions.setdefault(child, set()).add(position)
            gtin_positions.setdefault(child[2:16], set()).add(position)

    def ref(code, kind):
        problem_positions.update((gtin_positions if kind == 'gtin' else code_positions).get(code, ()))
        kind, index = locations.get(code, (kind, None))
        result = {'kind': kind, 'id': safe_identifier(code, kind)}
        if index is not None:
            result['position'] = index
        state = statuses.get(code, {}).get('status')
        if state in STATE_LABELS and statuses_consistent:
            result['state'] = state
        if kind == 'box' and index is not None:
            result['contents'] = content_counts(boxes[index - 1].get('productNumbersFull') or [], statuses,
                                                consistent=statuses_consistent)
        return result

    issues = []
    for key, values in sorted(errors.items()):
        if key == 'alreadyregistered':
            codes = [c for c in aggregates if statuses.get(c, {}).get('status') not in (None, '', 'NOT_FOUND')]
            refs = [ref(c, 'aggregate') for c in sorted(codes)]
        elif key == 'duplicateaggregation':
            refs = [ref(c, 'aggregate') for c in sorted(aggregates) if aggregates[c] > 1]
        elif key == 'duplicateattachment':
            refs = [ref(c, 'product') for c in sorted(children) if children[c] > 1]
        elif key in {'wrongsetstatus', 'wrongunitstatus', 'waitingstatus'}:
            # Existing validation formats status descriptions; use them only to
            # select exact source codes, never persist those descriptions.
            refs = [ref(c, 'product') for c in sorted(children)
                    if f"{c} (Статус: {statuses.get(c, {}).get('status') or 'Не найден'})" in values]
        elif key == 'gtinnotfound':
            refs = [ref(c, 'gtin') for c in sorted(set(values))]
        else:
            refs = []
        issues.append({'kind': key if key in ISSUES else 'unknown', 'count': len(values), 'items': refs})
    if scan:
        issues = scan['issues'] + issues
    products_ok = statuses_consistent and bool(children) and all(
        safe_identifier(c, 'product') and children[c] == 1
        and statuses.get(c, {}).get('status') == 'EMITTED'
        and not statuses.get(c, {}).get('parent') for c in children)
    problem_boxes = []
    for position in sorted(problem_positions):
        box = boxes[position - 1]
        code = (box.get('boxNumber') or '').split('\x1d', 1)[0]
        if len(code) == 18 and code.isdigit():
            code = '00' + code
        item = {'kind': 'box', 'id': safe_identifier(code, 'box'), 'position': position,
                'contents': content_counts(box.get('productNumbersFull') or [], statuses, consistent=statuses_consistent)}
        state = statuses.get(code, {}).get('status')
        if state in STATE_LABELS and statuses_consistent:
            item['state'] = state
        problem_boxes.append(item)
    return {'version': 1, 'problem_boxes': problem_boxes, 'scan_case': scan['case'] if scan else None, 'source_sha256': hashlib.sha256(content.encode()).hexdigest(),
            'products_total': sum(children.values()),
            'products_checked': sum(1 for c in children if statuses.get(c, {}).get('status')),
            'products_ok': bool(products_ok and set(errors) <= {'alreadyregistered', 'duplicateaggregation'}),
            'issues': issues}


def validated_facts(facts):
    """Reject unknown schemas/fields rather than letting payloads reach the outbox."""
    if facts is None:
        return None
    if not isinstance(facts, dict) or set(facts) - {'problem_boxes'} != {
        'version', 'scan_case', 'source_sha256', 'products_total', 'products_checked', 'products_ok', 'issues'
    }:
        return None
    if facts['version'] != 1 or not isinstance(facts['source_sha256'], str) or not re.fullmatch(r'[a-f0-9]{64}', facts['source_sha256']):
        return None
    if any(type(facts[k]) is not int or facts[k] < 0 for k in ('products_total', 'products_checked')):
        return None
    if type(facts['products_ok']) is not bool or not isinstance(facts['issues'], list) or not facts['issues']:
        return None
    if facts['scan_case'] not in (None, 'full_repeat', 'partial_repeat', 'mixed', 'pending'):
        return None
    extra = facts.get('problem_boxes', [])
    if not isinstance(extra, list) or any(not isinstance(i, dict) or i.get('kind') != 'box' for i in extra):
        return None
    kinds = []
    all_issues = facts['issues'] + ([{'kind': 'unknown', 'count': len(extra), 'items': extra}] if extra else [])
    for index, issue in enumerate(all_issues):
        if not isinstance(issue, dict) or set(issue) != {'kind', 'count', 'items'}:
            return None
        if issue['kind'] not in {*ISSUES, 'unknown'} or type(issue['count']) is not int or issue['count'] < 1:
            return None
        if not isinstance(issue['items'], list) or len(issue['items']) > issue['count']:
            return None
        if index < len(facts['issues']):
            kinds.append(issue['kind'])
        for item in issue['items']:
            if not isinstance(item, dict) or not {'kind', 'id'} <= set(item) <= {'kind', 'id', 'position', 'state', 'contents'}:
                return None
            if item['kind'] not in {'box', 'pallet', 'aggregate', 'product', 'gtin'}:
                return None
            if item['id'] is not None and safe_identifier(item['id'], item['kind']) != item['id']:
                return None
            if 'state' in item and item['state'] not in STATE_LABELS:
                return None
            if 'contents' in item:
                counts = item['contents']
                if (item['kind'] != 'box' or not isinstance(counts, dict)
                        or any(k not in {*STATE_LABELS, 'UNKNOWN'} or type(v) is not int or v < 1
                               for k, v in counts.items())):
                    return None
                if facts['products_ok'] and set(counts) != {'EMITTED'}:
                    return None
            if 'position' in item and (type(item['position']) is not int or item['position'] < 1):
                return None
    if len(set(kinds)) != len(kinds):
        return None
    if facts['products_ok'] and (not facts['products_total'] or facts['products_checked'] != facts['products_total']
                                or not set(kinds) <= {'alreadyregistered', 'duplicateaggregation'}):
        return None
    return facts
