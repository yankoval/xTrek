"""Read-only repeat-scan diagnosis; unknown/mismatching compositions fail closed."""
from .terminal_feedback_facts import safe_identifier, content_counts, STATE_LABELS


def normal(code):
    if not isinstance(code, str):
        return ''
    code = code.split('\x1d', 1)[0]
    return '00' + code if len(code) == 18 and code.isdigit() else code


def diagnose_scan(boxes, pallets, statuses, structural_errors, api, *, statuses_consistent=True):
    if not any(statuses.get(normal(c), {}).get('status') == 'INTRODUCED'
               for box in boxes for c in box.get('productNumbersFull', [])):
        return None
    if not statuses_consistent:
        items = [{'kind': 'box', 'id': safe_identifier(normal(box.get('boxNumber')), 'box'), 'position': i}
                 for i, box in enumerate(boxes, 1)]
        return {'case': 'pending', 'issues': [{'kind': 'unconfirmedbox', 'count': len(items), 'items': items}]}
    expected = {}
    for box in boxes:
        if box.get('boxNumber'):
            expected[normal(box['boxNumber'])] = {normal(c) for c in box.get('productNumbersFull', [])}
    for pallet in pallets:
        if pallet.get('palletNumber'):
            expected[normal(pallet['palletNumber'])] = {normal(b['boxNumber']) for b in pallet.get('readyBox', []) if b.get('boxNumber')}
    candidates = [c for c in expected if statuses.get(c, {}).get('status') == 'INTRODUCED']
    composition = {}
    for offset in range(0, len(candidates), 1000):
        try:
            response = api.get_aggregated_cis_list(candidates[offset:offset + 1000])
        except Exception:
            response = None
        if isinstance(response, dict) and 'error' not in response:
            # Keep only first-level identifiers for equality checks; nothing is
            # persisted from this API response.
            for code in candidates[offset:offset + 1000]:
                value = response.get(code)
                if isinstance(value, (dict, list)) and all(isinstance(c, str) for c in value):
                    normalized = {normal(c) for c in value}
                    if len(normalized) == len(value) and '' not in normalized:
                        composition[code] = normalized
    groups = {}
    repeated = ready = 0
    for position, box in enumerate(boxes, 1):
        sscc = normal(box.get('boxNumber', ''))
        codes = [normal(c) for c in box.get('productNumbersFull', [])]
        states = [statuses.get(c, {}).get('status') for c in codes]
        state = statuses.get(sscc, {}).get('status')
        if not codes or not safe_identifier(sscc, 'box') or any(not safe_identifier(c, 'product') for c in codes):
            kind = 'unconfirmedbox'
        elif state == 'INTRODUCED' and all(s == 'INTRODUCED' for s in states):
            if sscc not in composition:
                kind = 'unconfirmedbox'
            elif composition[sscc] != set(codes):
                kind = 'compositionmismatch'
            else:
                kind = 'repeatedbox'
                repeated += 1
        elif all(s == 'EMITTED' for s in states) and state == 'NOT_FOUND':
            kind = 'readybox'
            ready += 1
        elif all(s == 'INTRODUCED' for s in states):
            kind = 'introducedcontents'
        else:
            kind = 'mixedbox'
        item = {'kind': 'box', 'id': safe_identifier(sscc, 'box'), 'position': position,
                'contents': content_counts(codes, statuses)}
        if state in STATE_LABELS:
            item['state'] = state
        groups.setdefault(kind, []).append(item)
    for position, pallet in enumerate(pallets, 1):
        code = normal(pallet.get('palletNumber'))
        state = statuses.get(code, {}).get('status')
        if state == 'NOT_FOUND':
            continue
        if state == 'INTRODUCED' and code in composition:
            kind = 'repeatedpallet' if composition[code] == expected.get(code) else 'compositionmismatch'
        else:
            kind = 'unconfirmedbox'
        groups.setdefault(kind, []).append({'kind': 'pallet', 'id': safe_identifier(code, 'pallet'), 'position': position})
    structurally_ok = not structural_errors
    pallets_match = all(statuses.get(normal(p.get('palletNumber', '')), {}).get('status') == 'INTRODUCED'
                        and composition.get(normal(p.get('palletNumber', ''))) == expected.get(normal(p.get('palletNumber', '')))
                        for p in pallets)
    pallets_absent = all(statuses.get(normal(p.get('palletNumber', '')), {}).get('status') == 'NOT_FOUND' for p in pallets)
    case = 'mixed'
    if structurally_ok and repeated == len(boxes) and boxes and pallets_match:
        case = 'full_repeat'
    elif structurally_ok and repeated and ready and repeated + ready == len(boxes) and pallets_absent:
        case = 'partial_repeat'
    if 'unconfirmedbox' in groups or any(
            statuses.get(normal(c), {}).get('status') not in {'EMITTED', 'INTRODUCED', 'WRITTEN_OFF', 'RETIRED', 'DISAGGREGATED'}
            for box in boxes for c in box.get('productNumbersFull', [])):
        case = 'pending'
    # No INTRODUCED proof is ever treated as APPLIED continuation here.
    issues = [{'kind': kind, 'count': len(items), 'items': items} for kind, items in groups.items()]
    return {'case': case, 'issues': issues}
