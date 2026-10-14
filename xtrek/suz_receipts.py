"""Read and retain per-CIS utilisation errors without changing report state."""
import json
from collections import Counter


RESULT_EVENTS = {'RNMS_GIS_PROCESSED', 'RNMS_GIS_ROUTED'}


def merge_failure_details(previous, current):
    """Keep already retrieved immutable documents across retries and CAS races."""
    if not previous:
        return current
    if not current or previous.get('state') == 'complete':
        return previous
    documents = {(d['resultDocId'], d['docId']): d
                 for d in previous.get('documents', [])}
    documents.update({(d['resultDocId'], d['docId']): d
                      for d in current.get('documents', [])})
    merged = dict(current, documents=list(documents.values()))
    if not merged.get('receipts'):
        merged['receipts'] = previous.get('receipts', [])
    if documents and merged['state'] == 'unavailable':
        merged['state'] = 'partial'
    return merged


def _issue(stage, error=None, **identity):
    issue = {'stage': stage, **identity}
    if error is not None:
        # Exception messages/HTTP bodies may be arbitrarily large or echo auth.
        issue['errorType'] = type(error).__name__
        response = getattr(error, 'response', None)
        if response is not None:
            issue['httpStatus'] = response.status_code
    return issue


def collect_failure_details(api, report_id, previous=None):
    """Fetch every referenced result document; retry incomplete reads next poll.

    SUZ returns document content as a JSON string. Keep all CIS records (including
    successful ones) and receipts in the status snapshot; the log is bounded.
    """
    if previous and previous.get('state') == 'complete':
        return previous
    result = {'state': 'unavailable', 'receipts': [], 'documents': [], 'issues': []}
    issues = result['issues']
    try:
        answer = api.receipt_info(report_id)
        receipts = answer['results']
        if not isinstance(receipts, list) or not receipts:
            raise ValueError('No Receipt 2.0 results')
    except Exception as error:
        issues.append(_issue('receipt', error))
        return merge_failure_details(previous, result)

    result['receipts'] = receipts
    cached = {(d['resultDocId'], d['docId']): d
              for d in (previous or {}).get('documents', [])}
    seen = set()
    for receipt in receipts:
        if not isinstance(receipt, dict):
            issues.append(_issue('receipt_shape'))
            continue
        result_id = receipt.get('resultDocId')
        operations = receipt.get('operations')
        if not isinstance(result_id, str) or not result_id or not isinstance(operations, list):
            issues.append(_issue('receipt_shape'))
            continue
        candidates = [op for op in operations if isinstance(op, dict)
                      and op.get('operationType') in RESULT_EVENTS
                      and isinstance(op.get('docId'), str) and op['docId']]
        if not candidates:
            issues.append(_issue('result_document_missing', resultDocId=result_id))
        for operation in candidates:
            doc_id = operation['docId']
            identity = {'resultDocId': result_id, 'docId': doc_id}
            key = (result_id, doc_id)
            if key in seen:
                continue
            seen.add(key)
            if key in cached:
                result['documents'].append(cached[key])
                continue
            try:
                envelope = api.receipt_document(result_id, doc_id)
                content = envelope['content']
                content = json.loads(content) if isinstance(content, str) else content
                if not isinstance(content, dict):
                    raise ValueError('Invalid result document')
                # The specification has a lower-case spelling in one example.
                cis_list = content.get('cisList', content.get('cislist'))
                if not isinstance(cis_list, list) or not cis_list or any(
                    not isinstance(item, dict) or not isinstance(item.get('cis'), str)
                    or type(item.get('code')) is not int for item in cis_list
                ):
                    raise ValueError('Invalid CIS result list')
                result['documents'].append({
                    **identity, 'operationType': operation['operationType'],
                    'cisList': cis_list,
                })
            except Exception as error:
                issues.append(_issue('document', error, **identity))

    if result['documents']:
        result['state'] = 'partial' if issues else 'complete'
    return merge_failure_details(previous, result)


def log_failure_details(details, headers, report_id, output_path, *, include_details=True):
    """Reuse the existing 4096-byte journal / 0600 tmp-file diagnostic policy."""
    from .suz import log_suz_diagnostic

    errors = Counter()
    for document in details.get('documents', []):
        for item in document['cisList']:
            if item['code'] != 0:
                errors[(item['code'], str(item.get('description', '')))] += 1
    # A bounded summary keeps the reason visible even when all CIS go to tmp.
    summary = {
        'reportId': report_id,
        'state': details['state'],
        'failedCisRecords': sum(errors.values()),
        'reasons': [{'code': code, 'description': text[:200], 'count': count}
                    for (code, text), count in errors.most_common(3)],
        'reasonGroups': len(errors),
        'statusPath': output_path,
    }
    context = 'отчёт FAILED'
    log_suz_diagnostic(json.dumps(summary, ensure_ascii=False), headers, context=context)
    if include_details:
        log_suz_diagnostic(json.dumps(details, ensure_ascii=False), headers, context=context)
