"""Strict, bounded evidence for the confirmed SUZ utilisation date rejection.

Unknown codes/shapes remain reconciliation-only. Raw server text, marking codes,
credentials and signatures never become rejection checkpoints or notifications.
"""
from datetime import date
import hashlib
import json
import re

from .operation_state import ReconciliationRequired


class SuzUtilisationRejected(ReconciliationRequired):
    category = 'suz_expiration_date_rejected'

    def __init__(self, evidence):
        required = {'kind', 'http_status', 'code', 'field', 'submitted_value', 'request_sha256'}
        if (not isinstance(evidence, dict) or set(evidence) != required
                or evidence['kind'] != 'suz-utilisation-rejected-v1'
                or type(evidence['http_status']) is not int or evidence['http_status'] != 400
                or type(evidence['code']) is not int or evidence['code'] != 7745
                or evidence['field'] != 'attributes.expirationDate'
                or not isinstance(evidence['request_sha256'], str)
                or not re.fullmatch(r'[0-9a-f]{64}', evidence['request_sha256'])
                or (evidence['submitted_value'] is not None
                    and _iso_date(evidence['submitted_value']) is None)):
            raise ValueError('Invalid SUZ rejection evidence')
        self.evidence = dict(evidence)
        super().__init__('SUZ rejected utilisation: HTTP 400, code 7745, attributes.expirationDate')


def _iso_date(value):
    if not isinstance(value, str) or not re.fullmatch(r'\d{4}-\d{2}-\d{2}', value):
        return None
    try:
        return date.fromisoformat(value).isoformat()
    except ValueError:
        return None


def classify_utilisation_rejection(response, submitted_bytes):
    if response.status_code != 400 or len(response.content) > 65536:
        return None
    try:
        payload = response.json()
    except (ValueError, TypeError):
        return None
    if (not isinstance(payload, dict) or payload.get('success') is not False
            or set(payload) != {'success', 'fieldErrors'}):
        return None
    errors = payload.get('fieldErrors')
    # The demonstrated contract is one date validation error. Mixed/new errors
    # need an explicit contract and fixture; never guess non-acceptance.
    if not isinstance(errors, list) or len(errors) != 1:
        return None
    error = errors[0]
    if (not isinstance(error, dict) or type(error.get('errorCode')) is not int
            or error['errorCode'] != 7745 or error.get('fieldName') != 'attributes.expirationDate'):
        return None
    submitted_value = None
    try:
        request = json.loads(submitted_bytes)
        attributes = request.get('attributes')
        if isinstance(attributes, dict):
            submitted_value = _iso_date(attributes.get('expirationDate'))
    except (ValueError, TypeError, AttributeError):
        pass
    return SuzUtilisationRejected({
        'kind': 'suz-utilisation-rejected-v1', 'http_status': 400, 'code': 7745,
        'field': 'attributes.expirationDate', 'submitted_value': submitted_value,
        'request_sha256': hashlib.sha256(submitted_bytes).hexdigest(),
    })
