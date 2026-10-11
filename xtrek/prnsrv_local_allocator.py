"""Idempotent local mTLS allocator shim for the pinned prnsrv function."""
from __future__ import annotations

import hashlib
import re
from typing import Any
from urllib.parse import urlparse


class LocalAllocatorError(RuntimeError):
    pass


def _check_digit(payload: str) -> str:
    total = sum(int(digit) * (3 if index % 2 == 0 else 1)
                for index, digit in enumerate(reversed(payload)))
    return str((-total) % 10)


class LocalPrnsrvAllocator:
    """Adapt the existing PrnsrvFunction allocator protocol to worker94 mTLS.

    The server-side local ledger owns the range and returns the same range for
    the stable UUID/source-hash request ID after a lost response.
    """

    def __init__(self, *, url: str, prefix: str, session: Any,
                 timeout_seconds: float = 15.0):
        parsed = urlparse(url)
        if parsed.scheme != 'https' or not parsed.netloc:
            raise ValueError('local SSCC allocator URL must use HTTPS')
        if not re.fullmatch(r'\d{1,14}', str(prefix)):
            raise ValueError('local SSCC prefix must contain 1-14 digits')
        self.url = url.rstrip('/') + '/v1/allocate'
        self.prefix = str(prefix)
        self.session = session
        self.timeout_seconds = float(timeout_seconds)

    @staticmethod
    def request_id(job_uuid: str, source_hash: str) -> str:
        return hashlib.sha256(
            f'allocate-pallet-sscc\0{job_uuid}\0{source_hash}'.encode('utf-8')
        ).hexdigest()

    def allocate(self, *, job_uuid: str, source_hash: str, count: int,
                 auth_token: str | None = None):
        if auth_token:
            raise LocalAllocatorError('PRNSRV allocation must not use a SUZ/business signer token')
        request_id = self.request_id(job_uuid, source_hash)
        response = self.session.post(
            self.url,
            json={'request_id': request_id, 'prefix': self.prefix,
                  'count': count, 'extension': 3},
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()
        payload = response.json()
        ssccs = payload.get('ssccs') if isinstance(payload, dict) else None
        if not isinstance(ssccs, list) or len(ssccs) != count:
            raise LocalAllocatorError('Local allocator returned an incorrect SSCC count')
        expected_start = '3' + self.prefix
        for sscc in ssccs:
            if not isinstance(sscc, str) or not re.fullmatch(r'\d{18}', sscc):
                raise LocalAllocatorError('Local allocator returned a malformed SSCC')
            if not sscc.startswith(expected_start) or sscc[-1] != _check_digit(sscc[:-1]):
                raise LocalAllocatorError('Local allocator returned a wrong-prefix/invalid SSCC')
        # PrnsrvFunction consumes these two attributes only; no Cloud allocator
        # client or SUZ identity is imported on this route.
        from types import SimpleNamespace
        return SimpleNamespace(ssccs=tuple(ssccs), allocation_id=request_id)
