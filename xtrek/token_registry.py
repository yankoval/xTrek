"""Explicit token identities and versioned storage, independent of API clients.

No configuration discovery, network access, signing or automatic legacy inference.
The current TokenProcessor is not switched to this format by importing this module.
"""
import base64
from dataclasses import dataclass, field
from datetime import datetime, timezone
import json
import math
import os
from pathlib import Path
import re
import tempfile
from typing import Iterable, Optional
from uuid import UUID
from zoneinfo import ZoneInfo


class TokenValidationError(ValueError):
    """Messages deliberately exclude token values and payloads."""


def parse_time(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
        if parsed.tzinfo is None:
            raise ValueError()
        return parsed.astimezone(timezone.utc)
    except (AttributeError, TypeError, ValueError):
        raise TokenValidationError('Token timestamp requires an explicit timezone') from None


def jwt_claims(value: str) -> dict:
    # Decode metadata only. This is NOT cryptographic authentication of the JWT.
    try:
        header, payload, signature = value.split('.')
        if not header or not payload or not signature:
            raise ValueError()
        data = json.loads(base64.urlsafe_b64decode(payload + '=' * (-len(payload) % 4)))
        if not isinstance(data, dict):
            raise ValueError()
        return data
    except Exception:
        raise TokenValidationError('Invalid JWT metadata') from None


def jwt_expiry(value: str) -> datetime:
    exp = jwt_claims(value).get('exp')
    try:
        if isinstance(exp, bool) or not isinstance(exp, (int, float)) or not math.isfinite(exp):
            raise ValueError()
        return datetime.fromtimestamp(exp, timezone.utc)
    except (ValueError, TypeError, OverflowError, OSError):
        raise TokenValidationError('JWT requires a valid exp claim') from None


@dataclass(frozen=True)
class TokenRecord:
    purpose: str
    environment: str
    inn: str
    format: str
    token: str = field(repr=False)
    expireDate: str
    expiry_source: str
    issuer_base_url: str
    connection_id: Optional[str] = None
    oms_id: Optional[str] = None
    auth_uuid: Optional[str] = None
    certificate_sha256: Optional[str] = None

    def __post_init__(self):
        if self.purpose not in {'true_api', 'suz'}:
            raise TokenValidationError('Unknown token purpose')
        if self.environment not in {'production', 'sandbox'}:
            raise TokenValidationError('Unknown token environment')
        if not isinstance(self.inn, str) or not re.fullmatch(r'(?:[0-9]{10}|[0-9]{12})', self.inn):
            raise TokenValidationError('Organization INN must contain 10 or 12 digits')
        host = 'markirovka.crpt.ru' if self.environment == 'production' else 'markirovka.sandbox.crptech.ru'
        if self.issuer_base_url not in {f'https://{host}/api/v3/true-api', f'https://{host}/api/v4/true-api'}:
            raise TokenValidationError('Issuer does not match the selected environment')
        if not isinstance(self.token, str) or not self.token:
            raise TokenValidationError('Missing token value')
        expiry = parse_time(self.expireDate)
        if self.format == 'UUID':
            self._uuid(self.token, 'token')
            allowed = {'server'} if self.purpose == 'true_api' else {'server', 'suz_ttl'}
            if self.expiry_source not in allowed:
                raise TokenValidationError('UUID expiry must have an appropriate explicit source')
        elif self.format == 'JWT':
            claims = jwt_claims(self.token)
            if self.expiry_source != 'jwt_exp' or expiry != jwt_expiry(self.token):
                raise TokenValidationError('JWT expiry must equal the exp claim')
            if claims.get('inn') is not None and str(claims['inn']) != self.inn:
                raise TokenValidationError('JWT organization differs from the selected INN')
        else:
            raise TokenValidationError('Unknown token format')
        if self.purpose == 'suz':
            self._uuid(self.connection_id, 'connection_id')
            self._uuid(self.oms_id, 'oms_id')
            object.__setattr__(self, 'connection_id', self.connection_id.lower())
            object.__setattr__(self, 'oms_id', self.oms_id.lower())
        elif self.connection_id is not None or self.oms_id is not None:
            raise TokenValidationError('True API token must not carry a SUZ connection')
        if self.auth_uuid is not None:
            self._uuid(self.auth_uuid, 'auth_uuid')
        if self.certificate_sha256 is not None and (
            not isinstance(self.certificate_sha256, str)
            or not re.fullmatch(r'[0-9a-fA-F]{64}', self.certificate_sha256)
        ):
            raise TokenValidationError('Invalid certificate fingerprint')

    @staticmethod
    def _uuid(value, label):
        try:
            if not isinstance(value, str) or str(UUID(value)) != value.lower():
                raise ValueError()
        except (ValueError, AttributeError, TypeError):
            raise TokenValidationError(f'Invalid {label} UUID') from None

    @property
    def identity(self):
        """Logical audience. Wire format is deliberately not part of this identity."""
        return self.environment, self.purpose, self.inn, self.connection_id, self.oms_id

    @property
    def storage_key(self):
        # Keep a transitional JWT alongside a UUID for an explicit rollback policy.
        return self.identity + (self.format,)

    def remaining_seconds(self, now: Optional[datetime] = None) -> float:
        now = now or datetime.now(timezone.utc)
        if now.tzinfo is None:
            raise TokenValidationError('Clock must have a timezone')
        return (parse_time(self.expireDate) - now).total_seconds()

    def to_dict(self) -> dict:
        """Contains the secret: for private persistence only, never for logging."""
        return {k: v for k, v in self.__dict__.items() if v is not None}

    @classmethod
    def from_dict(cls, data: dict):
        try:
            return cls(**data)
        except TypeError:
            raise TokenValidationError('Incomplete or unknown token record fields') from None


class TokenRegistry:
    def __init__(self, records: Iterable[TokenRecord] = ()):
        self._records = {}
        for record in records:
            if not isinstance(record, TokenRecord):
                raise TokenValidationError('Expected a validated TokenRecord')
            if record.storage_key in self._records:
                raise TokenValidationError('Duplicate token storage key')
            self._records[record.storage_key] = record

    @property
    def records(self):
        return tuple(self._records.values())

    def upsert(self, record: TokenRecord):
        if not isinstance(record, TokenRecord):
            raise TokenValidationError('Expected a validated TokenRecord')
        self._records[record.storage_key] = record

    def select(self, *, purpose: str, environment: str, inn: str,
               connection_id: Optional[str] = None, oms_id: Optional[str] = None,
               preferred_format: Optional[str] = None, now: Optional[datetime] = None
               ) -> Optional[TokenRecord]:
        if purpose not in {'true_api', 'suz'} or environment not in {'production', 'sandbox'}:
            raise TokenValidationError('Explicit purpose and environment are required')
        if not isinstance(inn, str) or not re.fullmatch(r'(?:[0-9]{10}|[0-9]{12})', inn):
            raise TokenValidationError('Selector requires a valid organization INN')
        if purpose == 'suz':
            TokenRecord._uuid(connection_id, 'connection_id')
            TokenRecord._uuid(oms_id, 'oms_id')
            connection_id, oms_id = connection_id.lower(), oms_id.lower()
        elif connection_id is not None or oms_id is not None:
            raise TokenValidationError('True API selector must not carry a SUZ connection')
        if preferred_format not in {None, 'JWT', 'UUID'}:
            raise TokenValidationError('Unknown preferred format')
        identity = environment, purpose, inn, connection_id, oms_id
        now = now or datetime.now(timezone.utc)
        candidates = [r for r in self.records if r.identity == identity
                      and r.remaining_seconds(now) > 0
                      and (preferred_format is None or r.format == preferred_format)]
        if len(candidates) > 1:
            raise TokenValidationError('Choose an explicit format policy while JWT and UUID coexist')
        return candidates[0] if candidates else None

    def to_dict(self):
        return {'schema_version': 2, 'tokens': [r.to_dict() for r in sorted(
            self.records, key=lambda r: tuple(x or '' for x in r.storage_key))]}

    @classmethod
    def from_dict(cls, data):
        if (not isinstance(data, dict) or set(data) != {'schema_version', 'tokens'}
                or type(data['schema_version']) is not int or data['schema_version'] != 2
                or not isinstance(data['tokens'], list)):
            raise TokenValidationError('Expected token registry schema version 2; legacy migration is explicit')
        return cls(TokenRecord.from_dict(r) for r in data['tokens'])

    @classmethod
    def load(cls, path):
        try:
            return cls.from_dict(json.loads(Path(path).read_text(encoding='utf-8')))
        except json.JSONDecodeError:
            raise TokenValidationError('Invalid registry JSON') from None

    def save(self, path):
        """Atomic private snapshot. Caller coordinates a single writer; no S3 publishing."""
        target = Path(path)
        if target.is_symlink():
            raise TokenValidationError('Refusing a symlink registry target')
        temporary = None
        try:
            with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8',
                                             dir=target.parent, delete=False) as stream:
                temporary = Path(stream.name)
                json.dump(self.to_dict(), stream, ensure_ascii=False, indent=2)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, target)
            temporary = None
        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)


def migrate_legacy_record(data: dict, *, purpose: str, environment: str, inn: str,
                          issuer_base_url: str, connection_id: Optional[str] = None,
                          oms_id: Optional[str] = None, legacy_timezone: Optional[str] = None
                          ) -> TokenRecord:
    """Only for records whose origin the caller has established. Never infer purpose.

    A legacy UUID True API record needs the original server expireDate.
    A timezone for old SUZ wall-clock expiry must be supplied explicitly.
    """
    value = data.get('Токен')
    if not isinstance(value, str):
        raise TokenValidationError('Missing legacy token')
    if data.get('inn') is not None and str(data['inn']) != inn:
        raise TokenValidationError('Legacy record organization differs from the selected INN')
    if purpose == 'suz' and str(data.get('Идентификатор')) != connection_id:
        raise TokenValidationError('Legacy connection differs from the selected connection')
    if value.count('.') == 2:
        fmt, source, expiry = 'JWT', 'jwt_exp', jwt_expiry(value).isoformat()
    else:
        fmt = 'UUID'
        if purpose == 'true_api':
            source, expiry = 'server', data.get('expireDate')
        else:
            source, expiry = 'suz_ttl', data.get('ДействуетДо')
            try:
                parsed = datetime.fromisoformat(expiry.replace('Z', '+00:00'))
                if parsed.tzinfo is None:
                    if not legacy_timezone:
                        raise ValueError()
                    parsed = parsed.replace(tzinfo=ZoneInfo(legacy_timezone))
                expiry = parsed.isoformat()
            except (AttributeError, TypeError, ValueError, KeyError):
                raise TokenValidationError('Legacy SUZ expiry requires an explicit timezone') from None
    return TokenRecord(purpose, environment, inn, fmt, value, expiry, source,
                       issuer_base_url, connection_id, oms_id)
