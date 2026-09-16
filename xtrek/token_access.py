"""Purpose-based access shared by master and consumers during migration."""
from contextlib import contextmanager
from datetime import datetime, timezone
from uuid import uuid4
import os
from urllib.parse import urlsplit
from .token_registry import TokenRecord, TokenValidationError, jwt_claims


class PurposeTokenAccess:
    @contextmanager
    def writer_lock(self):
        """One master/CLI issuer at a time. Crashed writers leave a visible lock."""
        if self.tokens_read_only or not self.storage:
            raise PermissionError('A writable S3 source is required for the master lock')
        path = self.config.get('tokens_master_lock_path') or (
            self.config.get('tokens_path') or self.tokens_path) + '.master.lock'
        if not self.storage.acquire_lock(path, str(uuid4())):
            raise TokenValidationError('Token master is already locked; no issuance attempted')
        try:
            yield
        finally:
            self.storage.release_lock(path)

    def true_api_format(self, inn):
        policies = self.config.get('true_api_token_formats', {})
        value = policies.get(str(inn), self.config.get('true_api_token_format', 'JWT'))
        if value not in {'JWT', 'UUID'}:
            raise TokenValidationError('true_api_token_format must be JWT or UUID')
        if not self.registry_enabled and value != 'JWT':
            raise TokenValidationError('True API UUID requires tokens_registry_path')
        return value

    def _scope(self, inn, purpose, conid=None, oms_id=None, environment='production'):
        if purpose not in {'true_api', 'suz'}:
            raise TokenValidationError('Explicit token purpose is required')
        configured = self.config.get('tokens_environment', 'production')
        if environment != configured or environment not in {'production', 'sandbox'}:
            raise TokenValidationError('Consumer and token environments differ')
        if not self.registry_enabled and environment != 'production':
            raise TokenValidationError('Legacy tokens have production provenance only')
        if purpose == 'true_api':
            if conid or oms_id:
                raise TokenValidationError('True API cannot select a SUZ connection')
            if self.registry_enabled:
                host = self.config.get('true_api_host') or os.getenv('TRUE_API_HOST')
                expected = 'markirovka.crpt.ru' if environment == 'production' else 'markirovka.sandbox.crptech.ru'
                if host and (urlsplit(host).scheme != 'https' or urlsplit(host).netloc != expected):
                    raise TokenValidationError('True API host differs from the token environment')
            return None, None
        if not conid:
            raise TokenValidationError('SUZ requires connection_id')
        org = self.org_manager.find(connection_id=str(conid))
        if not org:
            matches = [o for o in self.org_manager.list()
                       if str(o.connection_id).lower() == str(conid).lower()]
            if len(matches) != 1:
                raise TokenValidationError('SUZ connection must identify one organization')
            org = matches[0]
        if not org or str(org.inn) != str(inn) or not org.oms_id:
            raise TokenValidationError('SUZ connection does not match the organization')
        if oms_id and str(oms_id).lower() != str(org.oms_id).lower():
            raise TokenValidationError('SUZ OMS does not match the connection')
        return str(conid).lower(), str(org.oms_id).lower()

    def get_token_record(self, inn, purpose, conid=None, oms_id=None, environment='production'):
        connection, oms = self._scope(inn, purpose, conid, oms_id, environment)
        if not self.registry_enabled:
            raise TokenValidationError('Structured records require tokens_registry_path')
        return self.registry.select(
            purpose=purpose, environment=environment, inn=str(inn),
            connection_id=connection, oms_id=oms,
            preferred_format=self.true_api_format(inn) if purpose == 'true_api' else 'UUID',
        )

    def get_token_for(self, inn, purpose, conid=None, oms_id=None, environment='production'):
        self._scope(inn, purpose, conid, oms_id, environment)
        if self.registry_enabled:
            record = self.get_token_record(inn, purpose, conid, oms_id, environment)
            return self._record_view(record) if record else None
        token_type = self.true_api_format(inn) if purpose == 'true_api' else 'UUID'
        value = self._find_active_token(inn, token_type, conid)
        if not value:
            return None
        candidates = [t for t in self.processed_tokens if t.get('Токен') == value
                      and str(t.get('inn')) == str(inn) and t.get('ТипТокена') == token_type
                      and (purpose == 'true_api' or str(t.get('Идентификатор')).lower() == str(conid).lower())
                      and self._is_token_active(t)]
        return max(candidates, key=self._token_expiry).copy() if candidates else None

    def get_token_value_for(self, inn, purpose, conid=None, oms_id=None, environment='production'):
        record = self.get_token_for(inn, purpose, conid, oms_id, environment)
        return record.get('Токен') if record else None

    def remaining_for(self, inn, purpose, conid=None, oms_id=None, environment='production'):
        record = self.get_token_for(inn, purpose, conid, oms_id, environment)
        if not record:
            return None
        return (self._token_expiry(record) - datetime.now(timezone.utc)).total_seconds()

    def refresh_token_for(self, inn, purpose, conid=None, oms_id=None, environment='production'):
        self.refresh_from_source()
        token = self.get_token_value_for(inn, purpose, conid, oms_id, environment)
        if not token:
            raise TokenValidationError('No active token for the requested purpose and scope')
        return token

    @staticmethod
    def _record_view(record):
        result = {'Токен': record.token, 'inn': record.inn, 'ТипТокена': record.format,
                  'ДействуетДо': record.expireDate, 'purpose': record.purpose,
                  'Идентификатор': record.connection_id or record.auth_uuid,
                  'omsId': record.oms_id}
        if record.format == 'JWT':
            claims = jwt_claims(record.token)
            result['exp_timestamp'] = claims['exp']
            result['Идентификатор'] = claims.get('pid', record.auth_uuid)
        return result

    def save_record(self, record):
        if self.tokens_read_only:
            raise PermissionError('Clients cannot publish tokens')
        if not isinstance(record, TokenRecord) or record.remaining_seconds() <= 0:
            raise TokenValidationError('An active validated TokenRecord is required')
        self._scope(record.inn, record.purpose, record.connection_id, record.oms_id, record.environment)
        if not self.registry_enabled and record.purpose == 'true_api' and record.format != 'JWT':
            raise TokenValidationError('True API UUID must not be written to legacy tokens.json')
        if self.storage:
            self._sync_from_s3(required=True)
        else:
            self.read_tokens_file()
            self.process_tokens()
        if record.remaining_seconds() <= 0:
            raise TokenValidationError('Token expired while reloading the source')
        previous = self.tokens
        if self.registry_enabled:
            self.registry.upsert(record)
            self.tokens = self.registry.to_dict()
        else:
            # Preserve legacy readers while keeping the issuer's actual expiry.
            view = self._record_view(record)
            self.tokens = [raw for raw, parsed in zip(self.tokens, self.processed_tokens)
                           if not (str(parsed.get('inn')) == record.inn
                                   and parsed.get('ТипТокена') == record.format
                                   and (record.purpose == 'true_api' or
                                        str(parsed.get('Идентификатор')).lower() == record.connection_id))]
            self.tokens.append({'Идентификатор': view['Идентификатор'], 'Токен': record.token,
                                'inn': record.inn, 'ДействуетС': datetime.now(timezone.utc).isoformat(),
                                'ДействуетДо': record.expireDate, 'ТокенОбновления': ''})
        try:
            self._write_tokens_file_atomic()
            if record.remaining_seconds() <= 0:
                raise TokenValidationError('Token expired before publication')
            self._sync_to_s3()
        except Exception:
            # A candidate cache is not proof of successful S3 publication.
            self._apply_tokens(previous)
            raise
        self.process_tokens()
