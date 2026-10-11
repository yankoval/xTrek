"""Issue scoped CRPT tokens with explicit local or SignJS authentication signing."""
import argparse
import base64
from datetime import datetime, timedelta, timezone
import logging
import re
import time
from uuid import uuid4
from urllib.parse import urlencode

import requests
from .config_loader import load_config
from .storage import get_storage
from .token_registry import TokenRecord, TokenValidationError, jwt_expiry
from .token_runtime import (checkpoint, cleanup_budget, current_runtime,
                            DeadlineExpired, MasterStopped, master_runtime, request_timeout)
from .sign import sign_token_data, SigningError, _thumbprint

logger = logging.getLogger(__name__)
AUTH_BASES = {
    'production': 'https://markirovka.crpt.ru/api/v3/true-api',
    'sandbox': 'https://markirovka.sandbox.crptech.ru/api/v3/true-api',
}
SUZ_BASES = {'production': 'https://suzgrid.crpt.ru', 'sandbox': 'https://suz.sandbox.crptech.ru'}


class TokenIssuanceError(RuntimeError):
    """Safe operational error: never include response bodies or credentials."""

    STAGES = {'configuration', 'challenge', 'signing_local', 'signing_storage',
              'issuance', 'verification', 'registry_sync', 'publication', 'unknown'}
    REASONS = {'invalid_configuration', 'invalid_challenge', 'signing_failed',
               'private_key_missing', 'certificate_not_found', 'native_unavailable', 'pin_unavailable',
               'certificate_inn_mismatch', 'certificate_validation_failed',
               'timeout', 'invalid_signature', 'storage_failed', 'transport_failed',
               'http_error', 'invalid_response', 'verification_failed',
               'reconciliation_required', 'writer_lock_required', 'publication_failed', 'operation_failed'}

    def __init__(self, message, *, stage='unknown', reason='operation_failed', outcome_unknown=False):
        super().__init__(message)
        self.stage = stage if isinstance(stage, str) and stage in self.STAGES else 'unknown'
        self.reason = reason if isinstance(reason, str) and reason in self.REASONS else 'operation_failed'
        self.outcome_unknown = outcome_unknown is True

    @property
    def diagnostic(self):
        # Recheck the allowlist: even exceptions from extensions are untrusted.
        stage = self.stage if isinstance(self.stage, str) and self.stage in self.STAGES else 'unknown'
        reason = self.reason if isinstance(self.reason, str) and self.reason in self.REASONS else 'operation_failed'
        return f'stage={stage} reason={reason}'


def safe_diagnostic(exc):
    if isinstance(exc, TokenIssuanceError):
        return TokenIssuanceError.diagnostic.fget(exc)
    if isinstance(exc, TokenValidationError) and getattr(exc, 'issuance_response_invalid', False) is True:
        return 'stage=issuance reason=invalid_response'
    return 'stage=unknown reason=operation_failed'


def _signing_route(inn, config):
    """Auth routing is independent of document signing; omitted INNs use storage."""
    try:
        section = config.get('tokens_signing', {})
        if not isinstance(section, dict) or set(section) - {'by_inn'}:
            raise ValueError()
        entries = section.get('by_inn', {})
        if not isinstance(entries, dict) or any(not isinstance(key, str)
                or not re.fullmatch(r'[0-9]{10}|[0-9]{12}', key) for key in entries):
            raise ValueError()
        options = entries.get(inn, {'mode': 'storage'})
        if not isinstance(options, dict):
            raise ValueError()
        if options.get('mode') == 'storage' and set(options) == {'mode'}:
            return 'storage', None
        if options.get('mode') != 'local' or set(options) - {'mode', 'thumbprint', 'store_location', 'pin_file'}:
            raise ValueError()
        thumbprint = _thumbprint(options.get('thumbprint'))
        location = options.get('store_location', 'current_user')
        pin_file = options.get('pin_file')
        if location not in {'current_user', 'local_machine'} or (pin_file is not None
                and (not isinstance(pin_file, str) or not pin_file)):
            raise ValueError()
        return 'local', dict(thumbprint=thumbprint, store_location=location,
                             pin_file=pin_file, expected_inn=inn)
    except (ValueError, TypeError):
        raise TokenIssuanceError('Invalid token signing configuration', stage='configuration',
                                 reason='invalid_configuration') from None


def _signature(value, stage):
    try:
        signature = ''.join(value.split())
        if not signature or not base64.b64decode(signature, validate=True):
            raise ValueError()
        return signature
    except Exception:
        raise TokenIssuanceError('Empty or invalid signing result', stage=stage, reason='invalid_signature') from None


def sign_data(data, inn, config, *, detached=False):
    mode, options = _signing_route(str(inn), config)
    if mode == 'local':
        checkpoint()
        try:
            signature = _signature(sign_token_data(data, detached=detached, **options), 'signing_local')
        except TokenIssuanceError:
            raise
        except SigningError as exc:
            raise TokenIssuanceError('Local authentication signing failed', stage='signing_local',
                                     reason=exc.reason) from None
        except Exception:
            raise TokenIssuanceError('Local authentication signing failed', stage='signing_local',
                                     reason='signing_failed') from None
        checkpoint()
        return signature
    directory = config.get('sign')
    try:
        timeout = float(config.get('SIGNING_TIMEOUT', 60))
    except (ValueError, TypeError):
        timeout = 0
    if not isinstance(directory, str) or not directory or not 0 < timeout <= 600:
        raise TokenIssuanceError('Configure sign and SIGNING_TIMEOUT (1..600 seconds)',
                                 stage='configuration', reason='invalid_configuration')
    storage = None
    # SignJS chooses attached CAdES for .txt and detached CAdES for .json.
    source = f"{directory.rstrip('/')}/{inn}_{uuid4()}_dataToSign.{'json' if detached else 'txt'}"
    signed = source + '.sig'
    try:
        storage = get_storage(directory, config.get('s3_config'))
        storage.write_text(source, data)
        deadline = time.monotonic() + timeout
        while not storage.exists(signed):
            checkpoint()
            if time.monotonic() >= deadline:
                raise TokenIssuanceError('Signing timed out', stage='signing_storage', reason='timeout')
            time.sleep(min(1, max(0, deadline - time.monotonic())))
        signature = _signature(storage.read_text(signed), 'signing_storage')
    except TokenIssuanceError:
        raise
    except Exception:
        raise TokenIssuanceError('Signing folder operation failed', stage='signing_storage', reason='storage_failed') from None
    finally:
        try:
            with cleanup_budget(5):
                for path in (signed, source):
                    try:
                        if storage.exists(path):
                            storage.delete(path)
                    except Exception:
                        logger.warning('Could not remove a temporary signing object')
        except DeadlineExpired:
            logger.warning('Temporary signing object cleanup exceeded its budget')
    checkpoint()
    return signature


def _request(session, method, url, *, attempts=1, stage='unknown', **kwargs):
    if method == 'POST' and attempts != 1:
        raise TokenIssuanceError('Issuance POST cannot be retried', stage='issuance', reason='invalid_configuration')
    for attempt in range(attempts):
        checkpoint()
        try:
            response = session.request(method, url, timeout=request_timeout(), allow_redirects=False, **kwargs)
        except requests.RequestException:
            if attempt + 1 < attempts:
                time.sleep(attempt + 1)
                continue
            raise TokenIssuanceError('CRPT transport failed; issuance outcome may be unknown',
                                     stage=stage, reason='transport_failed', outcome_unknown=method == 'POST') from None
        if response.status_code == 200:
            try:
                return response.json()
            except ValueError:
                raise TokenIssuanceError('CRPT returned invalid JSON', stage=stage, reason='invalid_response',
                                         outcome_unknown=method == 'POST') from None
        if (response.status_code == 429 or response.status_code >= 500) and attempt + 1 < attempts:
            time.sleep(attempt + 1)
            continue
        raise TokenIssuanceError(f'CRPT request failed with HTTP {response.status_code}', stage=stage,
                                 reason='http_error', outcome_unknown=method == 'POST')


def verify_token(record, session, config):
    if record.remaining_seconds() <= 0:
        raise TokenIssuanceError('Issued token has already expired', stage='verification', reason='verification_failed')
    headers = {'Accept': 'application/json'}
    if record.purpose == 'suz':
        path = '/api/v3/ping?' + urlencode({'omsId': record.oms_id})
        headers['clientToken'] = record.token
        if config.get('suz_ping_sign', False):
            headers['X-Signature'] = sign_data(path, record.inn, config, detached=True)
        result = _request(session, 'GET', SUZ_BASES[record.environment] + path,
                          headers=headers, attempts=3, stage='verification')
        if (not isinstance(result, dict) or str(result.get('omsId', '')).lower() != record.oms_id
                or not result.get('apiVersion') or not result.get('omsVersion')):
            raise TokenIssuanceError('SUZ ping did not confirm OMS and API versions', stage='verification', reason='verification_failed')
    else:
        headers['Authorization'] = 'Bearer ' + record.token
        _request(session, 'GET', record.issuer_base_url + '/elk/product-groups/balance/all',
                 headers=headers, attempts=3, stage='verification')
    if record.remaining_seconds() <= 0:
        raise TokenIssuanceError('Token expired during verification', stage='verification', reason='verification_failed')


def issue_token(inn, *, purpose, token_format='UUID', connection_id=None, oms_id=None, config=None):
    config = load_config() if config is None else config
    environment = config.get('tokens_environment', 'production')
    if environment not in AUTH_BASES or not re.fullmatch(r'(?:[0-9]{10}|[0-9]{12})', str(inn)):
        raise TokenValidationError('Explicit valid environment and INN required')
    if purpose not in {'true_api', 'suz'} or token_format not in {'JWT', 'UUID'}:
        raise TokenValidationError('Explicit purpose and format required')
    if purpose == 'suz':
        if token_format != 'UUID':
            raise TokenValidationError('SUZ requires UUID format')
        TokenRecord._uuid(connection_id, 'connection_id')
        TokenRecord._uuid(oms_id, 'oms_id')
    elif connection_id is not None or oms_id is not None:
        raise TokenValidationError('True API must not carry a SUZ connection')
    mode, _ = _signing_route(str(inn), config)
    if mode == 'storage' and not config.get('sign'):
        raise TokenIssuanceError('Signing directory is not configured', stage='configuration', reason='invalid_configuration')
    runtime = current_runtime()
    if runtime is not None:
        runtime.suz_post_attempted = False
    if purpose == 'suz':
        if runtime is None or runtime.guard is None:
            raise TokenIssuanceError('SUZ issuance requires the master writer lock', stage='issuance', reason='writer_lock_required')
        runtime.guard.check_suz(environment, str(inn), connection_id, oms_id)
    base = AUTH_BASES[environment]
    with requests.Session() as session:
        session.trust_env = False
        session.verify = True
        challenge = _request(session, 'GET', base + '/auth/key', stage='challenge')
        if not isinstance(challenge, dict) or not isinstance(challenge.get('data'), str) or not challenge['data']:
            raise TokenIssuanceError('Invalid signing challenge', stage='challenge', reason='invalid_challenge')
        try:
            TokenRecord._uuid(challenge.get('uuid'), 'auth_uuid')
        except TokenValidationError:
            raise TokenIssuanceError('Invalid signing challenge UUID', stage='challenge', reason='invalid_challenge') from None
        signature = sign_data(challenge['data'], str(inn), config)
        payload = {'uuid': challenge['uuid'], 'data': signature, 'inn': str(inn)}
        suffix = '/auth/simpleSignIn'
        if purpose == 'true_api':
            payload['unitedToken'] = token_format == 'UUID'
        else:
            suffix += '/' + connection_id
        issued_at = datetime.now(timezone.utc)
        # Never retry issuance: SUZ reissue invalidates the previous connection token.
        if runtime:
            runtime.assert_owner()
        if purpose == 'suz':
            runtime.guard.begin_suz(environment, str(inn), connection_id, oms_id, challenge['uuid'])
            runtime.suz_post_attempted = True
        result = _request(session, 'POST', base + suffix, json=payload, stage='issuance')
        if not isinstance(result, dict):
            raise TokenIssuanceError('Invalid issuance response', stage='issuance',
                                     reason='invalid_response', outcome_unknown=True)
        value = result.get('uuidToken' if purpose == 'true_api' and token_format == 'UUID' else 'token')
        try:
            if token_format == 'JWT':
                expiry, source = jwt_expiry(value).isoformat(), 'jwt_exp'
            elif purpose == 'true_api' or result.get('expireDate'):
                expiry, source = result.get('expireDate'), 'server'
            else:
                expiry, source = (issued_at + timedelta(hours=10)).isoformat(), 'suz_ttl'
            record = TokenRecord(purpose, environment, str(inn), token_format, value, expiry, source, base,
                                 connection_id=connection_id, oms_id=oms_id, auth_uuid=challenge['uuid'])
        except TokenValidationError as exc:
            exc.issuance_response_invalid = True
            raise
        try:
            verify_token(record, session, config)
        except Exception as exc:
            # Keep the actual stage (including detached ping signing) and an
            # allowlisted cause; never copy arbitrary exception text.
            stage = exc.stage if isinstance(exc, TokenIssuanceError) else 'verification'
            reason = exc.reason if isinstance(exc, TokenIssuanceError) else 'verification_failed'
            if purpose == 'suz':
                raise TokenIssuanceError('New SUZ token failed verification; previous connection token may be invalid',
                                         stage=stage, reason=reason, outcome_unknown=True) from None
            raise TokenIssuanceError('Issued token failed verification', stage=stage, reason=reason) from None
        return record


def get_new_token(inn, conid=None, mode='auth', timeout=None, *, oms_id=None, config=None):
    """Compatibility for JWT/SUZ callers. UUID True API requires the structured API."""
    if mode not in {'auth', 'jwt'}:
        raise TokenValidationError('Use issue_token for True API UUID so expiry is preserved')
    config = dict(load_config() if config is None else config)
    if timeout is not None:
        config['SIGNING_TIMEOUT'] = timeout
    return issue_token(inn, purpose='suz' if mode == 'auth' else 'true_api',
                       token_format='UUID' if mode == 'auth' else 'JWT',
                       connection_id=conid, oms_id=oms_id, config=config).token


def main(argv=None):
    from .tokens import TokenProcessor
    parser = argparse.ArgumentParser(description='Issue and verify a scoped CRPT token')
    parser.add_argument('--inn', required=True)
    parser.add_argument('--conid')
    parser.add_argument('--omsid')
    parser.add_argument('--mode', choices=['auth', 'jwt', 'uuid'], default='auth')
    parser.add_argument('--timeout', type=int, default=60)
    args = parser.parse_args(argv)
    try:
        with master_runtime():
            return _run_cli(args, TokenProcessor)
    except (Exception, MasterStopped) as exc:
        logger.error('Token generation failed (%s; %s)', type(exc).__name__, safe_diagnostic(exc))
        return 1


def _run_cli(args, TokenProcessor):
    try:
        processor = TokenProcessor(tokens_read_only=False)
        config = dict(processor.config, SIGNING_TIMEOUT=args.timeout)
        current_runtime().configure(config)
        purpose = 'suz' if args.mode == 'auth' else 'true_api'
        token_format = 'JWT' if args.mode == 'jwt' else 'UUID'
        if purpose == 'true_api' and token_format == 'UUID' and not processor.registry_enabled:
            raise TokenValidationError('Configure tokens_registry_path before issuing True API UUID')
        connection, oms = processor._scope(args.inn, purpose, args.conid, args.omsid,
                                           config.get('tokens_environment', 'production'))
        with processor.writer_lock():
            processor._sync_from_s3(required=True)
            record = issue_token(args.inn, purpose=purpose, token_format=token_format,
                                 connection_id=connection, oms_id=oms, config=config)
            processor.save_record(record)
        logger.info('Verified token published')
        return 0
    except Exception as exc:
        logger.error('Token generation failed (%s; %s)', type(exc).__name__, safe_diagnostic(exc))
        return 1


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    raise SystemExit(main())
