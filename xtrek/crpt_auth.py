"""Issue scoped CRPT tokens through the existing SignJS folder protocol."""
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

logger = logging.getLogger(__name__)
AUTH_BASES = {
    'production': 'https://markirovka.crpt.ru/api/v3/true-api',
    'sandbox': 'https://markirovka.sandbox.crptech.ru/api/v3/true-api',
}
SUZ_BASES = {'production': 'https://suzgrid.crpt.ru', 'sandbox': 'https://suz.sandbox.crptech.ru'}


class TokenIssuanceError(RuntimeError):
    """Safe operational error: never include response bodies or credentials."""


def sign_data(data, inn, config, *, detached=False):
    directory = config.get('sign')
    timeout = float(config.get('SIGNING_TIMEOUT', 60))
    if not directory or not 0 < timeout <= 600:
        raise TokenIssuanceError('Configure sign and SIGNING_TIMEOUT (1..600 seconds)')
    storage = get_storage(directory, config.get('s3_config'))
    # SignJS chooses attached CAdES for .txt and detached CAdES for .json.
    source = f"{directory.rstrip('/')}/{inn}_{uuid4()}_dataToSign.{'json' if detached else 'txt'}"
    signed = source + '.sig'
    try:
        storage.write_text(source, data)
        deadline = time.monotonic() + timeout
        while not storage.exists(signed):
            checkpoint()
            if time.monotonic() >= deadline:
                raise TokenIssuanceError('Signing timed out')
            time.sleep(min(1, max(0, deadline - time.monotonic())))
        signature = ''.join(storage.read_text(signed).split())
        if not signature:
            raise TokenIssuanceError('Empty signature')
        base64.b64decode(signature, validate=True)
    except TokenIssuanceError:
        raise
    except Exception:
        raise TokenIssuanceError('Signing folder operation failed') from None
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


def _request(session, method, url, *, attempts=1, **kwargs):
    for attempt in range(attempts):
        checkpoint()
        try:
            response = session.request(method, url, timeout=request_timeout(), allow_redirects=False, **kwargs)
        except requests.RequestException:
            if attempt + 1 < attempts:
                time.sleep(attempt + 1)
                continue
            raise TokenIssuanceError('CRPT transport failed; issuance outcome may be unknown') from None
        if response.status_code == 200:
            try:
                return response.json()
            except ValueError:
                raise TokenIssuanceError('CRPT returned invalid JSON') from None
        if (response.status_code == 429 or response.status_code >= 500) and attempt + 1 < attempts:
            time.sleep(attempt + 1)
            continue
        raise TokenIssuanceError(f'CRPT request failed with HTTP {response.status_code}')


def verify_token(record, session, config):
    if record.remaining_seconds() <= 0:
        raise TokenIssuanceError('Issued token has already expired')
    headers = {'Accept': 'application/json'}
    if record.purpose == 'suz':
        path = '/api/v3/ping?' + urlencode({'omsId': record.oms_id})
        headers['clientToken'] = record.token
        if config.get('suz_ping_sign', False):
            headers['X-Signature'] = sign_data(path, record.inn, config, detached=True)
        result = _request(session, 'GET', SUZ_BASES[record.environment] + path,
                          headers=headers, attempts=3)
        if (not isinstance(result, dict) or str(result.get('omsId', '')).lower() != record.oms_id
                or not result.get('apiVersion') or not result.get('omsVersion')):
            raise TokenIssuanceError('SUZ ping did not confirm OMS and API versions')
    else:
        headers['Authorization'] = 'Bearer ' + record.token
        _request(session, 'GET', record.issuer_base_url + '/elk/product-groups/balance/all',
                 headers=headers, attempts=3)
    if record.remaining_seconds() <= 0:
        raise TokenIssuanceError('Token expired during verification')


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
    if not config.get('sign'):
        raise TokenIssuanceError('Signing directory is not configured')
    base = AUTH_BASES[environment]
    with requests.Session() as session:
        session.trust_env = False
        session.verify = True
        challenge = _request(session, 'GET', base + '/auth/key')
        if not isinstance(challenge, dict) or not isinstance(challenge.get('data'), str) or not challenge['data']:
            raise TokenIssuanceError('Invalid signing challenge')
        TokenRecord._uuid(challenge.get('uuid'), 'auth_uuid')
        signature = sign_data(challenge['data'], str(inn), config)
        payload = {'uuid': challenge['uuid'], 'data': signature, 'inn': str(inn)}
        suffix = '/auth/simpleSignIn'
        if purpose == 'true_api':
            payload['unitedToken'] = token_format == 'UUID'
        else:
            suffix += '/' + connection_id
        issued_at = datetime.now(timezone.utc)
        # Never retry issuance: SUZ reissue invalidates the previous connection token.
        runtime = current_runtime()
        if runtime:
            runtime.assert_owner()
        result = _request(session, 'POST', base + suffix, json=payload)
        if not isinstance(result, dict):
            raise TokenIssuanceError('Invalid issuance response')
        value = result.get('uuidToken' if purpose == 'true_api' and token_format == 'UUID' else 'token')
        if token_format == 'JWT':
            expiry, source = jwt_expiry(value).isoformat(), 'jwt_exp'
        elif purpose == 'true_api' or result.get('expireDate'):
            expiry, source = result.get('expireDate'), 'server'
        else:
            expiry, source = (issued_at + timedelta(hours=10)).isoformat(), 'suz_ttl'
        record = TokenRecord(purpose, environment, str(inn), token_format, value, expiry, source, base,
                             connection_id=connection_id, oms_id=oms_id, auth_uuid=challenge['uuid'])
        try:
            verify_token(record, session, config)
        except Exception:
            if purpose == 'suz':
                raise TokenIssuanceError('New SUZ token failed verification; previous connection token may be invalid') from None
            raise
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
        logger.error('Token generation failed (%s)', type(exc).__name__)
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
        logger.error('Token generation failed (%s)', type(exc).__name__)
        return 1


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    raise SystemExit(main())
