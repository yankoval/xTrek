"""Offline regressions for token identity and persistence; no real credentials."""
import base64
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from itertools import permutations
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from xtrek.token_registry import TokenRecord, TokenRegistry, TokenValidationError, migrate_legacy_record

INN = '1234567890'
BASE = 'https://markirovka.crpt.ru/api/v3/true-api'
SANDBOX = 'https://markirovka.sandbox.crptech.ru/api/v3/true-api'
CON = 'aaaaaaaa-2222-4333-8444-555555555555'
OMS = 'bbbbbbbb-2222-4333-8444-555555555555'
NOW = datetime(2030, 1, 1, tzinfo=timezone.utc)
EXPIRY = NOW + timedelta(minutes=7)


def record(purpose='true_api', **kwargs):
    defaults = dict(purpose=purpose, environment='production', inn=INN, format='UUID',
                    token='11111111-2222-4333-8444-555555555555',
                    expireDate=EXPIRY.isoformat(), expiry_source='server', issuer_base_url=BASE)
    if purpose == 'suz':
        defaults.update(connection_id=CON, oms_id=OMS)
    defaults.update(kwargs)
    return TokenRecord(**defaults)


def jwt_record(**kwargs):
    payload = base64.urlsafe_b64encode(json.dumps({'inn': INN, 'exp': int(EXPIRY.timestamp())}).encode()).decode().rstrip('=')
    return record(format='JWT', token='eyJhbGciOiJIUzI1NiJ9.' + payload + '.offline-signature',
                  expiry_source='jwt_exp', **kwargs)


def select(registry, **kwargs):
    return registry.select(**dict(dict(purpose='true_api', environment='production', inn=INN, now=NOW), **kwargs))


class TokenRegistryTests(unittest.TestCase):
    def test_uuid_format_does_not_determine_purpose_or_environment(self):
        true = record()
        suz = record('suz')  # Even identical opaque values must not merge identities.
        sandbox = record(environment='sandbox', issuer_base_url=SANDBOX)
        another = record(inn='123456789012')
        for order in permutations([true, suz, sandbox, another]):
            registry = TokenRegistry(order)
            self.assertEqual(select(registry), true)
            self.assertEqual(select(registry, purpose='suz', connection_id=CON, oms_id=OMS), suz)
            self.assertEqual(select(registry, environment='sandbox'), sandbox)

    def test_jwt_uuid_coexistence_requires_explicit_policy(self):
        for order in permutations([record(), jwt_record(), record('suz')]):
            registry = TokenRegistry(order)
            with self.assertRaises(TokenValidationError):
                select(registry)
            self.assertEqual(select(registry, preferred_format='UUID'), record())
            self.assertEqual(select(registry, preferred_format='JWT'), jwt_record())

    def test_selected_value_and_ttl_are_the_same_record(self):
        registry = TokenRegistry([record(), record('suz', expireDate=(NOW+timedelta(hours=9)).isoformat())])
        chosen = select(registry)
        self.assertEqual(chosen.remaining_seconds(NOW), 420)
        self.assertEqual(chosen.expireDate, EXPIRY.isoformat())

    def test_independent_updates_preserve_jwt_suz_and_connections(self):
        true, jwt, suz = record(), jwt_record(), record('suz')
        other = record('suz', connection_id='cccccccc-2222-4333-8444-555555555555')
        for order in permutations([true, jwt, suz, other]):
            registry = TokenRegistry(order)
            new = replace(true, token='99999999-2222-4333-8444-555555555555')
            registry.upsert(new)
            registry.upsert(replace(jwt, token=jwt.token+'x'))
            registry.upsert(replace(suz, token='88888888-2222-4333-8444-555555555555'))
            self.assertEqual(len(registry.records), 4)
            self.assertEqual(select(registry, preferred_format='UUID'), new)
            self.assertEqual(select(registry, purpose='suz', connection_id=other.connection_id, oms_id=OMS), other)

    def test_exact_expiry_is_inactive_and_no_format_fallback(self):
        expired = record(expireDate=NOW.isoformat())
        registry = TokenRegistry([expired, jwt_record()])
        self.assertIsNone(select(registry, preferred_format='UUID'))
        self.assertEqual(select(registry).format, 'JWT')

    def test_unsafe_or_incomplete_identity_is_rejected(self):
        for kwargs in [dict(purpose='unknown'), dict(environment='unknown'), dict(inn='123'),
                       dict(issuer_base_url=SANDBOX), dict(connection_id=CON),
                       dict(token='not-a-uuid'), dict(format='auth')]:
            with self.subTest(kwargs=kwargs), self.assertRaises(TokenValidationError):
                record(**kwargs)
        with self.assertRaises(TokenValidationError):
            record('suz', connection_id=None)
        with self.assertRaises(TokenValidationError):
            select(TokenRegistry([record('suz')]), purpose='suz')

    def test_connection_identifiers_are_case_normalized(self):
        r = record('suz', connection_id=CON.upper(), oms_id=OMS.upper())
        self.assertEqual(select(TokenRegistry([r]), purpose='suz', connection_id=CON.upper(), oms_id=OMS), r)

    def test_expiry_source_and_timezone_are_not_guessed(self):
        for kwargs in [dict(expireDate=None), dict(expireDate='2030-01-01T10:00:00'),
                       dict(expireDate='broken'), dict(expiry_source='suz_ttl')]:
            with self.subTest(kwargs=kwargs), self.assertRaises(TokenValidationError):
                record(**kwargs)
        with self.assertRaises(TokenValidationError):
            replace(jwt_record(), expireDate=(EXPIRY+timedelta(hours=1)).isoformat())
        r = record(expireDate='2030-01-01T03:07:00+03:00')
        self.assertEqual(r.remaining_seconds(NOW), 420)

    def test_jwt_metadata_cannot_cross_organizations(self):
        with self.assertRaises(TokenValidationError):
            jwt_record(inn='9999999999')

    def test_duplicate_storage_keys_are_not_order_resolved(self):
        with self.assertRaises(TokenValidationError):
            TokenRegistry([record(), replace(record(), token='99999999-2222-4333-8444-555555555555')])

    def test_legacy_schema_is_not_automatically_interpreted(self):
        for value in [[{'Токен':record().token}], {'schema_version':3, 'tokens':[]},
                      {'schema_version':2, 'tokens':[{'token':'secret'}]}]:
            with self.subTest(value=value), self.assertRaises(TokenValidationError):
                TokenRegistry.from_dict(value)

    def test_explicit_legacy_jwt_migration_uses_exp(self):
        jwt = jwt_record()
        migrated = migrate_legacy_record({'Токен':jwt.token, 'ДействуетДо':'2099-01-01T00:00:00'},
                                         purpose='true_api', environment='production', inn=INN, issuer_base_url=BASE)
        self.assertEqual(migrated.expireDate, jwt.expireDate)

    def test_legacy_suz_needs_known_connection_and_wall_clock_timezone(self):
        legacy = {'Токен':record().token, 'Идентификатор':CON, 'ДействуетДо':'2030-01-01T03:07:00'}
        context = dict(purpose='suz', environment='production', inn=INN, issuer_base_url=BASE,
                       connection_id=CON, oms_id=OMS)
        with self.assertRaises(TokenValidationError):
            migrate_legacy_record(legacy, **context)
        migrated = migrate_legacy_record(legacy, **context, legacy_timezone='Europe/Moscow')
        self.assertEqual(migrated.remaining_seconds(NOW), 420)
        with self.assertRaises(TokenValidationError):
            migrate_legacy_record(dict(legacy, Идентификатор='wrong'), **context, legacy_timezone='Europe/Moscow')

    def test_legacy_uuid_trueapi_requires_original_server_expiry(self):
        args = dict(purpose='true_api', environment='production', inn=INN, issuer_base_url=BASE)
        with self.assertRaises(TokenValidationError):
            migrate_legacy_record({'Токен':record().token, 'ДействуетДо':EXPIRY.isoformat()}, **args)
        r = migrate_legacy_record({'Токен':record().token, 'expireDate':EXPIRY.isoformat()}, **args)
        self.assertEqual(r.purpose, 'true_api')

    def test_persistence_roundtrip_permissions_and_failed_replace(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder)/'registry.json'
            registry = TokenRegistry([record(), jwt_record(), record('suz')])
            registry.save(path)
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)
            self.assertEqual(TokenRegistry.load(path).to_dict(), registry.to_dict())
            before = path.read_bytes()
            registry.upsert(record(inn='9999999999'))
            with patch('xtrek.token_registry.os.replace', side_effect=OSError('unavailable')):
                with self.assertRaises(OSError):
                    registry.save(path)
            self.assertEqual(path.read_bytes(), before)
            self.assertEqual(len(list(Path(folder).iterdir())), 1)

    def test_repr_and_validation_errors_do_not_leak_secrets(self):
        secret = 'invalid-secret'
        with self.assertRaises(TokenValidationError) as caught:
            record(token=secret)
        self.assertNotIn(secret, str(caught.exception))
        self.assertNotIn(record().token, repr(record()))


if __name__ == '__main__':
    unittest.main()
