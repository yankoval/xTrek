"""Explicitly enabled terminal feedback; shared coordination and a separate drain.

No business writes, source discovery, event publication, guard changes or broker
administration. Shared S3 state supports multiple workers; SQLite remains single-host only.
"""
import argparse
import json
import os
import logging
from pathlib import Path

from .terminal_feedback import TerminalLedger, RouteConfigurationError, select_route, source_stage

PROTECTED_IDS = {'623c20fa-f90c-4478-aeae-9c6635cedc9f'}


def read_settings(path):
    settings = json.loads(Path(path).read_text())
    if settings.get('version') != 1:
        raise ValueError('Unsupported feedback configuration')
    if settings.get('enabled') is not True:
        return None
    for key in ('operator_routes', 'routes', 'production_order_overrides'):
        if not isinstance(settings.get(key), dict):
            raise ValueError('Missing feedback mapping')
    if not isinstance(settings.get('protected_ids'), list):
        raise ValueError('Explicit protection list required')
    if settings.get('ledger_backend', 'sqlite') not in {'sqlite', 's3'}:
        raise ValueError('Unknown feedback ledger backend')
    if settings.get('ledger_backend') == 's3':
        if not isinstance(settings.get('ledger_path'), str) or not settings['ledger_path'].startswith('s3://'):
            raise ValueError('Shared feedback ledger requires S3')
    elif not Path(settings['ledger_path']).is_absolute():
        raise ValueError('Feedback ledger must use an absolute persistent path')
    return settings


def worker_ledger(config, bucket, key):
    path = config.get('terminal_feedback_config')
    if not path or bucket != config.get('internal_bucket', '20ab2a0c-2726-4ba1-9c7c-7deae82941ff'):
        return None
    settings = read_settings(path)
    if settings is None or not source_stage(key):
        return None
    if any(identity and identity in key for identity in PROTECTED_IDS | set(settings['protected_ids'])):
        return None
    return open_ledger(settings, config)


def open_ledger(settings, config):
    if settings.get('ledger_backend') == 's3':
        from .storage import get_storage
        from .terminal_feedback_s3 import S3TerminalLedger
        root = settings['ledger_path']
        return S3TerminalLedger(get_storage(root, config.get('s3_config')), root)
    return TerminalLedger(settings['ledger_path'])


def _read_s3_json(config, path):
    from .storage import get_storage
    if not path.startswith('s3://'):
        raise ValueError('Feedback source must be S3')
    return json.loads(get_storage(path, config.get('s3_config')).read_text(path))


def _identifier(value):
    if (not isinstance(value, str) or not value or value in {'.', '..'}
            or any(c in value for c in ('/', '\\', '\x00', '\n', '\r'))):
        raise ValueError('Source identity is not an exact object name')
    return value


def resolve_order(row, settings, config, read_json=_read_s3_json):
    key = row['source_key']
    prefix = config.get('event_key_prefix', '').strip('/')
    if prefix:
        if not key.startswith(prefix + '/'):
            raise ValueError('Source outside configured namespace')
        key = key[len(prefix) + 1:]
    root = f"s3://{row['bucket']}/" + (prefix + '/' if prefix else '')
    folder, name = key.split('/')
    task_id = _identifier(name[:-5])
    override = settings['production_order_overrides'].get(key)
    if override is not None:
        order_id = _identifier(override)
    elif folder.startswith('equipment-reports'):
        order_id = task_id
    elif folder.startswith('cisInformationChange'):
        # Legacy CIS documents do not carry productionOrderId. Never infer by
        # stripping a suffix; an exact audited mapping is required for them.
        raise ValueError('CIS source needs an explicit productionOrder mapping')
    else:
        receipt = read_json(config, root + key)
        order_id = _identifier(receipt.get('productionOrderId'))
    if any(identity and identity in order_id for identity in PROTECTED_IDS | set(settings['protected_ids'])):
        raise ValueError('Protected source is held')
    order_root = config.get('production_orders_path') or root + 'productionOrders/'
    order = read_json(config, order_root.rstrip('/') + '/' + order_id + '.json')
    # Read only the exact field requested. No fallback to report.operator or aliases.
    return order_id, order.get('operator')


def drain(settings, config, *, transport_factory=None, read_json=_read_s3_json):
    from .terminal_feedback_max import MaxTerminalTransport, deliver
    ledger = open_ledger(settings, config)
    if transport_factory is None:
        import requests

        def transport_factory(route, row):
            token = os.environ[route['token_env']]
            session = requests.Session()
            session.verify = route.get('ca_file') or True
            return MaxTerminalTransport(session=session, token=token, route=route,
                                        created_at=row['created_at'],
                                        max_history_pages=settings.get('max_history_pages', 100))
    counts = {}
    for row in ledger.pending():
        try:
            if not row['route_json']:
                if row['production_order_id'] is None:
                    order_id, operator = resolve_order(row, settings, config, read_json)
                else:
                    # Freeze the first resolved source/operator for audit. Route configuration
                    # can be repaired later without silently changing the source.
                    order_id, operator = row['production_order_id'], json.loads(row['operator_raw'])
                # Freeze the exact source before route validation, including a broken default.
                row = ledger.bind_route(row['event_id'], order_id, operator, None)
                route = select_route(settings, json.loads(row['operator_raw']))
                row = ledger.bind_route(row['event_id'], row['production_order_id'],
                                        json.loads(row['operator_raw']), route)
            if not row['route_json']:
                state = 'ROUTE_HELD'
            else:
                route = json.loads(row['route_json'])
                state = deliver(ledger, row, transport_factory(route, row))
        except RouteConfigurationError as error:
            logging.getLogger(__name__).error('%s; event=%s remains pending', error, row['event_id'])
            state = 'ROUTE_CONFIGURATION_ERROR'
        except Exception:
            # Pending stays durable. Only safe aggregate state leaves this command.
            state = 'HELD_OR_RETRYABLE'
        counts[state] = counts.get(state, 0) + 1
    return counts


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    initialize = commands.add_parser('init')
    initialize.add_argument('--ledger', required=True)
    run = commands.add_parser('drain')
    run.add_argument('--settings', required=True)
    args = parser.parse_args()
    if args.command == 'init':
        TerminalLedger.initialize(args.ledger)
        print('Terminal ledger initialized; delivery is not activated')
    else:
        settings = read_settings(args.settings)
        if settings is None:
            print('DISABLED')
            return
        from .config_loader import load_config
        counts = drain(settings, load_config('suz_worker_config'))
        print(json.dumps(counts, sort_keys=True))
        if any(state != 'CONFIRMED' for state in counts):
            raise SystemExit(2)


if __name__ == '__main__':
    main()
