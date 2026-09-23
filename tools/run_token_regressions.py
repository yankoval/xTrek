#!/usr/bin/env python3
"""Run the migration regression suite locally; reject real socket connections."""
import os
from pathlib import Path
import socket
import sys

TEST_MODULES = (
    'token_registry', 'token_purpose_integration', 'token_worker', 'token_regressions',
    'token_master_resilience',
    'tokens_read_only', 'tokens_new', 'token_logic_fix', 'document_send_idempotency',
    'suz_new_methods', 'aggregate_operations', 'cis_information_change', 'create_emission_task',
    'create_virtual_tasks', 'equipment_set_report_from_report', 'fallback_logic', 'get_kodes',
    'introduce', 'introduce_permits', 'permit_filtering', 'utils',
)


def main():
    root = Path(__file__).resolve().parents[1]
    os.chdir(root)
    sys.path.insert(0, str(root))
    for key in list(os.environ):
        if key.startswith(('AWS_', 'YMQ_')) or key in {
            'TOKENS_CONFIG', 'token_config', 'suz_worker_config', 'HONEST_SIGN_TOKEN',
            'TRUE_API_TOKEN', 'TRUE_API_HOST', 'NK_API_HOST', 'CLIENT_TOKEN', 'OMSID',
            'FIND_TOKEN_BY_INN', 'API_KEY',
        }:
            os.environ.pop(key, None)
    os.environ['AWS_EC2_METADATA_DISABLED'] = 'true'
    os.environ['PYTEST_DISABLE_PLUGIN_AUTOLOAD'] = '1'
    import pytest

    def blocked(*args, **kwargs):
        raise AssertionError('Real network connections are forbidden in token regressions')

    original_connect, original_connect_ex = socket.socket.connect, socket.socket.connect_ex
    socket.socket.connect = socket.socket.connect_ex = blocked
    try:
        return pytest.main(['-q', *['tests/test_' + name + '.py' for name in TEST_MODULES], *sys.argv[1:]])
    finally:
        socket.socket.connect, socket.socket.connect_ex = original_connect, original_connect_ex


if __name__ == '__main__':
    raise SystemExit(main())
