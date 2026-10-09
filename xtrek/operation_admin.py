"""Explicit CAS recovery using a death proof collected on the owner's host.

No time-based takeover. A lost external reply requires an independently verified
result supplied by the operator; this command never calls a business API.
"""
import argparse
import json
from pathlib import Path
import time
from uuid import uuid4

from .config_loader import load_config
from .operation_state import OperationConflict, encode_result
from .storage import get_storage
from .token_master_lock import owner_identity, owner_is_dead


def prove_dead(owner):
    observer = owner_identity()
    return {'owner': owner, 'observer': observer, 'dead': owner_is_dead(owner, observer),
            'observed_at': time.time()}


def recover(storage, path, proof, result=None):
    stored = storage.read_lock_object(path)
    if not stored:
        raise OperationConflict('State object does not exist')
    value, etag = json.loads(stored[0]), stored[1]
    owner = value.get('owner')
    observer = proof.get('observer', {})
    if (not proof.get('dead') or proof.get('owner') != owner
            or observer.get('host') != owner.get('host')
            or observer.get('machine') != owner.get('machine')
            or not 0 <= time.time() - proof.get('observed_at', 0) <= 120):
        raise OperationConflict('Fresh death proof from the exact owner host is required')
    phase = value.get('phase')
    if phase == 'held' and result is None:
        value['phase'] = 'ready'
    elif phase in {'external', 'uncertain'} and result is not None:
        if not isinstance(result, dict) or not result:
            raise OperationConflict('A verified structured result is required')
        value['phase'] = 'accepted'
        value['result'] = encode_result(result)
        value['replay_allowed'] = True
    else:
        raise OperationConflict('This phase cannot be recovered by this action')
    value.update(updated_at=time.time(), recovery_id=str(uuid4()), recovery_proof=proof)
    if storage.write_lock_object(path, json.dumps(value), etag) is None:
        raise OperationConflict('State changed during reconciliation; recovery cancelled')
    return {'path': path, 'phase': value['phase'], 'business_api_calls': 0}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='command', required=True)
    proof = sub.add_parser('prove-dead')
    proof.add_argument('--owner-file', required=True)
    recovery = sub.add_parser('recover')
    recovery.add_argument('--state-path', required=True)
    recovery.add_argument('--proof-file', required=True)
    recovery.add_argument('--verified-result-file')
    args = parser.parse_args()
    if args.command == 'prove-dead':
        print(json.dumps(prove_dead(json.loads(Path(args.owner_file).read_text()))))
        return
    config = load_config('suz_worker_config')
    root = config.get('operation_state_path', '').rstrip('/') + '/'
    if root == '/' or not args.state_path.startswith(root):
        parser.error('state-path must be inside configured operation_state_path')
    result = json.loads(Path(args.verified_result_file).read_text()) if args.verified_result_file else None
    print(json.dumps(recover(get_storage(root, config.get('s3_config')), args.state_path,
                             json.loads(Path(args.proof_file).read_text()), result)))


if __name__ == '__main__':
    main()
