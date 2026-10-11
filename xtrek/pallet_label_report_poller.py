"""VPS discovery only: publish durable local Celery tasks; never contact MAX."""
from __future__ import annotations
import fcntl
import hashlib
import json
import os
import sqlite3
import time
from datetime import datetime
from pathlib import Path


def main():
    import boto3
    from .tasks import app, config
    settings = json.loads(Path(os.environ['XTREK_PALLET_DELIVERY_CONFIG']).read_text())
    if settings.get('enabled') is not True:
        raise RuntimeError('Pallet delivery is disabled')
    broker = app.conf.broker_url
    from urllib.parse import urlsplit
    if urlsplit(broker).hostname not in {'localhost', '127.0.0.1', '::1'}:
        raise RuntimeError('Discovery requires the local RabbitMQ broker')
    path = Path(settings['poller_ledger_path'])
    lock = path.with_suffix('.lock').open('a')
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    db = sqlite3.connect(path)
    db.execute('PRAGMA synchronous=FULL')
    db.execute('CREATE TABLE IF NOT EXISTS events (id TEXT PRIMARY KEY, report_key TEXT NOT NULL, state TEXT NOT NULL)')
    db.commit()
    client = boto3.client('s3', **config['s3_config'])
    cutoff = datetime.fromisoformat(settings['source_cutoff_utc'])
    protected = '623c20fa-f90c-4478-aeae-9c6635cedc9f'
    while True:
        for page in client.get_paginator('list_objects_v2').paginate(Bucket=settings['report_bucket'], Prefix='equipment-reports/'):
            for obj in page.get('Contents', []):
                key = obj['Key']
                if not key.endswith('.json') or '/' in key[len('equipment-reports/'):]:
                    continue
                rid = key.rsplit('/', 1)[-1][:-5]
                if rid.endswith(protected):
                    continue
                if (rid not in settings['backlog_report_ids'] and rid[-36:] not in settings['backlog_source_uuids']
                        and obj['LastModified'] <= cutoff):
                    continue
                event_id = hashlib.sha256((key + '\0' + obj['ETag']).encode()).hexdigest()
                db.execute('INSERT OR IGNORE INTO events VALUES (?,?,?)', (event_id, key, 'PENDING'))
        db.commit()
        for event_id, key in db.execute("SELECT id,report_key FROM events WHERE state='PENDING'").fetchall():
            # Crash after broker acceptance can redeliver; the Celery delivery ledger holds duplicate/uncertain POSTs.
            with app.connection_for_write() as connection:
                connection.transport_options['confirm_publish'] = True
                with app.amqp.Producer(connection) as producer:
                    app.send_task('tasks.deliver_pallet_report', args=[key], task_id='pallet-' + event_id,
                                  producer=producer, delivery_mode=2, retry=False)
            db.execute("UPDATE events SET state='SENT' WHERE id=?", (event_id,))
            db.commit()
            print(json.dumps({'state': 'PUBLISHED', 'report_key': key, 'task_id': 'pallet-' + event_id}), flush=True)
        time.sleep(30)


if __name__ == '__main__':
    main()
