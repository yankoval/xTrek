# Temporary S3 polling mode

This mode replaces YMQ event delivery with read-only S3 LIST polling and publishes the existing `{bucket, key}` task envelope to a **local durable RabbitMQ** queue. `tasks.process_s3_event` remains the business router. The durable SQLite outbox is the source of replayable discovery records; a crash between RabbitMQ publish and SQLite update can publish a duplicate, so the existing operation guards remain mandatory. Equipment-task chat delivery is handled by the separate local notifier and its own durable ledger.

Enable only on a single host after provisioning RabbitMQ with persistent storage and confirming the local credentials. Set:

```ini
XTREK_LOCAL_S3_POLLING=1
XTREK_LOCAL_BROKER_URL=amqp://DEDICATED_USER:PASSWORD@127.0.0.1/xtrek
XTREK_S3_POLL_LEDGER=/var/lib/xtrek/s3-poller/outbox.sqlite3
XTREK_S3_POLL_SECONDS=20
XTREK_S3_CUTOFF_UTC=2026-10-11T01:00:00Z
XTREK_S3_ACCESS_KEY=...
XTREK_S3_SECRET_KEY=...
```

The Celery queue is durable, messages are persistent, and the AMQP connection requests publisher confirms. Use a dedicated loopback RabbitMQ user and non-default vhost; `guest` and remote broker hosts are rejected. The RabbitMQ service must itself have durable storage and start before the poller and Celery worker. Do not use Kombu `filesystem://` or SQLAlchemy transport here: their current implementations remove/mark a message invisible at receive time and do not reclaim it after worker loss.

Run discovery with `python -m xtrek.s3_poller --role scan` and independent delivery with `python -m xtrek.s3_poller --role publish`. `--once` runs one cycle of that role. The old combined CLI is no longer supported. Discovery preserves the existing cutoff, per-prefix cursors and stable identities, checks known identities before JSON GET, and persists failed-object retries before advancing a page. See [reliability, metrics, migration and rollback](s3-poller-reliability.md) for budgets, error handling and the rollback gate.

Celery task lifecycle hooks record `started`, `retry_scheduled`, `succeeded`, and terminal `failed` states in the ledger. RabbitMQ publisher confirms and persistent durable messages are required; `acks_late` and `task_reject_on_worker_lost` keep an unacked message eligible for broker redelivery after worker loss. Retry/countdown messages remain broker-owned. A terminal failure is evidence for review, not an instruction to replay; the existing `uncertain` external-operation guards must never be bypassed. Stable Celery task IDs are not treated as deduplication.

The `xtrek-max-task-notifier.service` owns `equipment-tasks/T-*.json` and its own SQLite delivery ledger. It scans only objects at or after `XTREK_MAX_TASK_CUTOFF_UTC`, unless an exact key is in the optional line-based allowlist. Before POST it requires complete MAX history through `XTREK_MAX_HISTORY_CUTOFF_UTC`, verifies the exact S3 object and its 48-hour signed GET link, and reserves a SQLite CAS attempt. After an ambiguous POST it performs GET reconciliation only; it never automatically posts again. `printer-tasks/` CSV/VDF artifacts are a separate contract and are not sent by this notifier.

Install `xtrek-s3-poller.service`, `xtrek-s3-publisher.service` and `xtrek-max-task-notifier.service` from `deploy/systemd/`; all run as `xtrek`, load `/etc/xtrek/local-s3-mode.env`, and keep writable state under `/var/lib/xtrek`. This change requires an explicit UTC cutoff. Before production activation, verify RabbitMQ persistence and publisher confirms, static credential access to S3, all systemd units, and safe warm-drain of the old consumer. A worker-crash recovery probe is a known release limitation if it cannot be performed without risking live work. External operation guards in `operation_state.py` must not be bypassed or reset during migration.
