# Pallet PNG delivery on VPS Celery

`tasks.deliver_pallet_report` runs report/task validation, local GS1-128 rendering,
and MAX attachment delivery inside the released Celery worker. It is disabled
unless `XTREK_PALLET_DELIVERY_CONFIG` names an explicitly provisioned configuration
with `enabled: true`. The existing business report flow remains separate.

`python -m xtrek.pallet_label_report_poller` discovers S3 reports and publishes
persistent tasks to the existing local RabbitMQ queue. It has a SQLite outbox and
single-instance lock. It never uploads files or sends MAX messages. A crash after
broker acceptance may redeliver the same event; the delivery ledger reconciles
GET history and holds ambiguous POSTs. There is no automatic retry of a failed
Celery send task.

Provision configuration with task/report buckets, three explicitly allowed INNs,
exact `backlog_report_ids`/`backlog_source_uuids`, a UTC source cutoff, and the two
`previously_delivered_report_ids`. The protected uncertain UUID is excluded.
Production-order `Quantity` controls emission/printing preparation only; it does
not restrict equipment-report quantities or pallet PNG delivery. Label box and
product counts come from the actual scanned report. Report/task schema and assigned SSCCs must agree;
each pallet retains its existing assigned Fito SSCC (including historical extension 0), without allocating
another number. S3 reads use HEAD ETag + conditional GET.

The configuration must also contain `max_environment_file`, `ledger_path`,
`png_root`, `poller_ledger_path`, `history_start_ms`, `chat_id` and `bot_user_id`.
Recipient identity is fixed to the human-approved MAX chat `-72448994566325` and
bot `259171708`, with `/me` verification. The existing MAX environment provides
the token and certificate bundle; secrets are never put in broker payloads.

Initialize `DeliveryLedger` explicitly before starting delivery. Preserve prior
ledger and PNG files during release and rollback. Previously sent reports require
exact history GET confirmation and never issue another message POST. Ambiguous
POSTs require GET reconciliation, never clearing or resetting the ledger.

Install `requirements-pallet-labels.txt` in the released VPS environment. Add the
configuration path to the Celery and discovery-service environment, then deploy
full main and warm-restart only after active work is complete. Start discovery
after verifying the new Celery task is registered. Rollback stops discovery first,
keeps durable ledgers, and restores the previous complete release and settings.
No printer or hot-folder route is invoked by this pipeline.

For the human-authorized full undelivered backlog, inventory all reports and place
every valid report/task join in `backlog_report_ids`, then set
`all_valid_assigned_reports: true`. This uses quantities from scanned report boxes
and preserves existing pallet assignments; it never creates equipment or print
jobs from blank Quantity sources. Reports from those sources still receive pallet
PNGs when the report/task join and assigned SSCCs are valid. Future sources keep
the explicit cutoff guard. Invalid joins and uncertain POSTs remain held.

For a large available MAX history, `scan_history_batch` saves a durable VPS-only
checkpoint after every page and resumes in bounded batches until the server's
actual end. A partial scan cannot authorize a historical missing attachment.
The completed snapshot is bound to bot/chat, a SHA256 in root-owned config, and
`through_ms`. Celery merges its old attachment records with fresh overlapping
GET history (`history_start_ms <= through_ms`), so messages received while the
snapshot was being built are covered. Attachment candidates without exact compact
image-caption or legacy filename proof are held for reconciliation.
No raw history or historical SSCC export is needed.

The human-authorized bounded policy sets `allow_truncated_history: true`: absence
from the available 100 MAX pages permits delivery. An accepted truncated snapshot
must record exactly 100 pages and `policy: user-approved-100-pages`; its checksum,
bot/chat and fresh overlap remain mandatory. This is an available-history window,
not proof of global historical absence. Known previously GET-confirmed reports
are excluded independently of window coverage; uncertain POST ledger rows and
unconfirmed attachment candidates still never auto-repeat.
