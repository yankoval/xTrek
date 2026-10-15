# Bounded SSCC backups

`python -m xtrek.sscc_backup` keeps **at most five confirmed gzip SQLite snapshots**
in a dedicated private S3 bucket: three rolling changed states and two daily
checkpoints. A sixth, temporary candidate is allowed during upload or recovery
from an interrupted run. On the first check after a UTC day change, the last rolling state observed on
the previous day is preserved in a daily slot before rolling replacement. The
latest two such prior-day states are retained. Empty slots fill naturally. An
unchanged ledger does not invent new distinct daily states; after a gap, retain
the latest available state rather than claiming to reconstruct an unobserved day.

The timer checks every five minutes. Each check uses SQLite's online backup API,
validates `integrity_check`, prefix/extension, contiguous allocation ranges and
`next_serial`. Identical SHA-256 content skips rolling uploads; on a new UTC day
the previous day's latest state is preserved once if not already retained. Successful unchanged checks are logged.
Snapshots use deterministic gzip compression. No allocation API is called.

Before replacing the oldest eligible slot, the candidate is uploaded, downloaded,
decompressed and verified (SHA-256, schema and watermark). Each final slot is
also downloaded and verified. The candidate is removed only after successful
verification. A failed upload/check preserves the other confirmed slots. If a
PUT succeeds but its response is lost, retry reads existing slots and safely
converges. A failed final GET leaves the candidate for recovery. Every retained
slot and candidate is checked before the next rotation; corrupt objects, a source
watermark below any retained watermark, or a backwards clock fail closed and
require investigation. Do not "fix" these failures by deleting backup objects.

Only one producer is supported, on the allocator host. A nonblocking `flock`
serializes local runs; it is not a multi-host distributed lock. An S3 error must
fail the unit visibly, never silently fall back to the operation bucket.

## Storage boundaries

Production backup bucket: `xtrek-sscc-backups-20261015-01`, private, 512 MiB limit.
Only `sscc-backup/v1/{rolling-0,rolling-1,rolling-2,daily-0,daily-1}.sqlite3.gz`
and `sscc-backup/v1/candidate.sqlite3.gz` may be overwritten/deleted by this code.
Versioning must never have been enabled (both Enabled and Suspended are rejected),
no expiry lifecycle or business notification triggers may target these objects.
Use a dedicated, otherwise empty bucket; do not enable versioning later.

`sscc_backup_bucket` is explicit and **must differ** from the bucket in
`operation_state_path`. The latter is only read to enforce this boundary; it is
never changed. No old `local-sscc-backups/` object, operation guard or business
object is listed, modified or deleted. Existing legacy snapshots remain untouched.
A bucket size limit is a guard, not an automatic deletion policy.

## Release and activation

1. Merge the reviewed PR and deploy the **complete main release** by the normal
   procedure. Do not point a production unit/PYTHONPATH at a PR checkout or copy a
   single changed module into the running release.
2. Preserve the existing backup script, service/timer, configuration and release
   for rollback. Wait for any existing backup service to finish, then stop its
   timer before changing the unit. Leave the allocator and business workers running.
3. Copy the current worker JSON configuration to a separate private file
   `/etc/xtrek/sscc-backup-config.json` (mode 0600 or 0640, readable by xtrek), add
   `"sscc_backup_bucket": "xtrek-sscc-backups-20261015-01"`. Keep its existing
   `operation_state_path`, namespace and S3 credentials unchanged. Do not commit
   credentials. Install private `/etc/xtrek/sscc-backup.env` containing
   `SSCC_BACKUP_CONFIG=/etc/xtrek/sscc-backup-config.json`. This config is loaded
   last, after normal worker configuration, without changing it.
4. Confirm private ACL, versioning never enabled, no lifecycle or event triggers;
   verify existing worker credentials can GET/PUT these six keys and DELETE only
   the candidate. Install both unit files from `deploy/systemd`, daemon-reload,
   start the oneshot and inspect exit status and safe JSON summary.
5. Enable/start the timer only after the initial isolated restore below succeeds.
   Check a second unchanged run performs no upload, and a later changed state
   advances the recorded watermark. Monitor last successful **check**, not merely
   object age (an unchanged ledger intentionally leaves old captured timestamps).
6. Verify the next legacy timer cycle creates no new legacy objects. Existing
   legacy objects remain for a separately approved retention decision.

To roll back, stop the new timer between runs, restore the saved original unit,
script and environment, daemon-reload and restart the old timer. Preserve both
buckets and all confirmed snapshots. This resumes legacy growth; monitor capacity.
Never restore the live SSCC ledger as part of a code/service rollback.

## Isolated restore drill

As the configured service user, with its environment and full main release:

```sh
/srv/xtrek/venv/bin/python -m xtrek.sscc_backup --config SSCC_BACKUP_CONFIG \
  --restore-slot rolling-0.sqlite3.gz \
  --restore-directory /var/lib/xtrek-sscc/restore-drill-UNIQUE
```

Choose an occupied slot. The destination directory must not exist; the command
creates a private directory/file, refuses replacement, and verifies the downloaded
snapshot again. It **does not** replace the live ledger, start a service, allocate
SSCCs, or claim that a historical counter is safe for production. Compare all
three restored tables with the original immutable snapshot used for the upload,
not with a live database that may have advanced during the drill. Production
recovery requires independent reconciliation of every committed allocation;
a valid but old counter can reuse already issued numbers. Retain the newest
verified backup when investigating a corrupt/rolled-back source.

New backup volume is bounded by five times the compressed database size (plus
one candidate), rather than elapsed time. The 512 MiB bucket limit does not
reserve prepaid space. A snapshot exceeding the 64 MiB safety bound fails rather
than silently losing backups; review limits before approaching that size.

## Validation of the proposed release (2026-10-15)

- Full suite: 1304 passed, 10 skipped, 14 subtests passed.
- Real allocator snapshot: 1,605,632 raw bytes, 311,849 gzip bytes.
- Uploaded/read back using the existing production worker's S3 identity in the
  dedicated private bucket; repeated unchanged check uploaded no new snapshot.
- Restored into a new isolated directory as Unix user `xtrek`. All rows of
  `ledger_meta` (2), `counters` (1), and `allocations` (1382) matched the frozen
  source; SHA-256 and application watermark checks passed. No SSCC was allocated
  in the drill and the live database was not restored/replaced.
- The integration drill caught title-cased user metadata returned by the S3
  provider. Header names are normalized and a regression test covers that case.
- First-day initialization has one rolling slot. Five confirmed copies is a
  **retention ceiling**, not five duplicated initial uploads. Tests fill and
  rotate all five slots across multiple changed states and UTC days, including
  failure and restart cases.
- Provisioning and the isolated drill do not activate the scheduled production
  job. The original timer/script remains active until a reviewed full-main
  release installs the new unit and dedicated configuration. Legacy copies are
  retained, so legacy growth continues until that activation.
