# S3 discovery and independent outbox delivery

A failed input-task GET previously aborted `scan()` before `publish_pending()`.
Known tasks were also downloaded before outbox deduplication. The scanner and
publisher now run in **separate processes**, sharing the existing local SQLite
file. No business routes, operation-state guards, cutoff or S3 retention rules
are changed. No S3 objects are removed.

## Processes and configuration

Install both `deploy/systemd/xtrek-s3-poller.service` (discovery) and
`deploy/systemd/xtrek-s3-publisher.service` (delivery). The publisher is not
coupled to scanner or worker service health; RabbitMQ outages cause durable
publication retries. The scanner does not import Celery or worker code. The
publisher creates a minimal Celery producer without importing `xtrek.tasks`,
signing configuration, token loaders, or boto3. The worker still receives
`tasks.process_s3_event`, `{bucket, key}`, and the original stable task ID.

The CLI requires an explicit role; the old combined invocation fails closed:

```sh
python -m xtrek.s3_poller --role scan
python -m xtrek.s3_poller --role publish
```

`--once` runs one cycle of the selected role. A scan cycle may complete degraded;
use its metrics, not exit code alone, for health. A storage failure exits nonzero.
Do not use `--once` against production as a test: the publisher sends real events.

Keep the existing environment files and values. Additional optional settings:

| Variable | Default | Meaning |
|---|---:|---|
| `XTREK_S3_PUBLISH_SECONDS` | 2 | Pause after a publisher batch; minimum 1 second |
| `XTREK_S3_MAX_GETS` | 100 | Maximum GET calls per discovery cycle |
| `XTREK_S3_RETRY_LIMIT` | 20 | Maximum due object retries per cycle, additionally limited to a quarter of GET capacity (minimum one slot) |
| `XTREK_S3_CYCLE_SECONDS` | 30 | Soft discovery-cycle time budget |
| `XTREK_S3_POLL_SECONDS` | 20 | Existing pause **after** discovery, minimum 5 seconds |

The LIST budget remains 20 page slots, with persistent per-prefix cursors and
round-robin scheduling. Retries use at most a quarter of the time target before
fresh discovery. One discovery page slot is guaranteed even if an in-flight
retry overruns that target. API calls have 5-second connect and 10-second read
timeouts, SDK retry attempts are set to one, and durable retries own backoff.
The time budget is soft: in-flight network/body reads and SQLite commits are not
cancelled. A page already listed must finish durable bookkeeping for all its
records before its cursor advances. GETs that do not fit the budget become
pending discovery records without being downloaded. There is no transaction
held across S3 or RabbitMQ I/O.

Both roles require `XTREK_LOCAL_S3_POLLING=1` and the same canonical
`XTREK_S3_POLL_LEDGER`. Scanner additionally requires the existing explicit UTC
cutoff, signer INN allowlist and static S3 credentials. Publisher only needs the
existing local broker URL and queue name; it can operate with no S3 credentials.
Use a dedicated loopback AMQP user and non-default vhost, a durable queue and
persistent messages. Publisher confirms are enabled; hidden Celery publish
retries are disabled. The confirm and write timeouts are 10 seconds.

Each role holds a nonblocking `flock` beside the resolved database path for its
lifetime. `scan()` and `publish_pending()` also take those locks when called
outside the CLI. A second instance of the same role is rejected. Scanner and
publisher use different lock files and separate SQLite connections, WAL and
`synchronous=FULL`. **Never unlink lock files** or open the database through
hard-link aliases; use one local filesystem on one host, not a shared/NFS DB.
A killed process releases its lock; existing publication leases survive restart.
The publisher cannot steal its own unexpired lease after a crash (120 seconds).

## Discovery safety

Identity remains SHA-256 of bucket/key/ETag/version/size/LastModified. Tags do
not change it. A primary-key lookup in `s3_outbox` precedes JSON GET. Every known
state, including pending/published/completed, skips GET. New identities still
pass the existing signer-INN policy, including the equipment-only blank-Quantity
exception. The historical backfill allowlist does not bypass INN validation.

Input GET uses `IfMatch` (and VersionId when available). Returned ETag, size and
LastModified must match the listed identity before its bytes can authorize that
identity. A detected overwrite is retained as a `replaced` retry record; regular
LIST discovers and validates the replacement independently. The business task
envelope still contains bucket/key, not immutable version bytes: the existing
router and operation guards remain responsible for subsequent business reads.
This is not a versioned snapshot or an exactly-once external POST guarantee.

`discovery_retry` stores original metadata/identity, a safe reason category,
attempt count, next retry time, creation time and state. It is committed **before**
the corresponding page checkpoint. Repeated LISTs cannot bypass pending backoff.
Enqueue commits before resolving retry, so a crash between them is deduplicated
on recovery. Completed retry records are retained; no cleanup policy is added.

* Transient/network failures: exponential delay from 5 to 300 seconds.
* Auth/permission, invalid JSON/schema, and missing objects: 300 to 3600 seconds.
* Delays have bounded jitter (80–100% of the capped value).
* Missing objects remain pending for later recovery, not silently successful.
* A changed identity is recorded as replaced, with no further GET of that identity.
* Budget deferral does not increment attempts; an actual failed attempt does.

`discovery_prefix_retry` independently backs off failed LISTs and preserves their
continuation tokens while allowing other sources to advance. Credential
`ExpiredToken`/`InvalidToken` and ambiguous `InvalidArgument` never clear cursors.
Only an explicit `InvalidContinuationToken`, or `InvalidArgument` naming the
continuation-token argument, resets that prefix. An ambiguous stale token needs
operator review; it is intentionally not guessed from arbitrary error messages.

If a retry write, enqueue or checkpoint fails, the current page does not advance.
SQLite transactions roll back failed writes. Already committed discoveries and
retries remain usable. The failed-cycle log survives a health persistence error.

## Delivery and duplicate effects

Only successful `send_task` return with AMQP publisher confirms permits
`published_at`. A broker-accepted message with a lost confirm stays pending and
may be sent again with the same task ID. Worker completion racing the confirm
cannot be downgraded from `completed` to `published`.

Stable Celery IDs do **not** deduplicate tasks. Existing business operation guards
and their accepted/uncertain/complete states protect external effects; task
attempts provide evidence. An ambiguous external result still requires the
existing reconciliation procedure. No operation ledger is reset or bypassed.

## Measurements and health

Every scanner cycle, including partial/failed cycles, writes a structured log
and a latest-cycle snapshot in `poller_health`. Publisher batches do the same.
Snapshots are overwritten per role, rather than growing an unbounded metrics
history. They are not proof of health after process death: check `updated_at`.

| Metric | Interpretation |
|---|---|
| `list_calls`, `get_calls` | Actual SDK API invocations, including failures |
| `list_sdk_attempts`, `get_sdk_attempts` | HTTP send attempts counted by botocore hooks; null for clients without hooks |
| `records` | LIST records returned, including records later filtered out |
| `known_skips` | Registered identities skipped before GET |
| `new_events` | Newly committed outbox records, not S3 requests |
| `object_retries`, `object_errors`, `errors_*` | Attempted discovery retries, errors, safe classification |
| `retry_skips`, `deferred`, `inn_denied` | Backoff-owned identities, budget deferrals, rejected signer policy |
| `list_errors`, `prefix_skips`, `prefix_backoffs` | Failed LISTs and throttled sources |
| `checkpoints` | Successfully committed **listed pages**; rotating past failed/backed-off prefixes does not count |
| `duration_seconds` | Elapsed cycle/batch time |
| `pending`, `oldest_pending_age_seconds` | Pending delivery backlog, including publication leases/backoff |
| `retry_pending` | Unresolved object-discovery backlog |
| `last_successful_list` | Last successful LIST observed by scanner, retained across clean cycle/restart |
| `publish_attempts`, `published`, `last_confirmed_publish` | Delivery attempts, confirmed DB acknowledgements, last confirmed timestamp |
| `status` | `ok`, `degraded` (discovery backlog/errors), or `failed` |

A failed SQLite write can prevent a fresh health snapshot. Logs plus stale
`updated_at` must then signal failure. SIGKILL cannot produce a final-cycle log;
committed cursors/retries remain the recovery evidence. Counters are per cycle,
not cumulative; exported monitoring should retain the log series. Logs contain
no keys, source JSON, credentials, token values, signatures or marking codes.

Alert on increasing oldest pending age (for example >5 minutes), stale role
snapshots, persistent discovery retry/auth errors, or no checkpoints over the
expected traversal window. An old confirmation time alone is normal when pending
is zero. `active/running` and empty RabbitMQ alone do not demonstrate progress.
Use these measurements before changing source frequencies; receipt prioritization
beyond existing round robin is deferred to a separate change.

A local synthetic 581-known-object benchmark is recorded in
[s3-poller-validation.md](s3-poller-validation.md). It measures local overhead,
not S3 latency or production VPS memory.

## Release, migration, and rollback

This PR prepares a release; it does not deploy it. After merge, deploy the **full
current main** using the existing release procedure, not selected files or this
branch. Record the exact main SHA, run the full suite, verify the loaded source
paths/hashes for both roles and workers, preserve working environment/config and
the previous complete release. Finish in-flight work before any worker restart.
Do not configure automatic upgrades.

1. Plan a maintenance window and stop the old combined poller. Wait for it to
   finish/exit; do not run old and new discovery/publishers together. Old code
   does not honor the new role locks.
2. Create a consistent backup of **current** SQLite state with SQLite's backup
   API or `.backup` (never copy only the live main file while WAL is active).
   Include outbox, task attempts, cursors and all existing operation/SSCC state
   through their normal backup procedure. Preserve config and old unit files.
3. With the verified full release installed, first start the new publisher, then
   scanner using the two updated units. Keep the same ledger path and ownership.
   Startup performs additive `CREATE TABLE/INDEX IF NOT EXISTS`; original table
   layouts/rows, attempts, leases and cursors are unchanged. No replay/reset or
   cutoff changes are required. Reserve disk headroom for WAL and new indexes.
4. Check both role snapshots and journal status. Watch existing pending drain,
   confirmed publication timestamps, advancing checkpoints and retry reasons.
   Review any auth/missing/invalid-JSON backlog separately. Do not inject real
   business events for validation or repeat successful documents.
5. Keep normal backups and monitor state growth. Do not delete S3 source objects,
   retry history or operation records as part of this release.

**Rollback gate:** the old scanner does not understand `discovery_retry`.
Before downgrading, stop discovery, allow pending retry discovery to complete
with the new scanner as needed, then stop both roles and inspect the frozen DB:

```sql
SELECT COUNT(*) FROM discovery_retry WHERE state='pending';
```

This must be zero. With pending retries, a direct downgrade is **not a verified
lossless rollback**: keep the stopped state/backup and fix forward with a full
approved main release. Do not erase retries or rewind/reset cursors to force the
gate. Availability may be reduced until that repair is ready.

Once the gate is zero, return to the previous full release and its original
combined-poller unit; disable the new publisher unit before starting old code.
Keep the **current database**, including the additive tables/indexes and original
cursors/task attempts/leases. Old SQL is compatible with those additions. Do not
restore the pre-release database over newer progress: that can repeat delivery.
The tested rollback compatibility covers old SQL on the migrated/current DB and
reopening it with new code. It does not prove production broker/CSP behavior.
