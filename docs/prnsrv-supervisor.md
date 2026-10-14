# Managed PRNSRV supervisor

The CLI is part of the full xTrek main release: `python -m xtrek.prnsrv_supervisor`.
It keeps the existing state directory, cutoff, baseline, protected UUID, local
extension-3 allocator, generator and non-hot staging receiver. It does not publish
to a printer, emit marking codes, change Celery routes or reset business guards.

Only LIST/GET/history reads BEFORE ADMITTED are retried (DNS/connection/read
failure and HTTP408/429/500/502/503/504). Five attempts use exponential2/4/8/16s
with bounded jitter and a120s deadline; an in-flight SDK request may take up to
its existing connect5s/read15s timeout to return after that deadline. Returned
late/partial scan results cannot admit jobs. SDK retries are disabled on this
supervisor client so retry policies do not multiply.

After exhaustion, the process stays DEGRADED and closes new admission. Cooldown
increases30/60/120/240/300s with bounded jitter and cap300. Complete read recovery
resumes new jobs. Any read failure after admission/allocator/generation is NOT
retried: the job becomes BLOCKED. State persistence happens before business
entry. ADMITTED left by a crash is quarantined at every new scan/startup; the
supervisor never generates/allocates it again. STAGED/SKIPPED/BLOCKED records
are skipped unchanged. Manual reconciliation is required for an orphan;
accepted immutable outputs can later be resumed through a separately approved
stage/receipt action without generation or allocation.

`health.json` is separate from business state. It records main/pid/boot/owner,
phase, heartbeat, last complete scan, last completed cycle/progress, counts,
retry ETA and safe error metadata. Idle successful scans update liveness; last
successful timestamps are retained across process restart. Error/event JSONL
uses fsync and10MiB×5 rotation. Errors retain class, HTTP status and stack
module/function/line; text, source lines, locals, headers, response bodies,
codes and signatures are excluded. Disk/checkpoint failures close admission.
Logs are not a guarantee of physical printing.

SIGTERM sets a flag: the current admitted job finishes, then no new job begins.
The supplied static unit keeps the former environment and adds Restart=on-failure,
RestartSec30, StartLimit5/600s and RestartPreventExitStatus78 for fatal identity,
owner or configuration failures. TimeoutStopSec=infinity; no KILL is needed.
Autostart and automatic code updates are not added.

The S3 poller entrypoint resets INFO after imports, suppresses SDK DEBUG headers/
bodies and makes idle/per-source allowlist chatter DEBUG. New discovered/published
counts remain INFO. Discovery, ledger identity, publishing and business routing
semantics are unchanged.

## Tests and release

Fault tests use fake storage/clock/allocator: DNS, timeout,503/429, exhausted
reads, partial/late pagination, auth failure, crash after allocation, restart,
SIGTERM, failed checkpoint, existing history and secret redaction. No synthetic
production allocation/POST is required.

Release only a merged full main after CI/full tests. Preserve config, state,
cutoff/baseline/jobs, source/unit/ledger backups and rollback. Reach an idle window
with no ADMITTED work before changing the launcher; update state.main only to the
new full SHA. Install the versioned unit and validate it, without enabling it or
changing Windows15-minute timeouts/schedules/monitoring. Verify source/config/
newPID, heartbeat from a complete idle S3 scan, ledger ranges and old jobs.

Rollback requires a safe stop and old full source/manifest/unit/launcher. Change
only state.main for the restored SHA and retain CURRENT jobs/allocations/receipts;
do not restore an old business state/ledger over accepted new effects. The old
launcher does not have the new read retry or crash quarantine protections.
