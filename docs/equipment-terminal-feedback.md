# Terminal equipment-report feedback

This is the combined successor to PRs #109, #110 and #111, initially based on main
`0124f9815c243dd0fe4e4490f75e3b30ed802ba3` (PR #112), now including
`834ff1b38fab930251c25ca4df40b1e4181d45e9` (PR #114) by an ordinary merge. Review and merge the combined
PR once; do not also merge the three original branches. They remain open for
review provenance. All three changes applied cleanly; the combined baseline
passed 1025 tests before feedback changes.

## Business boundary and event identity

`terminal_feedback.py` classifies failures without importing MAX or performing
network requests. Definite, single-document `CHECKED_NOT_OK`, invalid/empty
report input, business validation/authorization stops and existing reconciliation
exceptions are terminal. A status plus an API `error`, ambiguous multi-document
response, IN_PROGRESS, eventual 404, network/S3/registry failure remains retryable.
After the configured **200 business retries** (201 total attempts), an unresolved
failure becomes `retry_exhausted` and requires reconciliation. An unconfirmed
virtual-task prerequisite is retryable, not a cached successful skip.

Supported sources are the exact folders in `terminal_feedback.STAGES`: equipment
report checks and downstream CIS, aggregation/SET, disaggregation, reaggregation,
utilisation and introduction routes. This does not change emission or order intake.

When activated, the router records `RECONCILIATION_REQUIRED` in the dedicated
terminal ledger **before failing the Celery delivery**. Identity is SHA-256 of
`[equipment-terminal-v1, bucket, complete physical S3 key]`, including any event
namespace. It excludes Celery task IDs, attempts, error text, ETags and configuration
versions. The first terminal classification wins. Another event for the same key
cannot reenter the business route, even after a worker restart. Separate document
keys are distinct terminal events; one report can legitimately have several
independently rejected documents.

The terminal ledger is separate from both operation guards and the S3 publication
outbox. Existing completed guards stay authoritative. Failure never marks an S3
outbox event completed or republishes it. No code resets guards, rewrites sources,
changes receipts, resends business documents, or discovers/replays historical work.
Protected ID `623c20fa-f90c-4478-aeae-9c6635cedc9f` is excluded from new feedback;
additional protected IDs must be supplied before activation. No recovery is attempted.

If terminal persistence fails, Celery publishes a **persistence-only** retry with
the safe category and original event coordinates. It bypasses business logic and
may outlive the 200-business-retry budget. If the retry cannot be published, reject
with requeue so the original broker delivery survives. A missing/unreadable ledger
blocks business entry. Disk or broker failure cannot be repaired by resetting state.

## Routing, audit and message privacy

`operator` is read from the raw JSON of the exact productionOrder. It is not read
from an equipment report or inferred from another field. Normalization is the
identity function: no trimming, case folding, Unicode normalization or fuzzy
matching. Null, nonstring and whitespace-only values are invalid. Other strings
match by exact Unicode code points, including leading/trailing spaces.

`operator_routes` explicitly maps allowed operators to route names. Two operators
can target distinct chats. `$default` resolves to the explicitly configured
`default_route`; **unknown and empty operators never use it**. A missing mapping,
null default, unsupported transport or invalid route stays `ROUTE_HELD` with no
POST. The first resolved order ID and original operator value are preserved as an
immutable audit snapshot, including when held. Allowlisting that exact value can
release the hold; changing the source to another operator cannot silently reroute it.
Once selected, recipient, bot identity and message body are frozen in the ledger.

For equipment reports the exact filename identifies the productionOrder. Other
receipts must contain `productionOrderId`. Legacy CIS task/receipt payloads lack
that relationship: provide an audited `production_order_overrides` entry from the
exact route key to the exact order ID. There is no suffix guessing or S3 listing.
`production_orders_path` is honored; otherwise use the event namespace's
`productionOrders/` folder. Source failures leave delivery pending and never rerun
business processing.

Messages contain the fixed category, stage and action, a canonical trailing task
UUID when available, the complete SHA-256 productionOrder reference and event key.
An authorized operator resolves the reference through `terminal_events` in the
local ledger (exact ID is retained there). Arbitrary identifier text, operator
values, exception strings, True API payloads, tokens and marking codes never enter
chat messages. The ledger is private audit data (0600 file; 0700 parent directory).
No tokens are stored in it: a route stores only the name of a token environment variable.

## Delivery state machine

The standalone drain reads pending ledger rows; it does not consume or publish
business events. `terminal_feedback_max.py` reuses the existing MAX GET bot identity
and paginated complete-history mechanism, with additional malformed-history checks.
A match must prove the exact text, configured sender, configured chat and message ID.
POST responses alone never confirm delivery.

`ROUTE_PENDING/ROUTE_HELD -> READY -> POST_STARTED -> CONFIRMED`.
SQLite atomically claims READY before making one message POST. Concurrent prefork
workers/drains cannot claim it twice. A successful POST, timeout, HTTP error, crash
or unknown result leaves POST_STARTED until GET proves delivery. Subsequent runs
are GET-only. Even a crash between reservation and POST is held, not auto-retried.
If GET fails before reservation, READY can retry GET safely. Incomplete or malformed
history blocks sending. Exact prior GET evidence can confirm without any POST.

This gives one automated POST attempt and durable reconciliation, not an impossible
promise of guaranteed delivery when the remote outcome is unknowable. Missing GET
proof never authorizes a second POST. An uncertain record must not be deleted or
reset. Drain exit code 2 indicates held, uncertain or retryable work; its stdout
contains aggregate counts only. A successful idle drain prints `{}`.

The ledger supports multiple processes **on one host/local filesystem**, matching
the current deployment. Do not use independent copies across hosts or NFS. A future
multi-host release requires shared transactional storage/CAS first. Never replace a
live ledger with a stale backup: it could erase POST_STARTED and permit duplication.
Adding another transport requires an adapter/drain factory; the business classifier
and router do not need transport-specific changes.

## Review-only migration and activation checklist

No production command below was executed. The JSON and systemd files are examples;
no services, settings or chats were changed. External validation used no production
APIs. From Yandex only S3 reads are implemented by this feature; it uses no Functions,
triggers, YMQ, VM, yc or resolver operations.

Before requesting activation, confirm:

| Value | Required confirmation |
| --- | --- |
| `EXACT_OPERATOR_A` -> `line-a` | Exact productionOrder.operator, chat A ID and bot membership |
| `EXACT_OPERATOR_B` -> `line-b` | A second exact operator, chat B ID and bot membership |
| `EXACT_OPERATOR_SHARED` -> `$default` | Whether this explicit allowlist entry is wanted; default route or null |
| Bot | Bot user ID, token environment variable and CA file for each route |
| Legacy CIS | Exact task/receipt key -> productionOrder ID mappings, independently verified |
| Protection | Complete historical protected-ID list; no historical replays |
| Runtime | One host; persistent 0700 directory; worker/drain write permissions; full GET history coverage |

The placeholders are not live assignments. Existing pallet-delivery chat IDs are
not reused implicitly. Empty/missing operator values require manual reconciliation;
this release does not rewrite productionOrders to invent them.

After explicit approval of the reviewed head and a separate release/activation:

1. Merge only the combined PR. Record the resulting **full main SHA**. Run relevant
   and full tests against that SHA. Back up the complete previous release, settings,
   operation state, broker state, S3 event outbox and existing delivery ledgers.
2. Finish the active task before a future warm restart. Deploy the entire approved
   main revision, not this branch or a partial patch. Compare actual loaded source
   hashes/paths and worker process command with that full release.
3. Provision a dedicated persistent 0700 directory and explicitly initialize a new
   ledger once (O_EXCL refuses to overwrite an existing ledger):
   `python -m xtrek.terminal_feedback_runtime init --ledger /var/lib/xtrek/terminal-feedback/events.sqlite3`.
   This creates schema version 1 in a new file; it does not migrate any old ledger.
4. Copy `examples/terminal-feedback.json`, replace all placeholders with confirmed
   values and set enabled true. Add `terminal_feedback_config` pointing to that file
   in suz_worker_config. Add the directory to the worker sandbox's approved write
   paths if needed. Supply token/CA through the separate private environment file.
   No startup code silently initializes state. Without the config key, feedback is
   inactive; activation is required for durable terminal capture.
5. Review/install the example service/timer against actual approved deployment paths.
   Activate the drain only after explicit permission to post to the selected chats.
   GET-confirm the first approved event. Monitor exit code 2 and held counts; no
   automatic retry of unknown message POSTs. Back up SQLite using its online backup
   API or with all writers stopped, retaining the latest reservation state.

## Rollback

Stop the new drain/timer first; keep its SQLite ledger and all pending/uncertain
rows. While the current full main release remains loaded, leave terminal capture
and read-before-business fencing active. Do not disable capture as a way to replay
held work. Finish active tasks and preserve settings and the newest ledger snapshot.

A binary rollback to main before this feature cannot interpret its new terminal
fences or `terminal_failure` retry argument. Therefore, **do not resume affected
business consumers under the old release**. Keep them stopped while reconciling any
persistence-only messages and terminal fences under separately approved recovery.
Never drain, purge or rewrite the queue as an implicit rollback step. Restore only a
complete previously approved main release, verify actual loaded sources, and retain
all current guards/receipts/outboxes/terminal ledger. Roll forward to a corrected
complete main release is preferred when active terminal events already exist.

## Unlimited aggregation sources with required SSCC

Missing `Quantity`, JSON null, an empty string or a whitespace-only string in an
aggregation production source means unlimited aggregation on the phone. It is not
an error and is not numeric zero. Source normalization preserves the original
field/value, including absence. Explicit zero retains its separate existing meaning.
Source/task classification is independent of scanned-report validation.

The equipment task retains the existing, previously working v1 format:
`reportSchemaVersion: 1`, `plannedPalletCount: 1`, `palletSsccReserve: 0`, and **one
required SSCC in palletNumbers**. The phone needs that SSCC, including the second
version of «Скан». No new `quantityMode`/`notificationOnly` fields are introduced in
the phone JSON. Unlimited Quantity cannot yield a finite pallet count from a box
norm, so this case uses the existing one-pallet assignment. MAX receives the same
JSON-link notification format and its existing idempotent GET-confirmed delivery.

The regular equipment-task allocator owns the SSCC. It requires configured durable
`operation_state_path`. The nested `allocate-pallet-sscc` guard retains the result
before publishing the task JSON; reopening the worker reuses that assignment after
a publication failure. In mTLS mode the existing stable request ID binds allocation
to the operation/source fingerprint. Safe local SSCC retry is now wired to that
existing guard: if the allocator accepted a request but lost its response, the same
request ID returns the same SSCC. Old uncertain guards without the explicit
`local-sscc-v1` marker remain reconciliation-only; no legacy guard is reset. Without
durable state, a new unlimited task is held before allocation instead of risking a
duplicate. Existing task JSON and completed guards stay authoritative.

The production-source PRNSRV adapter skips only its printing/extension-3 path:
no CSV, VDF, print marker or printing allocation is created. The regular equipment
SSCC allocation above still occurs. Protected UUID checks precede the shortcut.
The order router skips emission preparation, and the direct emission preparation
entrypoint is also a no-op for unlimited sources. There is no product-code emission,
utilisation or other business document POST on this source path. Generic printer
jobs with a separate `count` contract are unchanged.

A later actual equipment report must still contain physical boxes and nonempty
code arrays. Missing/empty/malformed report content remains terminal regardless of
Quantity. A valid unlimited source/task itself produces no terminal error message.
No historical objects, sources or completed guards are rewritten.
