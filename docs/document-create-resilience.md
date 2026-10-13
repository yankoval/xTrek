# Document create resilience

All six guarded document send operations share one CAS control object per HTTPS
endpoint under `operation_state_path/_document_create/v1/`. This uses the same
conditional storage primitive as operation guards and must be identical across
94, Jade, homepc2 and Cloud.ru. Concurrency remains one per existing worker.

Three 5xx/transport failures within 60 seconds open admission for 30 seconds.
After that, exactly one **new** guarded operation may probe. Failed probes double
the cooldown (60/120/240/300 seconds, capped at 300). The probe reservation expires
after 120 seconds if its process disappears. A successful non-5xx probe closes
the route; its own operation guard still handles rejection/uncertainty. Older
inflight successes cannot close an open route. At most six CAS attempts occur;
unreadable/corrupt/contended control state defers before POST. Normal closed state
admits distinct operations from all four workers; this is not a global concurrency
limit. Previously admitted requests can finish after the circuit opens.

An operation records `document_attempt_count`, `http_attempt`, `last_error` and
`next_retry_at`. Pre-POST deferral leaves it ready and preserves its retry deadline
through redelivery/restart. Existing Celery bounded retry scheduling remains; a
worker may wake before the deadline but cannot POST before admission. Clock
synchronization is required.

Every admitted send performs exactly one requests POST, with redirects disabled.
5xx, transport failures and non-429 rejection/redirect responses raise a typed
ReconciliationRequired error. An external/uncertain guard never becomes ready due
to route recovery. No automatic business replay, guard reset or inferred absence
is added. Existing explicit 429 rejection behavior is preserved. Accepted/complete
result recovery retains its existing behavior. A control/checkpoint failure after
HTTP cannot be converted into a pre-POST deferral or discard a valid HTTP result.
A crash after admission is handled by the existing authoritative operation guard.

Structured telemetry includes safe endpoint, operation/key hash, attempt, HTTP
status, elapsed time, whitelisted failure category and only UUID correlation
headers that do not echo the request body or authorization value. It excludes
response bodies, tokens, codes, signatures and business payloads. Raw aggregation
wrapper DEBUG logging is removed. TLS verification policy is unchanged here.

## Verification and release

The focused suite covers four processes racing for one half-open probe, bounded
backoff, CAS contention/corruption, old successes, duplicate callbacks, durable
uncertainty, duplicate delivery/ACK loss, transport failure and secret redaction.
The existing full suite covers workflows and storage primitives with mocks; these
are not live production API or S3 tests.

Release requires a merged full main, successful CI, preserved runtime configs,
source verification and rollback. Drain current work before restarting and update
all four ordinary workers plus the dedicated pallet worker together. Mixed old/new
workers do not enforce this shared gate consistently. The root is auto-initialized
on the first admitted new operation, without migration or guard/queue rewriting.
Rollback keeps the added metadata/control object, but old code does not honor the
route backoff. Any manual replay of an uncertain operation requires independent
read-only reconciliation and separate approval. This PR does not merge, release,
restart services or change production state.
