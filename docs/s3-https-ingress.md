# Temporary HTTPS intake for S3 notifications

The cloud adapter delivers object references to `/xtrek/s3-events` on a TLS proxy.
The loopback receiver canonicalizes current LIST metadata and registers the same
`event_id` in the existing scanner SQLite outbox. The existing publisher remains
the only AMQP publisher. Neither component performs business operations.

There is one function and one Object Storage trigger per business bucket. Yandex
triggers have a single bucket ID. Use bucket-wide triggers with `.json` suffix;
filter business prefixes in the adapter and receiver. Pilot configuration admits
only `equipment-reports/` in the internal bucket. Input events and other internal
prefixes remain covered by the unchanged scanner until explicit route expansion.
Keep legacy business/PRNSRV triggers paused; do not resume them for this intake.

## Receiver

Deploy a complete tested release from main. Install `xtrek-s3-ingress.service` and
include `deploy/nginx/s3-ingress.location.conf` inside an existing TLS server.
The service binds only to `127.0.0.1:18981`; never expose the Python server directly.
Keep the scanner's current poll interval and the existing publisher/worker PIDs.
A separately pinned complete main release may run the receiver while existing
workers continue their previously deployed complete main release. Set its working
directory/PYTHONPATH explicitly rather than overlaying individual Python files.

`/etc/xtrek/s3-ingress.env` supplies:

- `XTREK_S3_INGRESS_SECRET_FILE`: root-owned, service-readable secret, >=32 bytes.
- `XTREK_S3_INGRESS_ROUTES_JSON`: explicit bucket-to-prefix-list map.
- `XTREK_S3_ADMISSION_ENV_FILE=/etc/xtrek/local-s3-mode.env`: reload the persistent
  cutoff each request. The service must be able to read this file; do not execute it.
- Optional `XTREK_S3_INGRESS_PORT`, default 18981.

Reuse the existing S3 credentials, bucket configuration, signer allowlist,
`XTREK_S3_POLL_LEDGER` and optional backfill allowlist. The outbox must already exist.
Each request uses its own SQLite connection; registration uses the unique event
ID and commits before acknowledgement. Do not share scanner cursor/metrics state.

Requests contain `{"events":[{"bucket":"...","key":"..."}]}` (1–10 items).
HMAC-SHA256 signs ASCII Unix timestamp + newline + exact request bytes, using
`X-Xtrek-Timestamp` and `X-Xtrek-Signature`. Allowed clock skew is 300 seconds.
TLS certificate verification is mandatory. Repeats within that interval are safe
because durable event registration is idempotent. Secrets must not enter logs.

Successful HTTP 202 means every item was durably registered, already known, or
intentionally excluded by admission policy. Missing metadata, S3/SQLite errors
return failure; partial-batch writes are retained and retries deduplicate them.
LIST must match the exact key, including Unicode, spaces and literal plus signs.
Do not construct IDs from provider notification IDs, notification timestamps or
HEAD metadata with reduced timestamp precision. Overwritten objects are treated
as their latest observable state, matching scanner semantics; no intermediate
version recovery is promised without immutable sources/versioning.

## Cloud adapter and triggers

Package `deploy/cloud/s3-notifier/index.py` as the root `index.py`, entry point
`index.handler`. Standard library only; use a supported Python runtime, 128 MiB,
and execution timeout >=35 seconds. Supply a fixed HTTPS ingress URL, the same
signing secret (prefer a restricted secret store) and the same pilot route map.
No S3 write, signing, SUZ or RabbitMQ credentials are needed by the function.

Configure the two bucket triggers to invoke the same function version/tag with
batch size 1, cutoff 1 second, bounded invocation retries. Observe real create,
overwrite/copy and multipart notification behavior; do not assume that the
provider's `update-object` event means content overwrite rather than ACL changes.
The adapter ignores deletes/unknown event types. A raised delivery exception is
required for trigger retries; merely returning `statusCode: 500` is insufficient.

## Acceptance and rollback

Test both arrival orders, concurrent registration, response loss, partial batches,
S3 failure, unavailable broker, disallowed routes/INNs and live cutoff reload.
Check that existing source IDs/states, schema and service settings remain intact.
For production smoke, invoke the adapter for an existing already-registered object:
expect `known`, no new task. A harmless object outside scanner/business prefixes
can test bucket-wide trigger delivery without making an equipment job; remove it
after observing the function invocation. Observe a normal equipment report to
confirm the active route in addition to transport smoke. Do not rewrite a business
object or create synthetic equipment reports for testing.

Rollback: pause only these new triggers, stop/disable only the ingress service,
restore the proxy configuration after `nginx -t`, then reload the proxy. Retain
outbox rows: accepted events remain owned by the existing publisher. The scanner
continues at its original interval. Remove cloud resources only after checking
in-flight invocation/retry state. This temporary arrangement retains the 94/outbox
single point of failure; it does not implement the future distributed S3 intake.
