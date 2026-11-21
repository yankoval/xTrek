# S3 cost contracts

`python -m pytest tests/cost -q` is a blocking step of the existing `Tests / test`
GitHub Actions job. It runs on PRs and workflow pushes without path filters or
`continue-on-error`. The rest of the suite still runs after cost failures. JUnit
results include the measured call counts and byte estimates as properties and
are uploaded as `s3-cost-results`, including on failure.

## Workloads and budgets

The same production code is exercised with 1,000 and 10,000 historical entries.
Warm-up is excluded from the measurement. The measured ordinary cycle has no new
business inputs; the poller harness subsequently injects a new report and an
overwrite of an old key to reject implementations that simply stop discovery.

| Consumer | Entry point | History |
| --- | --- | --- |
| Pallet reports | `pallet_label_report_poller.main` | Old reports plus one initial fresh report |
| Discovery | `s3_poller.main --role scan` | One routed report prefix; real paging, cursor and outbox |
| MAX notifier | `scan_and_deliver` with real `TaskNotifier` | Persisted successful deliveries and unchanged S3 objects |
| SSCC backup | `BackupStore.run` | Valid allocator DB with 1,000/10,000 allocations and five verified slots |

An ordinary warmed idle cycle allows at most **1 LIST, 6 HEAD, 1 small GET,
0 PUT, 1 DELETE, 1 versioning check and 64 KiB of estimated response data**.
The small GET allowance permits a compact manifest/control object; it does not
permit downloading a large historical snapshot. The HEAD allowance permits
checking the five fixed backup slots and candidate by metadata. It does not
permit HEAD per historical task. A control/versioning check and cleanup of an
absent temporary candidate are accounted for but are not object downloads.

For history ×10, operation counts and response bytes may grow at most ×2.
There is a fixed floor of one request per method and 4 KiB for small control
responses. Both the growth and absolute idle contracts must pass; a fixed but
large page cap is not sufficient.

These are proposed engineering acceptance budgets, not Yandex tariffs or a
reproduction of the observed bill. At a 30-second cadence, 64 KiB/cycle is at
most 180 MiB/day per process (before separate reconciliation/verification work).
A complete cost estimate must add process count, retries, startup, periodic full
reconciliation and planned integrity checks. A full reconciliation needs its own
schedule and budget; do not hide it by classifying every short tick as full work.

## Measurement boundaries

The fake implements real pagination (up to 1,000 objects/page), computes the
serialized XML metadata response size, counts attempted operations including
missing-object GETs, and counts returned object bytes. HEAD uses a conservative
fixed 512-byte estimate. It does not model all HTTP/TLS overhead, retries within
an SDK or exact provider XML; use runtime byte metrics for production billing.
No object-size sum is substituted for LIST traffic. All IDs and data are
synthetic; no credentials, cloud API, broker or message delivery are used.

External transport boundaries are replaced, not the production scan, notifier,
backup verification or ledger logic. The network is blocked in fixtures.
Existing functional, corruption, concurrency and recovery tests remain required.

## Existing violations

The base `main` at `f2237b0f348ed2115f9aa7607a7c19788484e591` has known violations.
This change deliberately exposes them rather than declaring them acceptable:

- Pallet polling downloads every historical LIST page on every loop.
- Scanner has a page cap but spends it again on a warmed idle cycle.
- Notifier lists historical tasks and calls HEAD before checking delivered state.
- Backup downloads every retained snapshot before deciding it is unchanged.

Do not add `skip`/`xfail`, ignore the step exit status, or increase limits to
match these failures. Keep the PR in draft until agreed implementation fixes
satisfy the contracts without losing new events, old-key overwrites, backup
integrity or replay protection. Changing a discovery cadence also needs an
explicit latency/SLO decision; making a cycle cheap by silently delaying work
is not a sufficient fix.

## Merge enforcement

A failing step fails the `test` job. To make that failure a server-enforced merge
barrier, repository branch protection or a ruleset must require the `test`
status for `main`. Workflow YAML alone cannot prevent an administrator from
merging a red PR. Repository settings are separate from this code change and
must be checked explicitly when rolling out the gate.
