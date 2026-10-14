# Poller reliability validation — 2026-10-14

Baseline: full `origin/main` **27421e9c46bc9f93af7b0d3e5e55479bd4a52823**.
A clean archive of that revision was tested separately from the edited checkout.
Environment: macOS arm64, Python 3.14.0; repository CI uses Linux/Python 3.12.
Dependencies were installed from `requirements.txt`, `requirements-test.txt` and
`.[reports]` into an isolated virtualenv.

## Test results

* Clean main: **1239 passed, 10 skipped, 14 subtests passed**.
* Updated full suite: **1289 passed, 10 skipped, 14 subtests passed**.
* The 10 skips are opt-in Linux CSP/certificate integration tests. No real signing,
  business API mutation, RabbitMQ event injection or production restart was used.
* An initial attempt using an existing environment failed one baseline test due
  to missing `xlsxwriter`; the complete isolated environment resolved that gap.
* `git diff --check` passed. GitHub CI is tracked on the draft PR separately.

Command: `python -m pytest tests/ -q -rs`.

The new tests exercise real temporary SQLite databases, process death via
`os._exit`, separate scanner/publisher processes, and fake S3/broker transports:

| Scenario | Evidence |
|---|---|
| 581 known input identities | Exactly one LIST, zero GET, no outbox insert or retry update for known records |
| New/changed input and backfill | INN policy still checked; disallowed signer never enqueued |
| GET 500 and total LIST outage | Other records proceed; committed outbox publishes; retries/backoff survive reopen |
| Scanner blocked in LIST | A separate process delivers existing pending while scanner remains blocked |
| Access/auth, malformed JSON/schema, missing object | Separate safe categories, persistent attempts and bounded backoff |
| LIST milliseconds vs GET HTTP-date | Real botocore parser admits unchanged input, retains the pre-fix identity, skips GET after restart, and recovers persisted fractional retry metadata; signer rejection and true metadata differences remain enforced |
| Overwrite between LIST/GET | Conditional GET or returned metadata mismatch prevents admission under old identity |
| Slow retry and request exhaustion | Discovery gets a page slot; unvisited GET candidates are durable before checkpoint |
| SQL checkpoint failure | Cursor transaction rolls back; committed retry survives |
| Actual SQLite `SQLITE_FULL` | Retry cannot persist, so page does not advance and health reports failure |
| Crash after retry, checkpoint or enqueue | Restart recovers durable pending/identity without lost event |
| Crash after broker acceptance | Lease and task identity survive; later retry is possible |
| Lost confirm and duplicate delivery | Same task ID redelivers; real `OperationState` guards cache emission/signing/SSCC effects in the fake workflow |
| Fast worker completion vs confirm | `completed` is preserved |
| SDK API/wire counters | Fake HTTP transport observes the same LIST/GET counts, including failure |
| Additive schema and rollback gate | Legacy rows/cursors/attempts preserved, old SQL works after pending retries resolve, upgrade can reopen |
| Publisher role startup | Works with no S3 credentials, cutoff, signer config or worker import |

The full suite also retains existing concurrent business-workflow/operation-guard
and uncertain external-result tests. Fake-broker tests do not establish exactly
once delivery or exactly once external POST. Real RabbitMQ/systemd/CSP behavior
must be checked during a separately authorized release.

## P1 precision regression correction

Before the fix, new SDK-parser regression cases failed for both admission and
signer rejection: LIST `2026-01-01T00:00:00.870Z` and GET
`Thu, 01 Jan 2026 00:00:00 GMT` caused a false terminal `replaced` record.
After the fix, eight new cases pass. They retain the exact pre-fix SHA-256 event
identity, exercise enqueue/skip after restart and durable retry recovery, reject
changed second/ETag/size, preserve INN validation, and compare nonzero GET
fractions exactly. The targeted poller suite is **59 passed**. No production
state or event identities were rewritten.

## Offline memory and known-page benchmark

Reproduce with a clean main archive directory:

```sh
python tools/benchmark_s3_poller.py --baseline-dir /path/to/clean-main
```

This creates temporary ledgers with 581 known identities and takes the median of
20 one-page scans. GET bodies are in memory; no network requests are made.
The scanner initializes a boto3 client with fake explicit credentials; publisher
initializes its minimal Celery app without connecting. Peak RSS uses platform-
appropriate units. Example local run:

| Bootstrap/workload | Peak RSS, MiB | Median scan, ms | GET per page |
|---|---:|---:|---:|
| Baseline poller module + SDK, **without worker import** | 65.6 | 5.49 | 581 |
| New scanner + SDK | 64.5 | 3.55 | 0 |
| New minimal publisher | 38.9 | — | — |

These numbers are not total memory of the old production process: its import of
`xtrek.tasks` and business dependencies is deliberately excluded to avoid loading
live configuration. The publisher measurement is a useful local overhead
reference, not an estimate of the exact increase on the Linux VPS. Summing RSS
also double counts shared pages. Measure PSS/RSS and service memory on that host
at authorized rollout. Recheck after opening a broker connection and under load.

The timing comparison measures only local Python/SQLite work; it omits the
network cost of all 581 baseline GETs. It demonstrates the request reduction,
not a prediction of end-to-end production latency. New due/confirmation indexes
avoid scanning completed history for every publisher health check. No S3 or
SQLite-history retention policy was changed.
