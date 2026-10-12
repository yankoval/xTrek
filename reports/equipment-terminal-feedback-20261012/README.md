# Equipment terminal feedback — review evidence, 2026-10-12

Initial base: `0124f9815c243dd0fe4e4490f75e3b30ed802ba3` (PR #112).
Current integrated main: `52f0b1ee81759aa17b67485eeda0c38cb32fe7f6` (PR #115).
Branch: `codex/equipment-terminal-feedback`.

## Consolidation

| Original PR | Reviewed source head | Integration |
| --- | --- | --- |
| [#109](https://github.com/yankoval/xTrek/pull/109) | `be86425a83fb2fce8a9e5069063ec8bc039fdcdc` | CIS terminal classification retained; shared typed classification added |
| [#110](https://github.com/yankoval/xTrek/pull/110) | `f8a95be2483c9c8fd09d4b7a09aef41185fb42a4` | Normal/SET rejection retained; shared reconciliation boundary added |
| [#111](https://github.com/yankoval/xTrek/pull/111) | `9d3341ee75caf1bb239d7f5118cf1120cb233346` | Structural fail-closed validation retained; terminal feedback added |

All three cherry-picks applied cleanly on current main. One combined PR should be
reviewed/merged once. The originals remain open and should not also be merged.
No changes were pushed to their branches.

## Latest correction: unlimited aggregation WITH required SSCC

Human correction supersedes the unfinished no-SSCC interpretation. That variant
was never committed, pushed or activated. Missing/null/empty/whitespace Quantity is
preserved as unlimited, not rewritten to zero. Equipment task JSON uses the existing
v1 schema and contains one required pallet SSCC. The regular task allocator and
its durable operation guard own that SSCC; the independent printing path is skipped.
No new phone-task schema flags or changes to MAX JSON-link delivery were introduced.

- **27 regression cases** cover unlimited source -> equipment JSON with SSCC ->
  GET-confirmed MAX link, no terminal feedback, restart/repeat idempotency, emission
  rejection without side effects, explicit-zero distinction and unchanged malformed-report validation.
- Failure after allocation before publication reuses the saved SSCC with no second
  allocator call. Lost allocator reply retries the same mTLS request ID; the service
  returns the same issued SSCC. Old unmarked uncertain guards stay held.
- No product-code emission, utilisation, CSV/VDF/printing or extra business POSTs:
  only the required idempotent SSCC request and normal MAX notification are allowed
  by test doubles. All live HTTP is blocked; no production calls were made.
- Current main `834ff1b38fab930251c25ca4df40b1e4181d45e9` (PR #114) included by normal
  merge `c23fff0`; no history rewrite/force push. Its new XlsxWriter dependency was
  installed as version 3.2.9 into an isolated temporary test dependency directory.
- Latest focused suite: **309 passed**. [Log](unlimited-focused-tests.txt).
- Latest full suite: **1144 passed, 10 skipped, 14 subtests**.
  [Log and skip reasons](unlimited-full-tests.txt).
- The full command uses the previous interpreter with PYTHONPATH pointing to the
  isolated XlsxWriter directory and this complete worktree. The same ten opt-in
  Linux CSP certificate tests remain skipped.

## PR #115 integration verification

- Preserve PR #115 source routing and INN filter behavior, including original source
  Quantity checks before emission and one SSCC for unlimited tasks.
- Share the unlimited predicate; direct emission rejects an unlimited order without
  writing an artifact or making a business POST.
- Correct test isolation: keep the real Kombu module for real Celery regression tests,
  and inject the router storage in the source-to-MAX integration test.
- Focused conflict regression: **127 passed**.
- Full suite: **1157 passed, 10 skipped, 14 subtests passed**.
  [Full log](main115-full-tests.txt).
- The user authorized merge and full-main production release on 2026-10-12.
  Production release is sequenced after the creation chat's PR #115 release/recovery.

## Previous revision verification (c5f64d2)

- Combined baseline before new feature: **1025 passed, 10 skipped, 14 subtests**.
  [Baseline log](baseline-tests.txt).
- New terminal feedback tests: **81 passed**. [Feature log](feedback-tests.txt).
- Final focused suite: **228 passed**. [Focused log](relevant-tests.txt).
- Final full suite: **1106 passed, 10 skipped, 14 subtests**.
  [Full log and skip reasons](full-tests.txt).
- `git diff --check` passed. Local Python/pytest/Celery versions and SHA-256 hashes
  of changed runtime sources/configuration/tests: [manifest](source-manifest.json).

Commands used the existing local `.venv-sscc-check` interpreter and the new complete
worktree as working directory. Full command: `python -m pytest -q -rs`.
Focused command: `python -m pytest -q tests/test_terminal_feedback.py tests/test_equipment_report_prerequisites.py tests/test_aggregation_terminal_status.py tests/test_tasks_unit_set_flow.py tests/test_s3_poller.py tests/test_pallet_label_max_notifier.py`.
Ten opt-in signing integration tests require a Linux CSP certificate and were not
run. No live MAX delivery or production S3 read/write was performed; transport and
storage failure paths use mocks and local test ledgers. GitHub was used for repository
read/push/PR operations only. No Yandex administrative services were contacted.

## Covered failure boundaries

Every new category; pending/404/network/S3/registry without terminal events; retry
budget exhaustion; real Celery durable persistence retries beyond the business
budget; broker publish failure requeue; restart fence; concurrent POST claim;
POST timeout/HTTP error/accepted but not visible/crash before POST; exact GET sender,
chat, body and mid; malformed/incomplete GET history; two operators, explicit default,
unknown/empty/case/space/lookalike mismatch; frozen operator audit/destination;
secret/code exclusion from outbound messages; exact CIS mapping; explicit receipt
productionOrderId and namespace; completed historical guards; existing S3 outbox
stays published, never completed or republished by terminal feedback.

Three prior tests expected successful returns on stopped invalid/forbidden reports.
They now require a typed nonretryable failure; this prevents cached false success.
Definitive introduction/disaggregation/reaggregation rejections use the same boundary.
Unconfirmed virtual task creation remains retryable.

## Activation and rollback

See [design and runbook](../../docs/equipment-terminal-feedback.md) and
[disabled configuration example](../../examples/terminal-feedback.json).
The operator/chat confirmation table is in the runbook. No actual assignments are
assumed from other MAX features. Legacy CIS relationships must be confirmed exactly.
The example service/timer is not installed or enabled.

Production activation is tracked separately from this source verification. No
production mutation was performed to prepare this integration. The example routing
configuration remains disabled until actual operator/chat destinations are known.
Preserve all current state on rollback; an old binary cannot safely resume consumers
with new terminal fences/persistence-only retry messages.
