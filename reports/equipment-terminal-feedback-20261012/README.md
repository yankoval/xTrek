# Equipment terminal feedback — review evidence, 2026-10-12

Base: `0124f9815c243dd0fe4e4490f75e3b30ed802ba3` (`main`, PR #112).
Refetched immediately before PR preparation; main was unchanged.
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

## Verification

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

Production remains at the user-reported full main `0124f9815c243dd0fe4e4490f75e3b30ed802ba3`;
its running processes were not inspected or changed in this task. No merge/release,
source replay, business POST, guard reset, protected-task recovery or chat message
was performed. Exact-head approval and separate full-main release/activation approval
are still required. Preserve all current state on rollback; an old binary cannot
safely resume consumers with new terminal fences/persistence-only retry messages.
