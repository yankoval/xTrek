# Local PRNSRV generation and staging handoff

The xTrek Celery S3 poller now has a minimal opt-in hook for the existing
`PrnsrvFunction`, backed by the pinned `prnsrv` engine. The hook is disabled
unless `prnsrv_local_generator_enabled: true` is set in runtime config. No
service was installed or activated and no generator event, allocation, S3
write, or print ran while preparing this patch.

## Source, allocator, and durable identity

- Source: `s3://1bf11148-3595-4a07-a089-d460153b7c7a/Задания/<UUID>.json`.
- Count rule follows the pinned `prnsrv` package: `count` takes precedence over
  `Quantity`; empty/invalid/non-positive values complete as `NO_PRINT` without
  allocation.
- Source events arrive through the existing `xtrek.s3_poller` route for
  `Задания/<UUID>.json` and continue through `tasks.process_s3_event`. The
  guarded source branch calls
  `xtrek.prnsrv_local_runtime.process_local_prnsrv_s3_event(...)` before the
  existing xTrek order workflow when the runtime flag is enabled.
- `LocalPrnsrvFunction` reuses the existing `PrnsrvFunction` from the
  `cf-aggregate-ingress-pr12/prnsrv-generator` adapter at source commit
  `50b8264cf107fe6b53f530338ce5e939527db3a2`; count classification, route,
  template/mapping reads, CSV/VDF builders, output keys, immutable S3 writes,
  read-back hashes, and done markers stay with that function and pinned engine.
- `LocalPrnsrvAllocator` injects the existing function's allocator protocol and
  sends `extension=3` through the worker's HTTPS mTLS local endpoint. Its
  request ID is stable from UUID + canonical source hash; the local allocator
  ledger returns the same result after a lost response. No cloud `SSCCClient`,
  extension 0, or SUZ/business signing token is used.
- `LocalPrnsrvFunction` refuses the protected uncertain UUID
  `623c20fa-f90c-4478-aeae-9c6635cedc9f`. For a UUID without a done marker, it
  also requires an exact entry in `prnsrv_local_history_eligible` mapping UUID
  to canonical source hash. A prior generated marker must prove the same local
  extension-3 namespace; older or ambiguous evidence blocks the attempt.

## Immutable output and state contract

The engine is the pinned `prnsrv` package at
`698600cd571f5f9adf8b21e86f6582e210d9b3ce` (see `requirements.txt`).
`PrnsrvFunction` owns lifecycle and persistence:

| Purpose | S3 key |
|---|---|
| CSV | `printer-tasks/<UUID>.csv` |
| VDF | `printer-tasks/<route>_<UUID>.vdf` |
| completion | `_prnsrv/done/<UUID>.done` |

CSV uploads before VDF using conditional immutable writes. Both are read back
and SHA-256 verified; the done marker is published last. The local wrapper adds
`allocation.namespace=<prefix>/extension-3` and the stable request ID to the
existing generated marker. Duplicate events reuse the marker. Conflicting
outputs or old generated markers without local extension-3 proof are held for
reconciliation.

The `prnsrv-generator` source contract targets `printer-tasks/` and
`_prnsrv/done/`; it does not target `solmarkTasks/`, `equipment-tasks/`,
`equipment-reports/`, or the equipment task's pallet assignment.

## Receiver staging boundary

The 11 October runtime audit shows that neither active Windows downloader
consumes `printer-tasks/`: 10.0.5.2 reads `equipment-tasks/`; 10.0.5.3 reads
`solmarkTasks/`. `Downloaded` tags on the latter do not provide a PRNSRV receipt.
There is no PRNSRV receiver on worker94 and no installed PRN/Amica service.

For review and recovery, use an independent non-hot staging receiver:

1. Poll only `printer-tasks/<UUID>.csv` and
   `printer-tasks/<route>_<UUID>.vdf` for an allowlisted row whose done marker
   and hashes validate.
2. Copy to `/var/lib/xtrek/prnsrv/staging/<UUID>/` on worker94. Write files via
   temp + fsync + atomic rename; write a manifest last with source hash, UUID,
   S3 key, file SHA-256, and arrival time. Reject overwrite with different
   bytes.
3. Acknowledge S3 with a separate `printer-task-stage-receipts/<UUID>.json`
   create-only record only after both staged hashes match. Do not set or
   interpret `Downloaded` as printed.
4. Do not copy to `C:\\tmp`, VDFPR, PrintServer, a printer share, or any watched
   folder. There is no physical-print action in the receiver contract.

The S3 verification/staging/receipt code is implemented in
`xtrek/prnsrv_staging.py`; it is a separate, explicitly invoked step and is not
on the generator's critical path. It has no print operation. The review test
uses paired CSV/VDF fixtures, verifies both S3 GET bodies against the done
marker SHA-256 values, reads both staged files back, verifies the manifest,
and checks the create-only S3 receipt and its readback.

The receiver entrypoint is
`PrnsrvStagingReceiver.from_xtrek_config(bucket=..., s3_config=...,
staging_root=..., allowlisted_uuids=...).stage(uuid)`. Required arguments are
the source bucket, its existing S3 client configuration, an explicit staging
root, and an explicit UUID allowlist. `output_prefix`, `done_prefix`, and
`receipt_prefix` default to `printer-tasks/`, `_prnsrv/done/`, and
`printer-task-stage-receipts/`.
It returns `STAGED` only after it has: (1) found an immutable
`_prnsrv/done/<UUID>.done` marker with the same UUID and `GENERATED` result;
(2) verified both S3 output objects against their marker SHA-256 values and
expected filenames; (3) atomically staged and read back both files; (4) written
`manifest.json` last with the source hash, source/output keys, hashes, and
arrival timestamp; and (5) created/read back the immutable
`printer-task-stage-receipts/<UUID>.json` receipt. Different existing bytes,
missing metadata, stale lock, or non-allowlisted UUID blocks staging. The
receiver has no print, share, or watched-folder operation.

## MAX pallet-label handoff

`xtrek/pallet_label_max_notifier.py` provides a local, explicitly invoked MAX
sender for already validated pallet-label PNG bytes. It checks `/me`, reads
complete chat history before upload, and confirms delivery through GET using
sender, chat, report UUID, SSCC, and a file attachment. Since MAX GET does not
guarantee the original upload filename is returned, the sender includes the
filename in message text too. The SQLite ledger is explicitly initialized and
durable across restarts. After an ambiguous message POST, the next invocation
performs GET reconciliation only and never automatically repeats the POST.
Before enabling it, import any uncertain rows from the prior delivery ledger.
The recovery ledger currently has two such rows in chat `-72448994566325`:
`T-7913-77-002-05.2029-C-01-01-54f16b25-57bc-4629-8231-e6b269fb576d` /
`346070517900000057` / `pallet_1_346070517900000057.png`, and
`T-7427-77-001-10.2029-C-01-06-d30c3bd7-2fe3-4f04-ad67-392ffb9db52f` /
`346070517900000248` / `pallet_1_346070517900000248.png`. Call
`block_legacy_attempt` with each exact triplet. A matching GET can promote them
to confirmed, otherwise they remain blocked.

The isolated patch does not run against production credentials. The runtime
owner must provision the ledger outside watched folders and verify that the
current full-main deployment maps to bot `259171708` and chat
`-72448994566325`. The recovery snapshot recorded no exact pallet-label file
among 56 assigned equipment SSCCs before delivery. Two MAX POSTs were later
confirmed by GET and are now recorded as delivered in the ledger. Other
report/order notifications are not delivery evidence.

The transport follows the official MAX `/uploads`, `/messages`, `/me`, and GET
history API. This patch has not sent an outbound message.

The exact full local event entrypoint is
`process_equipment_report_event(report_storage=..., task_storage=...,
report_key="equipment-reports/<report-id>.json", notifier=...)` from
`xtrek/pallet_label_pipeline.py`. It reads the report and its corresponding
`equipment-tasks/<report-id>.json`, checks report key/id/task id/schema version,
assigned SSCCs and check digits, closed pallet/box state, product counts, then
renders GS1-128 AI 00 PNGs and calls the idempotent notifier. It uses separate
storage instances so report and task buckets can differ. Rendering requires
`Pillow`, `python-barcode==0.16.1`, and a Cyrillic-capable font path supplied as
`XTREK_LABEL_FONT` or `font_path`.

## Runtime release handoff

The generator, local extension-3 allocator adapter, S3 staging receiver, and
MAX report-to-label pipeline are implemented in this checkout. The generator
entrypoint is `tasks.process_s3_event` →
`process_local_prnsrv_s3_event(bucket, key, runtime_config, event_id=None)`;
it runs only when `prnsrv_local_generator_enabled` is true. Configure
`s3_config`, `prnsrv_local_allocator_by_inn[INN]` with `url`, `prefix`,
`client_cert`, `client_key`, optional `ca_bundle` and `timeout_seconds`, plus
`prnsrv_local_history_eligible[UUID] = canonical_source_hash`. The environment
variable `XTREK_LOCAL_ALLOWED_INNS` must contain the enabled INNs. Optional
keys are `prnsrv_input_prefix`, `prnsrv_output_prefix`, `prnsrv_done_prefix`,
`prnsrv_templates_bucket`, `prnsrv_templates_prefix`, `prnsrv_mapping_key`,
`prnsrv_column_name`, and `prnsrv_vdf_source_dir`. No runtime configuration or
service was changed here.

1. Set `prnsrv_local_allocator_by_inn` and `XTREK_LOCAL_ALLOWED_INNS` for each
   explicitly enabled organization, including the worker94 endpoint and mTLS
   cert/key paths. GlobalBio delegation identity and power of attorney are
   confirmed; the Smart/Global self-test passed. The remaining release action
   is runtime allowlist/prefix/endpoint configuration and verification, not
   signer discovery. Generator allocation does not sign SUZ business documents.
2. Populate `prnsrv_local_history_eligible` only with UUID/source-hash pairs
   whose S3 and prior allocator history are reconciled. Do not infer eligibility
   from a missing done marker.
3. Enable the non-hot receiver only after the owner confirms
   `/var/lib/xtrek/prnsrv/staging` ownership and permissions on worker94.
4. The MAX event entrypoint is
   `process_equipment_report_event(report_storage, task_storage, report_key,
   notifier)`; it joins and validates report/task payloads, renders pallet PNGs,
   then calls the idempotent sender. The caller supplies the two bucket-specific
   storage instances and notifier credentials/configuration.
5. Targeted verification in this checkout: 74 tests passed across
   `test_prnsrv_local.py`, `test_pallet_label_max_notifier.py`,
   `test_pallet_label_pipeline.py`, and `test_tasks_unit_set_flow.py`. Full
   project suite: 925 passed, 10 skipped, 14 subtests passed. The release
   owner must still inspect the release diff and runtime configuration before
   activation.

## Current 11 October recovery state

- 65/65 positive-Quantity source tasks have no PRNSRV done marker or CSV/VDF.
- Existing local extension-3 allocator and complete-main source are on worker94;
  verified release snapshot lists 56 Fito equipment allocations and a
  next-serial watermark of 56. This does not mean the PRNSRV allocations are
  present.
- This patch has not allocated numbers, written S3, delivered to 94 staging,
  or printed. The generation hook remains disabled by default.
