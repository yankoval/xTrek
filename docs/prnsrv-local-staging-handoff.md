# Local PRNSRV generation and staging handoff

This patch prepares the full-main owner to replace only the `prnsrv-generator`
Object Storage event path on worker94. It does not install or activate a worker,
change the printer runtime, invoke Cloud Functions, or print anything.

## Source, allocator, and durable identity

- Source: `s3://1bf11148-3595-4a07-a089-d460153b7c7a/Задания/<UUID>.json`.
- Count rule follows the pinned `prnsrv` package: `count` takes precedence over
  `Quantity`; empty/invalid/non-positive values complete as `NO_PRINT` without
  allocation.
- Local allocator: HTTPS mTLS endpoint already configured on worker94; prefix
  must equal the verified Fito prefix `460705179`, extension is fixed to `3`.
  Cloud extension `0` is never selected. SSCCs assigned to equipment tasks are
  kept separate: PRNSRV allocations use the operation business key
  `prnsrv:<UUID>` and a distinct stable request ID derived from UUID and
  canonical source hash.
- The adapter refuses the protected uncertain UUID
  `623c20fa-f90c-4478-aeae-9c6635cedc9f` and requires a per-UUID/hash history
  reconciliation callback before it can make the first allocation. This must
  verify legacy `_prnsrv/done`, both output objects, and available previous
  allocator evidence; missing or conflicting evidence blocks allocation.
- Existing local `OperationState` CAS persists the input hash before the local
  allocator call. Only this adapter's own `local-sscc-v1` request may be retried;
  it retries with the same allocator request ID, so a lost response returns the
  same SSCC range. An uncertain cloud extension-0 event is never resumed as an
  extension-3 retry.

## Immutable output and state contract

The engine is the pinned `prnsrv` package at
`698600cd571f5f9adf8b21e86f6582e210d9b3ce` (see
`requirements-prnsrv-local.txt`). This adapter owns lifecycle and persistence:

| Purpose | S3 key |
|---|---|
| CSV | `printer-tasks/<UUID>.csv` |
| VDF | `printer-tasks/<route>_<UUID>.vdf` |
| completion | `_prnsrv/done/<UUID>.done` |
| durable CAS | `<operation_state_path>/allocate-pallet-sscc/<sha256("prnsrv:<UUID>")>.json` |

CSV uploads before VDF. Both use create-only `upload_once`, then read-back
SHA-256 verification. The immutable done marker is published last and records
UUID, source hash, extension-3 namespace, output keys, and hashes. A repeated
event with a valid marker returns the existing result. A changed source hash,
unknown marker, conflicting output, or legacy output without the local guard
raises a reconciliation error; it never overwrites the artifact or allocates a
replacement. If the worker dies after the allocator commits but before its
reply, local idempotency repeats the same request ID. If it dies after saving
the accepted SSCC list, the adapter rebuilds identical outputs from that list.

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

Production delivery remains blocked until the full-main owner implements and
reviews this receiver against the correct non-hot filesystem and confirms its
ownership/permissions on 94. The staging spec is concrete; the staging program
is intentionally not installed or run here.

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

The isolated patch does not run against production credentials. The integration
owner must connect existing report/task identity checks and the pallet PNG
renderer, provision the ledger outside watched folders, and verify that the
current full-main deployment maps to bot `259171708` and chat
`-72448994566325`. The 11 October GET audit found no exact pallet-label file
among 56 assigned equipment SSCCs. Two report objects exist, but their report
and task contents must be compared before generating labels. Other
report/order notifications are not delivery evidence.

The transport follows the official MAX `/uploads`, `/messages`, `/me`, and GET
history API. This patch has not sent an outbound message.

## Integration points for the full-main owner

1. Adapt the S3 poller to enqueue exact `Задания/<UUID>.json` events into this
   adapter after its existing source-task processing is complete. Keep PRNSRV
   CSV/VDF generation a separate output stage.
2. Supply the pinned engine's `classify_count`, route selection, and byte
   builder through the adapter's engine interface. Supply an allocator wrapper
   around `get_sscc_from_service(..., extension=3, auth_mode="mtls",
   request_id=...)` and the configured 94 local service URL.
3. Supply the read-only per-UUID historical reconciliation result before the
   first call. Do not infer “never allocated” from a missing done marker alone.
4. Validate allowed prefixes and manufacturer route. The current local
   extension-3 namespace is Fito-only; the 9 GlobalBio tasks have no approved
   local prefix/signer route in the saved worker configuration and must remain
   blocked until that configuration is authorized and verified.
5. Add a receiver service that stages but never prints, and separately records
   the receipt contract above. Retain the existing distinct S3 task routes.
6. Run the isolated adapter tests and the project regression suite in the
   release checkout; inspect full-main diff and runtime config before release.
7. Wire the MAX sender to validated report/task content and the established
   pallet PNG renderer; retain GET-only reconciliation for ambiguous sends.

## Current 11 October recovery state

- 65/65 positive-Quantity source tasks have no PRNSRV done marker or CSV/VDF.
- Existing local extension-3 allocator and complete-main source are on worker94;
  verified release snapshot lists 56 Fito equipment allocations and a
  next-serial watermark of 56. This does not mean the PRNSRV allocations are
  present.
- This patch has not allocated numbers, written S3, delivered to 94 staging,
  or printed. The release owner must first reconcile historical evidence and
  mark exact `(UUID, source_hash)` rows eligible.
