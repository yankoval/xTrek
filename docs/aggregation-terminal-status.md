# Terminal aggregation status

Both `aggReceipts/` and `aggSetReceipts/` poll the document identified by the
existing receipt. A single `CHECKED_NOT_OK` response is a terminal business
rejection. `AggregationDocumentRejected` inherits `ReconciliationRequired`, which
the existing Celery router already excludes from automatic retries.

The status updater preserves the response and its status tag. Rejection does not
finalize the equipment report, generate or send another aggregation, change
packaging codes, replace a receipt, or reset its send guard. The task fails and
the local event ledger records `failed`. The published event remains deduplicated
across ledger restarts; it is not marked completed or republished.

Pending, unknown, malformed, multiple-document and transport responses retain the
status retry policy. Only a single `CHECKED_OK` response without an API error
continues the existing flow: normal aggregation checks the equipment report;
SET aggregation starts its existing transport aggregation flow.

Exceptions do not include the API response or marking codes. An operator must
reconcile a rejection before any separately approved corrective business action.
The terminal status does not itself authorize replacement labels, new SSCCs,
disaggregation or resubmission.

## Release

Deploy the complete approved `main` revision, preserving the configuration,
original receipts, status evidence, guards and durable queue/event ledgers.
Finish the active task before a warm restart; do not purge or reset the queue.
An already scheduled poll may read its original rejected document once more and
then fail without another retry. Verify that failure and the unchanged receipt
and send-guard identity. Existing successful reports remain protected.
