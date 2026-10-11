# CIS_INFORMATION_CHANGE status handling

The receipt route polls the existing document recorded in
`cisInformationChangeReceipts/`. It never sends a replacement document.

| Status response | Celery result |
| --- | --- |
| A single document with `CHECKED_OK` | Success |
| A single document with `CHECKED_NOT_OK` | Failure, without automatic retry |
| Pending, unknown, missing or ambiguous status | Existing status retry policy |
| Transport error | Existing status retry policy |

`CHECKED_NOT_OK` is a rejection, even when its errors describe a condition that
an operator could later correct. It does not authorize resubmission. The status
updater preserves the API response and its status tag; the receipt and completed
send guard keep their original document identity. The task raises
`CisInformationChangeRejected`, excluded from Celery's automatic retries.

The local S3 event ledger records task `failed`. It does not mark the event
`completed`, and it does not republish the same event. This is a failed business
operation, not successful processing of the correction. The existing route
guard's exception behavior is unchanged.

Unknown statuses remain retryable until their meaning is established. Responses
containing multiple documents cannot confirm success or a terminal rejection.
Status exceptions do not include the API response or marking codes in the log.

## Release and acceptance

Release the complete approved `main` revision. Preserve the configuration,
receipts, status objects, guards and local broker/event ledgers. Finish the active
task before a warm worker restart; do not purge the queue or reset business state.

An already scheduled receipt task can poll its original document again after the
release. A rejection then fails without scheduling another retry. Confirm the
failure in the worker journal and local event ledger, and confirm the original
receipt and send-guard document ID. Do not resend the correction, mark it
successful, or change its source to resolve a terminal rejection.
