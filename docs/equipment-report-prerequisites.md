# Equipment report prerequisites

Before entering the initial-validation route, an equipment-report event checks
saved final aggregation evidence. `aggs/<id>.json` must identify a single
`AGGREGATION_DOCUMENT` with `CHECKED_OK`, match `agg-receipts/<id>.json` by document
and production-order IDs, and have a completed `prepare-aggregation` guard bound
to the current source fingerprint and preparation parameters. Both automatic INN
selection (`inn_override=None`) and an explicit INN matching the successful
document's `senderInn` are checked; the fingerprint is mandatory in either case.
The probe reads the event's exact source object on every replay and does not cache
a new initial-route result. An unchanged completed report therefore skips current
CIS-status validation, business sends, check-tag writes and terminal notification,
even if some goods have subsequently been retired. Missing/unmatched evidence
retains ordinary validation; unreadable evidence retries without notification.
Existing terminal records are not cleared or bypassed by this check.

Without verified final completion, the initial report route revalidates even if
its legacy guard says `complete`. That route is reacquired with CAS using its
existing identity, so concurrent workers still cannot own it together. There is
no guard-key/hash migration or new route namespace. Downstream operations keep
their own unchanged guards: revalidating the route does not repeat an external
send. Only `route-logic_start_equipment_reports` enables `recheck_complete`.

The initial equipment-report route validates its source before selecting a token
or constructing True API/NK clients. For schema v1 or v2 it requires physical
boxes and nonempty arrays of full marking codes in every box. Empty pallets,
malformed structure or JSON, and missing/empty full codes fail with
`EquipmentReportValidationError`.

This exception inherits the existing nonretryable `ReconciliationRequired` class.
The task fails without starting utilisation, introduction or aggregation. It does
not invent codes from Quantity, mark the report finished, or cache a successful
skip. The local event ledger records failure and retains event deduplication.

Unreadable/missing S3 input, unavailable registry/token selection and other
unverified prerequisites raise retryable `EquipmentReportResourceError`. The
analyzer also validates the snapshot it actually uses and propagates input-read
and check-tag write failures. There is no business POST after a failed precheck.
Existing valid-report analysis and downstream flow remain in place, including
the existing handling of explicit True API HTTP errors and business status checks.

The change does not add a new PO/INN resolver, change token policy, refresh tokens,
rewrite source data, replace SSCCs, republish events or print labels. Completed
initial-route guards alone are not proof of final report completion; guards for
all other routes and external operations retain their cached-success semantics.

## Release and historical reports

Release only the complete approved `main`, preserving configuration, receipts,
guards, durable event/broker ledgers and rollback. Finish active work before a warm
restart. Do not purge or reset the queue.

A historical empty report requires actual scanned source data and a separately
reviewed recovery plan. A requested quantity is not evidence of marking codes.
Existing complete guards must not be cleared merely because new validation would
reject their original input.
