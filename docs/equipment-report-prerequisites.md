# Equipment report prerequisites

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
rewrite source data, replace SSCCs, reset guards, republish events or print labels.
Already completed route guards remain authoritative; this change does not recover
historical successful-skip results automatically.

## Release and historical reports

Release only the complete approved `main`, preserving configuration, receipts,
guards, durable event/broker ledgers and rollback. Finish active work before a warm
restart. Do not purge or reset the queue.

A historical empty report requires actual scanned source data and a separately
reviewed recovery plan. A requested quantity is not evidence of marking codes.
Existing complete guards must not be cleared merely because new validation would
reject their original input.
