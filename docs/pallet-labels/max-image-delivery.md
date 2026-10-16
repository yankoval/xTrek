# Pallet label image delivery

New pallet labels use the same MAX image contract as the former cloud sender:
`POST /uploads?type=image`, multipart `data` with PNG bytes, the single token
from the upload response's `photos` map, then an `image` message attachment.
Upload errors (including error JSON in HTTP 200) never start a message POST.
Only an explicit `attachment.not.ready` response permits a bounded message retry;
unknown POST outcomes remain subject to GET reconciliation.

Both the caption and PNG include **Номера коробов** for that particular pallet.
The formatter validates box SSCCs, removes AI 00 where present, extracts the
seven-digit serial, preserves leading zeros and merges consecutive serials.
Different SSCC namespaces are rejected rather than merged into misleading ranges.
The caption also names the actual source report and equipment-task JSON files.
No product marking codes are included in the caption. Long label text wraps at
measured font widths. Labels exceeding MAX's 7680-pixel image dimensions fail
validation rather than clipping ranges or shrinking text until it is unreadable.
Captions exceeding MAX's 4000-character limit are also rejected before any upload
or message POST, with no silent removal of box ranges.

## Existing deliveries and cached PNGs

- Existing `CONFIRMED` records remain confirmed, with their original PNG hash.
  They are not resent merely to replace a file attachment with an image.
- Exact historical file attachments prevent duplicate sends and return
  `LEGACY_FILE_CONFIRMED_BY_GET`; new sends require an image in the confirming GET.
- For pending deliveries with an earlier renderer hash, the runtime reuses the
  original cached PNG after verifying its hash and source identity. It sends
  that original image with the expanded caption. It never overwrites that cache.
- A missing or mismatched legacy PNG fails closed for reconciliation. Ambiguous
  message POST records never authorize an automatic second message.
- `FILE_UPLOADED` remains a compatible ledger state name, including image uploads;
  existing database rows and schemas do not need migration.

## Release verification

Before deployment, inspect the still-configured cloud notification trigger and
confirm which sender owns each report route. Do not enable a second sender or
silently disable the cloud trigger. Deploy a complete tested main release only
with separate authorization, retaining configuration, delivery ledger and rollback.
Verify an eligible new natural delivery by GET (`type=image`, exact recipient,
report, pallet SSCC, filename proof) and check both caption and image ranges.
Historical messages are outside the rollout scope.
