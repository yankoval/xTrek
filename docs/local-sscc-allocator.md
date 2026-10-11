# Local SSCC allocator (extension 3)

The service is intended to run on the 94 worker and bind only to its Tailscale
address. It requires HTTPS with a client certificate issued by the configured
client CA; the certificate subject must also appear in the allowlist. The
worker client verifies the server CA and presents its certificate. There is no
HTTP or Yandex IAM fallback when `sscc_auth_mode` is `mtls`.

The ledger uses SQLite rollback journaling (`DELETE`) and `synchronous=EXTRA`.
Counter advancement and the exact response are committed in one immediate
transaction. Repeating a request ID with identical parameters returns the
stored response; changing the count or namespace for that ID is rejected.
Request IDs derive from the guarded workflow name, business key, and source
fingerprint. The existing operation record is preserved. A Cloud request in
`external` or `uncertain` state has no local retry marker and remains blocked;
only this ledger's own pre-request marker permits an idempotent local retry.

## One-time initialization

Install the complete `main` release first. Provision the database directory
with owner-only permissions and initialize exactly once, after checking that
the prefix/extension namespace is historically unused and the Cloud allocator
has a deployed server-side block for extension 3:

```sh
python -m xtrek.sscc_local --init-ledger /var/lib/xtrek-sscc/ledger.sqlite3 PREFIX
```

The normal service startup never creates a database, schema, or counter. It
fails closed if the file, schema, namespace metadata, or counter row is absent
or invalid. Do not initialize again after loss or corruption. Restoring a
backup requires independent confirmation that its counter is at least as high
as every allocation already committed; an old backup must not be started as a
live ledger. Keep an off-host backup and its verified counter watermark before
activation. This repository does not yet automate that independent watermark.

## Runtime environment

`XTREK_SSCC_BIND` must be the 94 Tailscale address, `XTREK_SSCC_PORT` is 9443,
and `XTREK_SSCC_DB` names the initialized ledger. Set the server certificate
and private key, client CA, and semicolon-separated exact allowed certificate
subjects through a root-owned mode-0600 environment file. Never commit those
values. The worker's root-owned configuration uses `sscc_auth_mode: "mtls"`
and `https://<94-tailscale-address>:9443/v1/allocate`; `XTREK_SSCC_CA`,
`XTREK_SSCC_CLIENT_CERT`, and `XTREK_SSCC_CLIENT_KEY` point to its trust and
client identity files.

Before activation, verify the certificate identity from each intended worker,
the source and effective systemd unit, service binding, database backup and
restore procedure, the Cloud production deployment and its 409 response for
extension 3. Do not resume historical uncertain operation records or enable a
second allocator during rollback.
