# Organization fixtures

Files in this directory are test data only. Release and deployment tooling must
not copy them into `xtrek/my_orgs`, because that directory is loaded by the
runtime token worker.

`org123.json` intentionally represents an organization without a production
certificate and is used to keep that scenario available for tests.
