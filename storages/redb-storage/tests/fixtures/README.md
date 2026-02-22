# redb fixture files

This directory stores binary test fixtures for migration tests.

## Pairing rule

Each `.redb` file must be paired with a `.sql` sidecar that documents:

- how the fixture was generated
- which schema/schemaless scenario it contains
- representative SQL queries and expected outcomes after migration

Example pair:

- `v1/mixed_schema_schemaless.redb`
- `v1/mixed_schema_schemaless.sql`

## Version directory

`v1/` means the fixture was created with storage format v1 (no storage format metadata table).

## Fixture provenance

`v1/mixed_schema_schemaless.redb` was generated from commit `266c214d` in a detached
`git worktree`, using `gluesql-redb-storage` with SQL statements listed in the sidecar.

SHA-256:

`984f4d5e77da49914e1a8eb5e5f666c9b6253a671747878749da264a6e956415`
