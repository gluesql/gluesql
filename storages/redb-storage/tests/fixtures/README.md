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

## Version directories

`v1/` — storage format v1 (no `__GLUESQL_META__` table; legacy row serialisation).

`v2/` — storage format v2 (`__GLUESQL_META__` version=2; redb file format v2; `(Key, Vec<Value>)` row serialisation).

## Fixture provenance

`v1/mixed_schema_schemaless.redb` was generated from commit `266c214d` in a detached
`git worktree`, using `gluesql-redb-storage` with SQL statements listed in the sidecar.

SHA-256:

`984f4d5e77da49914e1a8eb5e5f666c9b6253a671747878749da264a6e956415`

---

`v2/mixed_schema_schemaless.redb` was generated from commit `2545eccb` using raw
`redb` + `bincode` (no GlueSQL storage API) as documented in the sidecar.

SHA-256:

`60ce479d89d16d1f0d39a0ce75835ede12b9e99dad78bd5b1e4cb41b5fe0c17b`
