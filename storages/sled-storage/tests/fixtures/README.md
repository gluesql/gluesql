# sled fixture files

This directory stores binary sled fixture trees for migration tests.

## Pairing rule

Each `.sled/` fixture directory must be paired with a `.sql` sidecar that documents:

- how the fixture was generated
- which schema/schemaless scenario it contains
- representative SQL queries and expected outcomes after migration

Example pair:

- `v1/mixed_schema_schemaless.sled/`
- `v1/mixed_schema_schemaless.sql`

## Version directory

`v1/` means the fixture was created with storage format v1
(no `__GLUESQL_STORAGE_FORMAT_VERSION__` key).

## Fixture provenance

`v1/mixed_schema_schemaless.sled/` was generated from commit `266c214d` in a detached
`git worktree`, using `gluesql_sled_storage` with SQL statements listed in the sidecar.

SHA-256:

- `conf`: `916d2ae94a3f9440b0db932b60a323e5980da640ec4fb2d07d5cbd74df0497f2`
- `db`: `851c176c3186ada73dee3ade729c1723a27e787a88f332aefc0cb084074daa03`
- `blobs/`: empty directory
