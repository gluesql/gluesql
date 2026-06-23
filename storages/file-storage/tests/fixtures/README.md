# file-storage fixture files

This directory stores filesystem fixtures for migration tests.

## Directory layout

Fixtures are grouped by migration step and case name:

- `v1_to_v2/<case>/actual/` (input tree before migration)
- `v1_to_v2/<case>/expected/` (expected tree after migration)
- `v1_to_v2/<case>.sql` (human-readable scenario description)

Example:

- `v1_to_v2/mixed_schema_schemaless/actual/`
- `v1_to_v2/mixed_schema_schemaless/expected/`
- `v1_to_v2/mixed_schema_schemaless.sql`

`actual/` contains v1 file-storage layout:

- schema files without format header
- rows in legacy v1 shape (`row: Vec(...)` / `row: Map(...)`)

`expected/` contains the post-migration v2 tree expected from `migrate_to_latest`.
