# redb fixture files

This directory stores fixed input databases for migration integration tests.
A fixture describes one storage version's state. The target version and expected
result belong to the test that migrates that state.

## Pairing rule

Each `.redb` file must be paired with a `.sql` sidecar that documents:

- how the fixture was generated
- the input storage version, schema and row representation
- the stored data and its meaning in the original writer

The sidecar is a reference, not an executable test script. It must distinguish
SQL executed by the writer from ASTs inserted through StoreMut. Migration
success, refusal, report counts and target SQL assertions belong in:

- `../migration_v1_to_v4_fixtures.rs`
- `../migration_v2_to_v4_fixtures.rs`
- `../migration_v3_to_v4_fixtures.rs`

The corresponding files without `_fixtures` construct database scenarios for
batching, mixed layouts, rollback and retry. They may use a fixture as setup;
their assertions describe the constructed scenario. Conversion branches and
decoder limits are unit-tested beside their implementation, without opening
these databases. `../migration/database.rs` contains only file/raw-table I/O.

Example pair:

- `v1/mixed_schema_schemaless.redb`
- `v1/mixed_schema_schemaless.sql`

## Version directories

`v1/` — storage format v1 (no `__GLUESQL_META__` table; legacy row serialisation).

`v2/` — storage format v2 (`__GLUESQL_META__` version=2; redb file format v2; `(Key, Vec<Value>)` row serialisation).

`v3/` — storage format v3 (redb file format v3). Historical and current schema layouts may coexist under this version.

## Release schema writers

The schema fixtures use the GlueSQL v0.19.0 and v0.20.0 release source APIs.
They do not patch current enum tags or add layouts from intermediate development
commits. Release source identifiers, generator modes, dependency versions,
source contents, and SHA-256 are recorded in the SQL sidecars. These are release
source fixtures, not verified published `.crate` artifacts.

| Fixtures | Release source | Layout/scenario |
| --- | --- | --- |
| `v1/` | v0.19.0 | v1 rows and schemas, including old projection and execution metadata |
| `v3/current_*` | v0.20.0 | Schemas written directly by the released v3 storage |
| `v3/upgraded_v1_*` | v0.19.0 -> v0.20.0 | Actual released migration; v1 schema bytes and upgraded rows |
| `v2/defaults`, `v2/typed_default` | Above release-upgrade fixtures | Constructed v2 route fixtures with the same raw schemas/rows, metadata 2 and file format 2 |

The v2 route fixtures exercise the retained migration entry point. They do not
represent an additional released schema layout. `v1 -> v2` changed the row
envelope and retained the v1 schema bytes.

To reproduce, extract the release source with `git archive`, and add a standalone
`fixture-writer` directory to the checkout. Copy [generate.rs](generate.rs) to
`fixture-writer/src/main.rs`. Its manifest is:

```toml
[package]
name = "redb-fixture-writer"
version = "0.1.0"
edition = "2024"

[workspace]

[features]
legacy-async = []

[dependencies]
gluesql-core = { path = "../core" }
gluesql-redb-storage = { path = "../storages/redb-storage" }
futures = "0.3"
redb = "=2.6.0"
bincode = "=1.3.3"
```

Copy the source checkout's `Cargo.lock` into the writer directory before its first
build. Resolve once, confirm the sidecar dependency versions, then use `--locked`.
Generation used Rust 1.94.1, bincode 1.3.3 and the historical core's serde features.
v0.19.0 needs `--features legacy-async`; v0.20.0 is synchronous. For `upgrade`
mode, first copy the specified v1 fixture to the output path; the writer then
runs the actual v0.20.0 migration. `v2` mode takes a release-upgrade fixture as
its third argument and copies its raw schemas/rows into a new v2 file. Other
modes start with a nonexistent output file. `hash-join` and `index-plan` require
the v0.19.0 source and `--features legacy-async`.
The writer compacts the final file without calling `Database::upgrade()` outside
that historical migration.

`index-payload` prints the v0.19.0 schema bytes used by the inline
`migration::v2_to_v4::index_expression` unit test, without creating a DB.
For example, run the v0.19.0 writer with
`--features legacy-async -- /tmp/unused.redb index-payload`. The expected Schema
is declared independently in that unit. There is no index-expression DB fixture.

The ambiguity pair contains identical full schema bytes from different real
writers. The old TEXT defaults insert `true`; the v0.20 Decimal AST inserts `false`.
The ambiguity integration test compares the actual schema bytes and stored
results before asserting refusal. Hash-join and index-plan AST fixtures use the
public StoreMut API; their sidecars describe the stored plan fields.

The retained 12 inputs have distinct roles: four default-schema states, four
typed-default/ambiguity states, two schema/schemaless states, one complex Query
and one unrecoverable historical plan. The plain Query and its upgraded copy
are covered by the representative hash-join input plus direct conversion units.

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
