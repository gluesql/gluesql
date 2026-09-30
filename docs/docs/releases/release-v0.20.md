---
title: Release v0.20.0
description: Release notes for GlueSQL v0.20.0
---

# GlueSQL v0.20.0

GlueSQL v0.20.0 makes the Rust core synchronous, introduces `StatementPlan` and a typed query execution pipeline, and adds a clearer upgrade path for persistent storage. It also brings struct-based inserts, PostgreSQL-style regular-expression operators, and important correctness and performance improvements.

This release contains breaking API and storage-format changes. Read the upgrade notes before updating an existing application or database.

## ⚠️ Upgrade Notes

### Upgrade persistent storage before opening it

The schemaless row representation now uses the same `Vec<Value>` model as schemaful tables. Schemaless rewrites happen during planning instead of execution, removing the former `DataRow` wrapper and unifying the storage interface ([#1865](https://github.com/gluesql/gluesql/pull/1865)).

Existing persistent data may require a one-time migration:

| Storage | Upgrade action |
| --- | --- |
| FileStorage | Upgrade format v1 to v2 with `--upgrade` or `gluesql_file_storage::migrate_to_latest` |
| RedbStorage | Upgrade format v1 or v2 to v3 with `--upgrade` or `gluesql_redb_storage::migrate_to_latest` |
| SledStorage | Upgrade format v1 to v2 with `--upgrade` or `gluesql_sled_storage::migrate_to_latest` |
| RedisStorage | Reset the GlueSQL namespace before reuse because the stored row payload changed |
| CSV, JSON, MongoDB, and Parquet | No persisted-data migration is required |

Back up persistent data before upgrading. The CLI can migrate supported storage types:

```shell
gluesql --storage file --path ./data --upgrade
gluesql --storage redb --path ./data.redb --upgrade
gluesql --storage sled --path ./data --upgrade
```

RedbStorage now creates databases using redb file format v3 ([#1901](https://github.com/gluesql/gluesql/pull/1901)). The migration keeps GlueSQL's existing row serialization while upgrading the underlying redb file. Older GlueSQL releases cannot reopen the upgraded v3 file, so keep the backup if rollback may be necessary.

### SledStorage is deprecated

`SledStorage` is deprecated in v0.20.0 and is planned for removal in v0.21.0 ([#2013](https://github.com/gluesql/gluesql/pull/2013)). Existing deployments can continue using it during the v0.20 release cycle, but new persistent deployments should use `RedbStorage`.

Before upgrading to v0.21.0, export an existing Sled database as SQL and recreate it with RedbStorage:

```shell
gluesql --path ~/glue_data --dump ./dump.sql
gluesql --execute ./dump.sql --path ~/new_data --storage=redb
```

## 🌊 Breaking Changes

### The Rust core and native storages are synchronous

GlueSQL's core execution model and native storage implementations no longer require async plumbing ([#1928](https://github.com/gluesql/gluesql/pull/1928)). Calls such as `Glue::plan`, `Glue::execute`, and Query Builder's `execute` now return their results directly:

```rust
// v0.19
let payloads = glue.execute("SELECT * FROM User").await?;

// v0.20
let payloads = glue.execute("SELECT * FROM User")?;
```

Custom storage implementations need to update their `Store`, `StoreMut`, and `Planner` implementations:

- Remove `async_trait`, `async fn`, and `.await` from the storage interface.
- Return a standard `Iterator` from `scan_data` instead of a futures `Stream`.
- Use `Vec<Value>` for stored rows instead of `DataRow`.
- Accept and return `StatementPlan` from custom planners.

### JavaScript and Python packages moved to dedicated repositories

The language bindings now evolve outside the Rust workspace:

- JavaScript, WebStorage, and IdbStorage moved to [gluesql/gluesql-js](https://github.com/gluesql/gluesql-js) ([#1926](https://github.com/gluesql/gluesql/pull/1926)).
- Python moved to [gluesql/gluesql-py](https://github.com/gluesql/gluesql-py) ([#1925](https://github.com/gluesql/gluesql/pull/1925)).

This separation keeps browser-specific async requirements out of the synchronous Rust storage contract. JavaScript and Python users should follow releases from their respective repositories.

Before the JavaScript package moved, its npm release workflow adopted Trusted Publishing ([#1870](https://github.com/gluesql/gluesql/pull/1870)) and updated its Node.js and wasm-pack versions ([#1872](https://github.com/gluesql/gluesql/pull/1872)). Further JavaScript release changes now belong in `gluesql-js`.

### `StatementPlan` and storage-specific planners

`StatementPlan` is the new public, execution-facing representation produced by `Glue::plan` and consumed by `Glue::execute_stmt`, custom planners, and Query Builder ([#1900](https://github.com/gluesql/gluesql/pull/1900)). The SQL AST remains a representation of parsed syntax and storage-facing definitions, while `StatementPlan` carries the concrete decisions needed by planning and execution.

This separation moves execution metadata out of the AST. Aggregate expressions, for example, now receive their execution slots during planning instead of being matched dynamically at runtime ([#1895](https://github.com/gluesql/gluesql/pull/1895)). SELECT and VALUES plans then form a typed execution pipeline whose stages can only accept valid preceding stages ([#1937](https://github.com/gluesql/gluesql/pull/1937)).

The existing `Planner` store trait now accepts and returns `StatementPlan`, giving each storage implementation a real execution-plan boundary to customize. The default planner validates the statement and applies schemaless, primary-key, hash-join, and aggregate planning. A storage can override `Planner::plan` to compose the available passes differently or add a storage-specific access strategy. `SledStorage`, for example, adds index planning so eligible queries carry `TableAccessPlan::Index`, while other storages keep a full scan or primary-key access plan.

The SELECT pipeline is represented explicitly as:

```text
Source
  -> Join
  -> Filter
  -> Aggregate
  -> Project
  -> Order By
  -> Distinct
  -> Offset
  -> Limit
```

### AST Builder is now Query Builder

The public `ast_builder` module has been renamed to `query_builder` ([#1933](https://github.com/gluesql/gluesql/pull/1933)). Imports and error names need to be updated, including `AstBuilderError` to `QueryBuilderError`.

Query Builder now produces `StatementPlan` values and mirrors the same typed execution relationships, so invalid stage combinations are rejected by the Rust type system. The join planner has also been named more precisely as the hash-join planner ([#1988](https://github.com/gluesql/gluesql/pull/1988)).

### Other public API changes

- `EvaluateError::FormatParseError` now stores an owned error string instead of exposing `chrono::ParseError` through GlueSQL's public error type ([#1913](https://github.com/gluesql/gluesql/pull/1913)).
- The obsolete `gluesql-utils` crate has been removed after dropping its unused map exports ([#1929](https://github.com/gluesql/gluesql/pull/1929)) and custom `Vector` wrapper ([#1931](https://github.com/gluesql/gluesql/pull/1931)). `Tribool` is now available from `gluesql_core::data::Tribool` ([#1932](https://github.com/gluesql/gluesql/pull/1932)).

## ✨ Highlights

### Insert Rust structs with `ToGlueRow`

The new `ToGlueRow` derive macro and Query Builder's `values_from` method convert named struct fields directly into INSERT columns and values ([#1948](https://github.com/gluesql/gluesql/pull/1948), [#1955](https://github.com/gluesql/gluesql/pull/1955)). `Option::None` becomes SQL `NULL`, and `#[glue(rename = "...")]` maps a Rust field to a different column name.

```rust
use gluesql::{
    core::query_builder::{Execute, table},
    ToGlueRow,
};

#[derive(ToGlueRow)]
struct Item {
    id: i64,
    #[glue(rename = "name")]
    title: String,
    rate: Option<f64>,
}

let items = vec![
    Item {
        id: 1,
        title: "Fish".to_owned(),
        rate: Some(0.2),
    },
    Item {
        id: 2,
        title: "Bread".to_owned(),
        rate: None,
    },
];

table("Item")
    .insert()
    .values_from(&items)?
    .execute(&mut glue)?;
```

### PostgreSQL-style regular-expression operators

GlueSQL now supports `~`, `~*`, `!~`, and `!~*` for regular-expression matching ([#2001](https://github.com/gluesql/gluesql/pull/2001)). Patterns use Rust `regex` syntax, support case-sensitive and case-insensitive matching, and preserve SQL `NULL` behavior.

```sql
SELECT name
FROM Item
WHERE name ~* '^fish|bread$';
```

The same operations are available through Query Builder.

### Query Builder improvements

Query Builder expressions can now specify ascending or descending order directly ([#1882](https://github.com/gluesql/gluesql/pull/1882)). The typed pipeline also exposes explicit source, join strategy, table access, distinct, ordering, offset, and limit stages.

## 🚀 Performance and Correctness

- ParquetStorage streams converted rows instead of loading an entire table into memory ([#1968](https://github.com/gluesql/gluesql/pull/1968)).
- `SUM`, `MIN`, `MAX`, `AVG`, `VARIANCE`, and `STDEV` now ignore `NULL` inputs according to SQL set-function semantics ([#1990](https://github.com/gluesql/gluesql/pull/1990)).
- CompositeStorage rollback now delegates to rollback on each inner storage instead of accidentally committing the transaction ([#1963](https://github.com/gluesql/gluesql/pull/1963)).
- Primary-key lookup planning no longer applies an unsafe predicate to the wrong side of a join ([#1943](https://github.com/gluesql/gluesql/pull/1943)).
- Index planning preserves a primary-key access path instead of replacing it with a less appropriate index plan ([#1919](https://github.com/gluesql/gluesql/pull/1919)).
- Table-factor fields are checked before expressions are considered safe to evaluate during planning ([#1961](https://github.com/gluesql/gluesql/pull/1961)).
- ORDER BY expressions are included when scanning schema dependencies ([#1982](https://github.com/gluesql/gluesql/pull/1982)).
- Schemaless rewrites now apply to CREATE TABLE AS SELECT sources ([#1987](https://github.com/gluesql/gluesql/pull/1987)).
- One-shot Sled migration opens no longer run the background flusher ([#1924](https://github.com/gluesql/gluesql/pull/1924)).
- SledStorage waits for a previous database lock to be released before failing to open the same path ([#1944](https://github.com/gluesql/gluesql/pull/1944)).

## 🛡️ Clearer SQL Validation

Unsupported syntax is now rejected during translation instead of being silently ignored or failing later:

- Multi-column `IN` subqueries ([#1936](https://github.com/gluesql/gluesql/pull/1936))
- `TEMPORARY`, `LIKE`, and `CLONE` options on CREATE TABLE ([#1972](https://github.com/gluesql/gluesql/pull/1972))
- Arguments supplied to unsupported table functions ([#1983](https://github.com/gluesql/gluesql/pull/1983))
- Unsupported transaction statement clauses ([#1991](https://github.com/gluesql/gluesql/pull/1991))
- Unsupported CREATE INDEX options ([#1998](https://github.com/gluesql/gluesql/pull/1998))

Translate errors now use typed enums for unsupported SQL options, making programmatic error handling more reliable ([#1980](https://github.com/gluesql/gluesql/pull/1980)).

## 🔧 Tooling and Maintenance

- Replaced Docusaurus with Zensical for the documentation site and updated its dependencies ([#1916](https://github.com/gluesql/gluesql/pull/1916), [#1874](https://github.com/gluesql/gluesql/pull/1874)).
- Documented Redb transaction support, updated Rust examples for synchronous execution, and refreshed the Rust getting-started guide ([#1832](https://github.com/gluesql/gluesql/pull/1832), [#1934](https://github.com/gluesql/gluesql/pull/1934), [#1940](https://github.com/gluesql/gluesql/pull/1940)).
- Updated the project Rust toolchain to 1.94 ([#1921](https://github.com/gluesql/gluesql/pull/1921)).
- Replaced grcov with cargo-llvm-cov, improved trusted coverage publishing, and now runs coverage on pushes to `main` ([#1938](https://github.com/gluesql/gluesql/pull/1938), [#1923](https://github.com/gluesql/gluesql/pull/1923), [#1954](https://github.com/gluesql/gluesql/pull/1954)).
- Migrated the SQL test suite to readable file-based fixtures ([#1941](https://github.com/gluesql/gluesql/pull/1941)).
- Strengthened Sled migration tests with isolated paths and reliable temporary-directory setup ([#1914](https://github.com/gluesql/gluesql/pull/1914), [#1918](https://github.com/gluesql/gluesql/pull/1918)).
- Simplified Rust CI package selection by using `cargo test -p` directly ([#1885](https://github.com/gluesql/gluesql/pull/1885)).
- Moved the `FromGlueRow` derive implementation into its own module alongside the new `ToGlueRow` implementation ([#1956](https://github.com/gluesql/gluesql/pull/1956)).
- Simplified the Rust crates.io publishing workflow ([#1922](https://github.com/gluesql/gluesql/pull/1922)).
- Removed deprecated Cargo `authors` fields from workspace packages ([#2012](https://github.com/gluesql/gluesql/pull/2012)).

## 👏 Contributors

Thanks to everyone who contributed to this release:

[@miinhho](https://github.com/miinhho), [@panarch](https://github.com/panarch), [@OmarAshour02](https://github.com/OmarAshour02), [@devgony](https://github.com/devgony), [@ssseft](https://github.com/ssseft), [@Bortlesboat](https://github.com/Bortlesboat), [@red-sprout](https://github.com/red-sprout), [@edomaur](https://github.com/edomaur), [@juhee200](https://github.com/juhee200), [@aswitocom](https://github.com/aswitocom), [@ShreyasUday](https://github.com/ShreyasUday), [@seosangwon](https://github.com/seosangwon), [@dgg1dbg](https://github.com/dgg1dbg), [@sweetpark](https://github.com/sweetpark), [@MinhyukWoo](https://github.com/MinhyukWoo), [@kwondo1017](https://github.com/kwondo1017), [@jinwooky](https://github.com/jinwooky), [@teddytennant](https://github.com/teddytennant), and [@jun02160](https://github.com/jun02160).

**Full Changelog:** [v0.19.0...v0.20.0](https://github.com/gluesql/gluesql/compare/v0.19.0...v0.20.0)
