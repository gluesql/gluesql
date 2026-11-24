---
title: Release v0.21.0
description: GlueSQL v0.21.0 adds SlateDB and SQLite storage backends.
---

# GlueSQL v0.21.0

GlueSQL v0.21 adds two storage backends. **SlateDB** lets applications store tables in an Amazon S3 or Cloudflare R2 bucket and update them in SQL transactions. The **SQLite** backend stores GlueSQL tables in a local database and can use SQLite's own query engine to filter rows.

If you have an existing Sled or Redb database, see [Upgrading from v0.20](#upgrading-from-v020).

## ✨ Highlights

### New SlateDB storage backend for S3 and Cloudflare R2

GlueSQL runs the SQL in your Rust application, and SlateDB writes committed changes to the bucket. With bucket credentials configured, an import job can record how many rows it processed in a table on S3.

```rust
use gluesql::prelude::{Glue, SlatedbStorage};

fn main() -> gluesql::core::error::Result<()> {
    let storage = SlatedbStorage::open("s3://my-bucket/import-history")?;
    let mut glue = Glue::new(storage);

    glue.execute("CREATE TABLE IF NOT EXISTS ImportRun (rows_loaded INT)")?;
    glue.execute("INSERT INTO ImportRun VALUES (120)")?;
    glue.storage.close()
}
```

If the next run opens the same URL and inserts a count of 80, `SELECT SUM(rows_loaded) FROM ImportRun` returns 200. For Cloudflare R2, use your account's endpoint with the bucket name and database path in the URL. [S3 and R2 connection examples](../storages/supported-storages/slatedb-storage.md#s3-and-cloudflare-r2)

Use one writer at a time for each database path. A job can group changes with `BEGIN` and `COMMIT`. Reads in the transaction see a consistent snapshot, and a successful commit means the writes have reached object storage. [SlateDB backend (#2068)](https://github.com/gluesql/gluesql/pull/2068)

### New SQLite storage backend

`SqliteStorage` lets your application use GlueSQL as its SQL interface and SQLite as its database. You create tables and send queries through `glue.execute`, using the same GlueSQL syntax, types, and functions as with other backends. SQLite stores the tables and their rows.

Because SQLite has its own SQL engine, GlueSQL's planner can translate part of a query into a SQLite `SELECT`. SQLite plans and runs that statement against its tables. GlueSQL finishes the original query on the rows SQLite returns.

An inventory application, for example, can declare a `tags LIST` column in a table stored in its SQLite file. Suppose it has 20 apples tagged `fruit`, 30 untagged pears, and 5 plums tagged `fruit`. This GlueSQL query finds tagged items with more than ten units in stock.

```sql
SELECT name
FROM Item
WHERE qty > 10 AND NOT IS_EMPTY(tags);
```

SQLite runs the `qty > 10` filter, so only the apple and pear rows reach GlueSQL. GlueSQL evaluates `NOT IS_EMPTY(tags)` on their lists and returns the apple row. [SQLite backend (#2078)](https://github.com/gluesql/gluesql/pull/2078)

The planner started with simple comparisons in single-table `WHERE` clauses. It now recognizes `IFNULL`, `COALESCE`, and `NULLIF`, including nested calls when both engines agree on types and comparisons. SQLite can therefore evaluate a whole condition such as `COALESCE(qty, 0) > 10`. In v0.21, GlueSQL handles joins and aggregates. [NULL functions in SQLite filters (#2097)](https://github.com/gluesql/gluesql/pull/2097)

Our goal is for SQLite to run larger parts of each query wherever it gives the same result as GlueSQL. We want to extend the planner to more functions and expressions, and eventually to whole joins and subqueries. For example, SQLite could join orders with inventory and return just the orders for out-of-stock items to GlueSQL.

Open a file with `SqliteStorage::new("inventory.sqlite")` or use `SqliteStorage::in_memory()` for a temporary database. GlueSQL records the logical column types and defaults alongside each table so they are available when you reopen the file. [SQLite storage guide](../storages/supported-storages/sqlite-storage.md)

## ⚠️ Upgrading from v0.20

### Export Sled data before switching versions

v0.21 removes SledStorage, its Cargo feature, and the CLI's Sled backend. To move an existing Sled database to Redb, first export it with the **v0.20 CLI**. Then load the dump into a new database with the **v0.21 CLI**. [Sled removal (#2066)](https://github.com/gluesql/gluesql/pull/2066)

```sh
# Run with GlueSQL v0.20
gluesql --path ./sled-data --dump ./dump.sql

# Run with GlueSQL v0.21
gluesql --storage redb --path ./restored.redb --execute ./dump.sql
```

The v0.21 CLI can also dump Redb, JSON, CSV, Parquet, and FileStorage databases. Specify `--storage` whenever you pass `--path` or `--dump`. [CLI guide](../getting-started/cli.md#migration-using-cli) · [CLI dump changes (#2065)](https://github.com/gluesql/gluesql/pull/2065)

### Upgrade existing Redb files

RedbStorage now uses GlueSQL storage format v5. Back up your database, then upgrade it with the v0.21 CLI before opening it. The command accepts GlueSQL storage formats v1 through v4. Keep the backup if you need to reopen the database with an older GlueSQL release. [Redb migration (#2056)](https://github.com/gluesql/gluesql/pull/2056) · [Redb format v5 (#2063)](https://github.com/gluesql/gluesql/pull/2063)

```sh
gluesql --storage redb --path ./data.redb --upgrade
```

Rust applications can run the same migration with `gluesql_redb_storage::migrate_to_latest(path)`. Format v5 reserves table names starting with `__GLUESQL_INDEX__` and rejects table names containing `/`. [Redb storage guide](../storages/supported-storages/redb-storage.md)

## 🚀 Faster reads in Redb and Parquet

### Redb supports secondary indexes

Use `CREATE INDEX` on a column or expression. Redb maintains the index as rows change, and its planner can use the index for a range scan or to read rows in either sort direction. [Redb indexes (#2063)](https://github.com/gluesql/gluesql/pull/2063)

```sql
CREATE INDEX by_qty ON Item (qty);
SELECT name, qty FROM Item WHERE qty >= 10 ORDER BY qty DESC;
```

### Parquet skips unused columns

For a query such as `SELECT name, qty FROM Item`, Parquet now reads only the two requested columns and the primary key if it needs one to identify rows. [Projected storage scans (#2072)](https://github.com/gluesql/gluesql/pull/2072)

## 🌊 SQL behavior changes

### NULL in filters and comparisons

Queries using `AND`, `OR`, `IN`, `NOT IN`, `BETWEEN`, and `LIKE` now follow the three-valued logic shared by PostgreSQL, MySQL, and SQLite for these cases. In particular, a `NOT IN` list containing NULL can change which rows a `WHERE` clause keeps. [NULL evaluation (#2076)](https://github.com/gluesql/gluesql/pull/2076)

| Expression | v0.21 result |
| --- | --- |
| `FALSE AND NULL` | `FALSE` |
| `TRUE OR NULL` | `TRUE` |
| `2 NOT IN (1, NULL)` | `NULL` |
| `1 BETWEEN 2 AND NULL` | `FALSE` |
| `'apple' LIKE NULL` | `NULL` |

### Other SQL changes

- Use `->>` to read a MAP entry or LIST element as text. For a MAP containing `{"id": 42}`, `attributes->>'id'` returns the text `'42'`. A missing key returns SQL NULL. [Text extraction (#1858)](https://github.com/gluesql/gluesql/pull/1858)
- GlueSQL prepares stored function calls during planning, including nested and recursive calls. Function bodies can query `SERIES` or `VALUES`. The planner rejects bodies that read stored tables and argument defaults that call a custom function. [Custom function planning (#2053)](https://github.com/gluesql/gluesql/pull/2053)
- `CREATE TABLE IF NOT EXISTS ... AS SELECT` leaves an existing table's rows alone when a script runs again. When GlueSQL copies a source schema, it also retains column names assigned by a table alias. [Repeated CREATE TABLE AS (#2044)](https://github.com/gluesql/gluesql/pull/2044) · [Copied column names (#2060)](https://github.com/gluesql/gluesql/pull/2060)
- GlueSQL rejects multi-table `DELETE` statements before modifying any rows. Earlier versions could delete from the first table named in the statement. [Multiple FROM tables (#2039)](https://github.com/gluesql/gluesql/pull/2039) · [Multiple DELETE targets (#2094)](https://github.com/gluesql/gluesql/pull/2094)
- `GLUE_OBJECTS` exposes the metadata keys supplied by the storage. `OBJECT_NAME` and `OBJECT_TYPE` are always present. Columns such as `CREATED` depend on the metadata returned. [Metadata columns (#2036)](https://github.com/gluesql/gluesql/pull/2036)
- Dumps written by v0.21 preserve the types of individual LIST elements, including nested lists. Generated SQL also round-trips POINT defaults, adjacent minus signs, and double quotes in DDL table names. [LIST elements (#2091)](https://github.com/gluesql/gluesql/pull/2091) · [POINT defaults (#2083)](https://github.com/gluesql/gluesql/pull/2083) · [Minus signs (#2089)](https://github.com/gluesql/gluesql/pull/2089) · [Quoted names (#2088)](https://github.com/gluesql/gluesql/pull/2088)

## 🔧 Rust API and custom storage upgrades

- **Executing plans.** `glue.execute_stmt(statement)` now takes ownership, plans the statement, and executes it. To run the plans returned by `glue.plan(sql)?`, pass each one to `glue.execute_prepared(&plan)`. In a SQL script, each statement is planned as it runs, so later statements see tables created earlier in the script. [Statement execution (#2069)](https://github.com/gluesql/gluesql/pull/2069) · [Batched statements (#2038)](https://github.com/gluesql/gluesql/pull/2038)
- **Inspecting plans.** Match on `statement.kind` using `StatementPlanKind`. The surrounding `StatementPlan` holds the schemas and custom function definitions prepared for execution. Query Builder's `with_schema` accepts a `Schema` or a `CREATE TABLE` string. [Statement schemas (#2069)](https://github.com/gluesql/gluesql/pull/2069) · [Custom function planning (#2053)](https://github.com/gluesql/gluesql/pull/2053)
- **Reading rows.** `Store::scan_data` now takes `ScanColumns::All` or `ScanColumns::Selected`. Return selected values in schema order and preserve each row's key, even when a query such as `SELECT COUNT(*)` requests zero columns. [Projected storage scans (#2072)](https://github.com/gluesql/gluesql/pull/2072)
- **Native reads.** Custom storages used with `Glue` now implement `NativeQuery`. An empty `impl NativeQuery for MyStorage {}` uses the default behavior. Implement `query_native` when your planner produces queries for the backend's own engine. [NativeQuery trait](../storages/developing-custom-storages/store-traits/native-query.md)
- **DDL hooks.** Overrides of `AlterTable::add_column` receive the evaluated default as a `Value`. Drop any `Store::fetch_referencings` override when updating your backend. Planners for backends that support `CREATE INDEX` can opt into the shared validation pass. [Column defaults (#2038)](https://github.com/gluesql/gluesql/pull/2038) · [Statement schemas (#2069)](https://github.com/gluesql/gluesql/pull/2069) · [Index validation (#2077)](https://github.com/gluesql/gluesql/pull/2077)
- **Inserting Rust structs.** `#[derive(ToGlueRow)]` borrows fields through `ToParamLiteral`. User-defined field types can implement this trait and participate in `values_from` without implementing `Clone`. [Borrowed field conversion (#1981)](https://github.com/gluesql/gluesql/pull/1981)

## 👏 Contributors

Thanks to [@chiliec](https://github.com/chiliec), [@ChobobDev](https://github.com/ChobobDev), [@dodok8](https://github.com/dodok8), [@juhee200](https://github.com/juhee200), [@panarch](https://github.com/panarch), [@SteelCrab](https://github.com/SteelCrab), [@sweetpark](https://github.com/sweetpark), and [@zmrdltl](https://github.com/zmrdltl) for contributing to this release.

[Full changelog from v0.20.0 to v0.21.0](https://github.com/gluesql/gluesql/compare/v0.20.0...v0.21.0)
