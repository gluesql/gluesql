# GlueSQL

[![crates.io](https://img.shields.io/crates/v/gluesql.svg)](https://crates.io/crates/gluesql)
[![docs.rs](https://docs.rs/gluesql/badge.svg)](https://docs.rs/gluesql)
[![LICENSE](https://img.shields.io/crates/l/gluesql.svg)](https://github.com/gluesql/gluesql/blob/main/LICENSE)
![Rust](https://github.com/gluesql/gluesql/workflows/Rust/badge.svg)
[![Chat](https://img.shields.io/discord/780298017940176946)](https://discord.gg/C6TDEgzDzY)
[![codecov.io](https://codecov.io/github/gluesql/gluesql/coverage.svg?branch=main)](https://codecov.io/github/gluesql/gluesql?branch=main)

## SQL Database Engine as a Library

GlueSQL is a SQL database library written in Rust.  
It provides a parser ([sqlparser-rs](https://github.com/ballista-compute/sqlparser-rs)), execution layer, and optional storages ([`sled`](https://github.com/spacejam/sled) or `memory`) packaged into a single library.  
Developers can choose to use GlueSQL to build their own SQL database, or as an embedded SQL database using the default storage engine.

## Standalone Mode

You can use GlueSQL as an embedded SQL database.  
GlueSQL provides two reference storage options.

- `SledStorage` - Persistent storage engine based on [`sled`](https://github.com/spacejam/sled "sled")
- `MemoryStorage` - Non-persistent storage engine based on `BTreeMap`

### Installation

In your `Cargo.toml`:

```toml
[dependencies]
gluesql = "0.9"
```

### Usage

```rust
use gluesql::prelude::*;

fn main() {
    let storage = SledStorage::new("data/doc-db").unwrap();
    let mut glue = Glue::new(storage);
    let sqls = vec![
        "DROP TABLE IF EXISTS Glue;",
        "CREATE TABLE Glue (id INTEGER);",
        "INSERT INTO Glue VALUES (100);",
        "INSERT INTO Glue VALUES (200);",
        "SELECT * FROM Glue WHERE id > 100;",
    ];

    for sql in sqls {
        let output = glue.execute(sql).unwrap();
        println!("{:?}", output)
    }
}
```

## SQL Library Mode (For Custom Storage)

### Installation

`sled-storage` and `memory-storage` features are optional, so these are not required for custom storage makers.

```toml
[dependencies.gluesql]
version = "0.9"
default-features = false
features = ["sorter", "alter-table", "index", "transaction"]
```

#### Four features below are also optional

- `sorter` - ORDER BY support for non-indexed expressions.
- `alter-table` - ALTER TABLE query support
- `index` - CREATE INDEX & DROP INDEX, index support
- `transaction` - BEGIN, ROLLBACK and COMMIT, transaction support

### Usage

#### Two mandatory store traits to implement

- [`Store & StoreMut`](https://github.com/gluesql/gluesql/blob/main/core/src/store/mod.rs)

```rust
pub trait Store<T: Debug> {
    async fn fetch_schema(..) -> ..;
    async fn scan_data(..) -> ..;
}

pub trait StoreMut<T: Debug> where Self: Sized {
    async fn insert_schema(..) -> ..;
    async fn delete_schema(..) -> ..;
    async fn insert_data(..) -> ..;
    async fn update_data(..) -> ..;
    async fn delete_data(..) -> ..;
}
```

#### Optional store traits

- [`AlterTable`](https://github.com/gluesql/gluesql/blob/main/core/src/store/alter_table.rs), [`Index & IndexMut`](https://github.com/gluesql/gluesql/blob/main/core/src/store/index.rs) and [`Transaction`](https://github.com/gluesql/gluesql/blob/main/core/src/store/transaction.rs)

```rust
pub trait AlterTable where Self: Sized {
    async fn rename_schema(..) -> ..;
    async fn rename_column(..) -> ..;
    async fn add_column(..) -> ..;
    async fn drop_column(..) -> ..;
}

pub trait Index<T: Debug> {
    async fn scan_indexed_data(..) -> ..;
}

pub trait IndexMut<T: Debug> where Self: Sized {
    async fn create_index(..) -> ..;
    async fn drop_index(..) -> ..;
}

pub trait Transaction where Self: Sized {
    async fn begin(..) -> ..;
    async fn rollback(..) -> ..;
    async fn commit(..) -> ..;
}
```

## Use Cases

### [GlueSQL-js](https://github.com/gluesql/gluesql-js)

<https://github.com/gluesql/gluesql-js>
Use SQL in web browsers!
GlueSQL-js provides 3 storage options,

- in-memory
- localStorage
- sessionStorage

### [GlueSQL Sheets](https://sheets.gluesql.com)

<https://sheets.gluesql.com>
Turn **Google Sheets** into a SQL database!  
It uses Google Sheets as a storage.  
Data is stored and updated from Google Sheets.

### Other expected use cases

- Add SQL layer to NoSQL databases: Redis, CouchDB...
- Build new SQL database management system

## SQL Features

GlueSQL currently supports a limited subset of queries. It's being actively developed.

- `CREATE TABLE` with 8 types: `INTEGER`, `FLOAT`, `BOOLEAN`, `TEXT`, `DATE`, `TIMESTAMP`, `TIME` and `INTERVAL`.
- `ALTER TABLE` with 4 operations: `ADD COLUMN`, `DROP COLUMN`, `RENAME COLUMN` and `RENAME TO`.
- `CREATE INDEX`, `DROP INDEX`
- `INSERT`, `UPDATE`, `DELETE`, `SELECT`, `DROP TABLE`
- `GROUP BY`, `HAVING`
- `ORDER BY`
- Transaction queries: `BEGIN`, `ROLLBACK` and `COMMIT`
- Nested select, join, aggregations ...

You can see tests for the currently supported queries in [test-suite/src/\*](https://github.com/gluesql/gluesql/tree/main/test-suite/src).

## Contribution

There are a few simple rules to follow.

- No `mut` keywords in `src/executor` and `src/data`.
- Iterator should not be evaluated in the middle of execution layer.
- Every error must have corresponding integration test cases to generate.  
  (except for `Unreachable-` and `Conflict-` error types)
