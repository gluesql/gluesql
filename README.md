# GlueSQL

[![crates.io](https://img.shields.io/crates/v/gluesql.svg)](https://crates.io/crates/gluesql)
[![npm](https://img.shields.io/npm/v/gluesql?color=red)](https://www.npmjs.com/package/gluesql)
[![LICENSE](https://img.shields.io/crates/l/gluesql.svg)](https://github.com/gluesql/gluesql/blob/main/LICENSE)
![Rust](https://github.com/gluesql/gluesql/workflows/Rust/badge.svg)
[![docs.rs](https://docs.rs/gluesql/badge.svg)](https://docs.rs/gluesql)
[![Chat](https://img.shields.io/discord/780298017940176946)](https://discord.gg/C6TDEgzDzY)
[![Coverage Status](https://coveralls.io/repos/github/gluesql/gluesql/badge.svg?branch=main)](https://coveralls.io/github/gluesql/gluesql?branch=main)

## SQL Database Engine as a Library

GlueSQL is a SQL database library written in Rust.  
It provides a parser ([sqlparser-rs](https://github.com/sqlparser-rs/sqlparser-rs)), execution layer, and optional storages ([`sled`](https://github.com/spacejam/sled) or `memory`) packaged into a single library.  
Developers can choose to use GlueSQL to build their own SQL database, or as an embedded SQL database using the default storage engine.

## Standalone Mode

You can use GlueSQL as an embedded SQL database.  
GlueSQL provides three reference storage options.

- `SledStorage` - Persistent storage engine based on [`sled`](https://github.com/spacejam/sled "sled")
- `MemoryStorage` - Non-persistent storage engine based on `BTreeMap`
- `SharedMemoryStorage` - Non-persistent storage engine which works in multi-threaded environment

### Installation

- `Cargo.toml`

```toml
[dependencies]
gluesql = "0.13"
```

- CLI application

```
$ cargo install gluesql
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
version = "0.13"
default-features = false
features = ["alter-table", "index", "transaction"]
```

#### Four features below are also optional

- `alter-table` - ALTER TABLE query support
- `index` - CREATE INDEX and DROP INDEX, index support
- `transaction` - BEGIN, ROLLBACK and COMMIT, transaction support

### Usage

#### Two mandatory store traits to implement

- [`Store & StoreMut`](https://github.com/gluesql/gluesql/blob/main/core/src/store/mod.rs)

```rust
pub trait Store {
    async fn fetch_schema(..) -> ..;
    async fn fetch_data(..) -> ..;
    async fn scan_data(..) -> ..;
}

pub trait StoreMut where Self: Sized {
    async fn insert_schema(..) -> ..;
    async fn delete_schema(..) -> ..;
    async fn append_data(..) -> ..;
    async fn insert_data(..) -> ..;
    async fn delete_data(..) -> ..;
}
```

#### Optional store traits

- [`AlterTable`](https://github.com/gluesql/gluesql/blob/main/core/src/store/alter_table.rs), [`Index & IndexMut`](https://github.com/gluesql/gluesql/blob/main/core/src/store/index.rs), [`Transaction`](https://github.com/gluesql/gluesql/blob/main/core/src/store/transaction.rs)

```rust
pub trait AlterTable where Self: Sized {
    async fn rename_schema(..) -> ..;
    async fn rename_column(..) -> ..;
    async fn add_column(..) -> ..;
    async fn drop_column(..) -> ..;
}

pub trait Index {
    async fn scan_indexed_data(..) -> ..;
}

pub trait IndexMut where Self: Sized {
    async fn create_index(..) -> ..;
    async fn drop_index(..) -> ..;
}

pub trait Transaction where Self: Sized {
    async fn begin(..) -> ..;
    async fn rollback(..) -> ..;
    async fn commit(..) -> ..;
}
```

## GlueSQL.js

GlueSQL.js is a SQL database for web browsers and Node.js. It works as an embedded database and entirely runs in the browser. GlueSQL.js supports in-memory storage backend, but it will soon to have localStorage, sessionStorage and indexedDB backend supports.

#### More info

- [GlueSQL for web browsers and Node.js](https://github.com/gluesql/gluesql/tree/main/pkg/javascript)

## SQL Features

GlueSQL currently supports a limited subset of queries. It's being actively developed.

#### Data Types

| Category | Type                                                                       |
| -------- | -------------------------------------------------------------------------- |
| Numeric  | `INT8`, `INT16`, `INT32`, `INTEGER`, `INT128`, `UINT8`, `FLOAT`, `DECIMAL` |
| Date     | `DATE`, `TIME`, `TIMESTAMP`, `INTERVAL`                                    |
| Others   | `BOOLEAN`, `TEXT`, `UUID`, `MAP`, `LIST`, `BYTEA`                          |

#### Queries

- `CREATE TABLE`, `DROP TABLE`
- `ALTER TABLE` - `ADD COLUMN`, `DROP COLUMN`, `RENAME COLUMN` and `RENAME TO`.
- `CREATE INDEX`, `DROP INDEX`
- `INSERT`, `UPDATE`, `DELETE`, `SELECT`
- `GROUP BY`, `HAVING`
- `ORDER BY`
- Transaction queries: `BEGIN`, `ROLLBACK` and `COMMIT`
- Nested select, join, aggregations ...

You can see tests for the currently supported queries in [test-suite/src/\*](https://github.com/gluesql/gluesql/tree/main/test-suite/src).
