# GlueSQL
[![crates.io](https://img.shields.io/crates/v/gluesql.svg)](https://crates.io/crates/gluesql)
[![docs.rs](https://docs.rs/gluesql/badge.svg)](https://docs.rs/gluesql)
[![LICENSE](https://img.shields.io/crates/l/gluesql.svg)](https://github.com/gluesql/gluesql/blob/main/LICENSE)
![Rust](https://github.com/gluesql/gluesql/workflows/Rust/badge.svg)
[![Chat](https://img.shields.io/discord/780298017940176946)](https://discord.gg/C6TDEgzDzY)

## SQL Database Engine as a Library
GlueSQL is a SQL database library written in Rust. It provides a parser ([sqlparser-rs](https://github.com/ballista-compute/sqlparser-rs)), execution layer, and optional storage ([sled](https://github.com/spacejam/sled)) packaged into a single library.
Developers can choose to use GlueSQL to build their own SQL database, or as an embedded SQL database using the default storage engine.  

## Standalone Mode
You can use GlueSQL as an embedded SQL database. GlueSQL provides [sled](https://github.com/spacejam/sled "sled") as a default storage engine.

### Installation
In your `Cargo.toml`:
```toml
[dependencies]
gluesql = "0.6"
```

### Usage
```rust
use gluesql::{parse, Glue, SledStorage};

fn main() {
    let storage = SledStorage::new("data.db").unwrap();
    let mut glue = Glue::new(storage);

    let sqls = "
        CREATE TABLE Glue (id INTEGER);
        INSERT INTO Glue VALUES (100);
        INSERT INTO Glue VALUES (200);
        SELECT * FROM Glue WHERE id > 100;
        DROP TABLE Glue;
    ";
    
    for query in parse(sqls).unwrap() {
        glue.execute(&query).unwrap();
    }
}
```

## SQL Library Mode (For Custom Storage)
### Installation
`sled-storage` is optional. So in `Cargo.toml`:
```toml
[dependencies]
gluesql = { version = "0.6", default-features = false, features = ["alter-table"] }

# alter-table is optional.
# If your DB does not have plan to support ALTER TABLE, then use this below.
gluesql = { version = "0.6", default-features = false }
```

### Usage
There are two required 2 traits for using GlueSQL: `Store` and `StoreMut`.
In `src/store/mod.rs`,
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

.. there is also a single, optional trait:
In `src/store/alter_table.rs`,
```rust
pub trait AlterTable where Self: Sized {
    async fn rename_schema(..) -> ..;
    async fn rename_column(..) -> ..;
    async fn add_column(..) -> ..;
    async fn drop_column(..) -> ..;
}
```

## Use Cases
### [GlueSQL-js](https://github.com/gluesql/gluesql-js)
https://github.com/gluesql/gluesql-js  
Use SQL in web browsers!
GlueSQL-js provides 3 storage options,
* in-memory
* localStorage
* sessionStorage

### [GlueSQL Sheets](https://sheets.gluesql.com)
https://sheets.gluesql.com  
Turn **Google Sheets** into a SQL database!  
It uses Google Sheets as a storage.  
Data is stored and updated from Google Sheets.

### Other expected use cases
* Add SQL layer to NoSQL databases: Redis, CouchDB...
* Build new SQL database management system

## SQL Features
GlueSQL currently supports a limited subset of queries. It's being actively developed.

* `CREATE TABLE` with 8 types: `INTEGER`, `FLOAT`, `BOOLEAN`, `TEXT`, `DATE`, `TIMESTAMP`, `TIME` and `INTERVAL`.
* `ALTER TABLE` with 4 operations: `ADD COLUMN`, `DROP COLUMN`, `RENAME COLUMN` and `RENAME TO`.
* `INSERT`, `UPDATE`, `DELETE`, `SELECT`, `DROP TABLE`
* `GROUP BY`, `HAVING`
* Nested select, join, aggregations ...

You can see tests for the currently supported queries in [src/tests/*](https://github.com/gluesql/gluesql/tree/main/src/tests).

## Contribution
There are a few simple rules to follow.
- No `mut` keywords in `src/executor` and `src/data`.
- Iterator should not be evaluated in the middle of execution layer.
- Every error must have corresponding integration test cases to generate.  
(except for `Unreachable-` and `Conflict-` error types)
