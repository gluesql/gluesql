# GlueSQL
[![crates.io](https://img.shields.io/crates/v/gluesql.svg)](https://crates.io/crates/gluesql)
[![docs.rs](https://docs.rs/gluesql/badge.svg)](https://docs.rs/gluesql)
[![LICENSE](https://img.shields.io/crates/l/gluesql.svg)](https://github.com/gluesql/gluesql/blob/main/LICENSE)
![Rust](https://github.com/gluesql/gluesql/workflows/Rust/badge.svg)
[![Chat](https://img.shields.io/discord/780298017940176946)](https://discord.gg/C6TDEgzDzY)

## SQL Database Engine as a Library
GlueSQL is a SQL database library written in Rust which provides parser ([sqlparser-rs](https://github.com/ballista-compute/sqlparser-rs)), execution layer, and an optional storage ([sled](https://github.com/spacejam/sled)).  
Developers can use GlueSQL to build their own SQL databases or they can simply use GlueSQL as an embedded SQL database using default storage.  

## Standalone Mode
You can simply use GlueSQL as an embedded SQL database, GlueSQL provides [sled](https://github.com/spacejam/sled "sled") as a default storage engine.

### Installation
In your `Cargo.toml`
```toml
[dependencies]
gluesql = "0.3"
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
Now you don't need to include `sled-storage`. So in `Cargo.toml`,
```toml
[dependencies]
gluesql = { version = "0.3", default-features = false, features = ["alter-table"] }

# alter-table is optional.
# If your DB does not have plan to support ALTER TABLE, then use this below.
gluesql = { version = "0.3", default-features = false }
```

### Usage
All you only need to do is implementing 2 traits: `Store` and `StoreMut`!
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

And a single optional trait, whether implementing this is all up to you!
In `src/store/alter_table.rs`,
```rust
pub trait AlterTable where Self: Sized {
    async fn rename_schema(..) -> ..;
    async fn rename_column(..) -> ..;
    async fn add_column(..) -> ..;
    async fn drop_column(..) -> ..;
}
```

### Examples - [GlueSQL-js](https://github.com/gluesql/gluesql-js)  
Use SQL in web browsers!  
GlueSQL-js provides 3 storage options,
* in-memory
* localStorage
* sessionStorage.

## SQL Features
GlueSQL currently supports limited queries, it's in very early stage.

* `CREATE` with 4 types: `INTEGER`, `FLOAT`, `BOOLEAN`, `TEXT` with an optional `NULL` attribute.
* `ALTER TABLE` with 4 operations: `ADD COLUMN`, `DROP COLUMN`, `RENAME COLUMN` and `RENAME TO`.
* `INSERT`, `UPDATE`, `DELETE`, `SELECT`, `DROP TABLE`
* `GROUP BY`, `HAVING`
* Nested select, join, aggregations ...

You can see current query supports in [src/tests/*](https://github.com/gluesql/gluesql/tree/main/src/tests).

### Other expected use cases
* Run SQL in web browsers - [gluesql-js](https://github.com/gluesql/gluesql-js)
It would be cool to make state management library using `gluesql-js`.
* Add SQL layer to NoSQL databases: Redis, CouchDB...
* Build new SQL database management system

## Contribution
It's very early stage, please feel free to do whatever you want to.  
Only the thing you need to be aware of is...  
- Except for `src/glue.rs`, `src/tests/` and `src/utils/`, there is no place to use `mut` keyword.  
