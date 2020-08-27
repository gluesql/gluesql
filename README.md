# GlueSQL
[![crates.io](https://img.shields.io/crates/v/gluesql.svg)](https://crates.io/crates/gluesql)
[![docs.rs](https://docs.rs/gluesql/badge.svg)](https://docs.rs/gluesql)
[![LICENSE](https://img.shields.io/crates/l/gluesql.svg)](https://github.com/gluesql/gluesql/blob/main/LICENSE)
![Rust](https://github.com/gluesql/gluesql/workflows/Rust/badge.svg)

## SQL Database Engine as a Library
GlueSQL is a SQL database library written in Rust which provides parser ([sqlparser-rs](https://github.com/ballista-compute/sqlparser-rs)), execution layer, and an optional storage ([sled](https://github.com/spacejam/sled)).  
Developers can use GlueSQL to build their own SQL databases or they can simply use GlueSQL as an embedded SQL database using default storage.  

## Standalone Mode
You can simply use GlueSQL as an embedded SQL database, GlueSQL provides [sled](https://github.com/spacejam/sled "sled") as a default storage engine.

### Installation
In your `Cargo.toml`
```toml
[dependencies]
gluesql = { version = "0.1.15", features = ["sled-storage"] }
```

### Usage
```rust
use gluesql::*;

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
gluesql = "0.1.15"
```

### Usage
All you only need to do is implementing 2 traits: `Store` and `StoreMut`!
In `src/store.rs`,
```rust
pub trait Store<T: Debug> {
    fn fetch_schema(&self, table_name: &str) -> Result<Schema>;
    fn scan_data(&self, table_name: &str) -> Result<RowIter<T>>;
}

pub trait StoreMut<T: Debug> where Self: Sized {
    fn generate_id(self, table_name: &str) -> MutResult<Self, T>;
    fn insert_schema(self, schema: &Schema) -> MutResult<Self, ()>;
    fn delete_schema(self, table_name: &str) -> MutResult<Self, ()>;
    fn insert_data(self, key: &T, row: Row) -> MutResult<Self, Row>;
    fn delete_data(self, key: &T) -> MutResult<Self, ()>;
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
* `INSERT`, `UPDATE`, `DELETE`, `SELECT`, `DROP TABLE`
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
- Except for `src/glue.rs` and `src/tests/`, there is no place to use `mut` keyword.  
