# GlueSQL - Coming Soon
[![crates.io](https://img.shields.io/crates/v/gluesql.svg)](https://crates.io/crates/gluesql)
[![docs.rs](https://docs.rs/gluesql/badge.svg)](https://docs.rs/gluesql)
[![LICENSE](https://img.shields.io/crates/l/gluesql.svg)](https://github.com/gluesql/gluesql/blob/main/LICENSE)
![Rust](https://github.com/gluesql/gluesql/workflows/Rust/badge.svg)

## SQL Database Engine as a Library
GlueSQL is a SQL database library written in Rust.  
You can use GlueSQL itself, or it's also quite easy to make your own SQL database using GlueSQL.

## Standalone Mode
You can simply use GlueSQL as an embedded SQL database, GlueSQL provides [sled](https://github.com/spacejam/sled, "sled") as a default storage engine.

### Installation
In your `Cargo.toml`
```toml
[dependencies]
gluesql = { version = "0.1.10", features = ["sled-storage"] }
```

### Usage
```rust
use gluesql::*;

fn main() {
    let storage = SledStorage::new("data.db").unwrap();
    let mut glue = Glue::new(storage);

    let sql = "
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
gluesql = "0.1.10"
```

### Usage
All you only need to do is implement 2 traits: `Store` and `StoreMut`!
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

### Examples - [GlueSQL-js](https://github.com/gluesql/gluesql-js, "GlueSQL-js")
Use SQL in web browsers!  
GlueSQL-js provides 3 storage options,
* in-memory
* localStorage
* sessionStorage.

## SQL Features
`src/tests/*`
:smile:

## Plans
:smile:
* More SQL syntax supports - GROUP BY, HAVING ...

### Providing more `Store` traits
Not only `Store` and `StoreMut`, but also GlueSQL will separately provides,  
* ForeignKey trait
* Transaction trait
* Index trait

Then users can make their own SQL database with only using
* `Store` & `StoreMut`, or  
* `Store` + `StoreMut` + `ForeignKey` but without `Index` and `Transaction` support.
* with all traits.
* etc...

## Contribution
It's very early stage, please feel free to do whatever you want to.  
Only the thing you need to be aware of is...  
- Except for `src/glue.rs` and `src/tests/`, there is no place to use `mut` keyword.  
