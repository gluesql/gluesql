//! # GlueSQL
//!
//! `gluesql` is a SQL database engine fully written in Rust.
//! You can simply use `gluesql` as an embedded SQL database using default storage
//! [sled](https://crates.io/crates/sled).
//! Or you can make your own SQL database using `gluesql`, it provides parser & execution layer as
//! a library.
//!
//! `gluesql` uses [sqlparser-rs](https://crates.io/crates/sqlparser) as a parser, and has own implementation of execution layer.
//! And the entire codes of execution layer are pure functional!
//!
//! ## Examples
//!
//! ```
//! use gluesql::*;
//!
//! #[cfg(feature = "sled-storage")]
//! fn main() {
//!     let storage = SledStorage::new("data/doc-db").unwrap();
//!     let mut glue = Glue::new(storage);
//!
//!     let sqls = "
//!         CREATE TABLE Glue (id INTEGER);
//!         INSERT INTO Glue VALUES (100);
//!         INSERT INTO Glue VALUES (200);
//!         SELECT * FROM Glue WHERE id > 100;
//!         DROP TABLE Glue;
//!     ";
//!
//!     for query in parse(sqls).unwrap() {
//!         glue.execute(&query).unwrap();
//!     }
//! }
//!
//! #[cfg(not(feature = "sled-storage"))]
//! fn main() {}
//! ```
//!
//! ## Custom Storage
//! All you need to implement for `gluesql` is implementing 3 traits (2 for functions, 1 for
//! running tests).
//! There is also an optional trait (AlterTable), whether implementing it or not is all up to you.
//!
//! * [Store](store/trait.Store.html)
//! * [StoreMut](store/trait.StoreMut.html)
//! * [AlterTable - optional](store/trait.AlterTable.html)
//! * [Tester](tests/trait.Tester.html)
//!
//! Custom storage examples to see,
//! * [GlueSQL-js](https://github.com/gluesql/gluesql-js)
//!
//! ## Tests
//! For making easy for developers to implement custom storages, `gluesql` also provides integration
//! tests as a module.
//!
//! So, in `/tests/`, it looks quite empty, but actual test cases exist in `src/tests/`.
//!
//! Example code to see,
//! * [tests/sled_storage.rs](https://github.com/gluesql/gluesql/blob/main/tests/sled_storage.rs)
//!
//! After you implement `Tester` trait, the only thing you need to do is calling `generate_tests!` macro.

// re-export
pub use chrono;
pub use sqlparser as parser;

mod executor;
mod glue;
mod parse_sql;
mod storages;
mod utils;

pub mod data;
pub mod result;
pub mod store;
pub mod tests;

pub use data::*;
pub use executor::*;
pub use parse_sql::*;
pub use result::*;
pub use store::*;

#[cfg(feature = "sled-storage")]
pub use glue::Glue;
#[cfg(feature = "sled-storage")]
pub use sled;
#[cfg(feature = "sled-storage")]
pub use storages::*;
