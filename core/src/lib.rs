//! # GlueSQL
//!
//! `gluesql` is a SQL database library written in Rust.  
//! It provides a parser ([sqlparser-rs](https://github.com/ballista-compute/sqlparser-rs)), execution layer,
//! and optional storages ([`sled`](https://github.com/spacejam/sled) or `memory`) packaged into a single library.  
//! Developers can choose to use GlueSQL to build their own SQL database,
//! or as an embedded SQL database using the default storage engine.
//!
//! ## Examples
//!
//! ```
//! use gluesql::prelude::*;
//!
//! #[cfg(feature = "sled-storage")]
//! fn main() {
//!     let storage = SledStorage::new("data/doc-db").unwrap();
//!     let mut glue = Glue::new(storage);
//!     
//!     let sqls = vec![
//!         "DROP TABLE IF EXISTS Glue;",
//!         "CREATE TABLE Glue (id INTEGER);",
//!         "INSERT INTO Glue VALUES (100);",
//!         "INSERT INTO Glue VALUES (200);",
//!         "SELECT * FROM Glue WHERE id > 100;",
//!     ];
//!
//!     for sql in sqls {
//!         let output = glue.execute(sql).unwrap();
//!         println!("{:?}", output)
//!     }
//! }
//!
//! #[cfg(not(feature = "sled-storage"))]
//! fn main() {}
//! ```
//!
//! ## Custom Storage
//! To get started, all you need to implement for `gluesql` is implementing three traits
//! (two for functions, one running tests).  
//! There are also three optional traits (`AlterTable`, `Index`, `IndexMut`, and `Transaction`),
//! whether implementing it or not is all up to you.
//!
//! ### Store traits
//! * [Store](store/trait.Store.html)
//! * [StoreMut](store/trait.StoreMut.html)
//!
//! ### Optional Store traits
//! * [AlterTable](store/trait.AlterTable.html)
//! * [Index](store/trait.Index.html)
//! * [IndexMut](store/trait.IndexMut.html)
//! * [Transaction](store/trait.Transaction.html)
//!
//! ### Trait to run integration tests
//! * [Tester](tests/trait.Tester.html)
//!
//! ## Tests
//! `gluesql` provides integration tests as a module.  
//! Developers who wants to make their own custom storages can import and run those tests.  
//! `/tests/` might look quite empty, but actual test cases exist in `src/tests/`.  
//!
//! Example code to see,
//! * [tests/sled_storage.rs](https://github.com/gluesql/gluesql/blob/main/tests/sled_storage.rs)
//! * [tests/memory_storage.rs](https://github.com/gluesql/gluesql/blob/main/tests/memory_storage.rs)
//!
//! After you implement `Tester` trait, the only thing you need to do is calling `generate_tests!` macro.

// re-export
pub use chrono;
#[cfg(feature = "sled-storage")]
pub use sled;
pub use sqlparser;

mod glue;

pub mod ast;
pub mod data;
pub mod executor;
pub mod parse_sql;
pub mod plan;
pub mod result;
pub mod store;
pub mod tests;
pub mod translate;

pub mod prelude {
    pub use crate::{
        data::value::Value,
        executor::{execute, Payload},
        glue::Glue,
        parse_sql::parse,
        plan::plan,
        translate::translate,
    };
}
