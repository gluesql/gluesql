//! # GlueSQL
//!
//! `gluesql` is a SQL database library written in Rust.  
//! It provides a parser ([sqlparser-rs](https://github.com/sqlparser-rs/sqlparser-rs)), execution layer,
//! and optional storages ([`sled`](https://github.com/spacejam/sled) or `memory`) packaged into a single library.  
//! Developers can choose to use GlueSQL to build their own SQL database,
//! or as an embedded SQL database using the default storage engine.
//!
//! ## Examples
//!
//! ```
//! #[cfg(feature = "sled-storage")]
//! fn main() {
//!     use gluesql::prelude::*;
//!
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
//! (two for functions, one for running tests).  
//! There are also optional traits.
//! Whether to implement it or not is all up to you.
//!
//! ### Store traits
//! * [Store](core/store/trait.Store.html)
//! * [StoreMut](core/store/trait.StoreMut.html)
//!
//! ### Optional Store traits
//! * [AlterTable](core/store/trait.AlterTable.html)
//! * [Index](core/store/trait.Index.html)
//! * [IndexMut](core/store/trait.IndexMut.html)
//! * [Transaction](core/store/trait.Transaction.html)
//! * [Metadata](core/store/trait.Metadata.html)
//!
//! ### Trait to run integration tests
//! * [Tester](test_suite/trait.Tester.html)
//!
//! ## Tests
//! `gluesql` provides integration tests as a module.  
//! Developers who wants to make their own custom storages can import and run those tests.  
//! `/tests/` might look quite empty, but actual test cases exist in `test-suite` workspace.
//!
//! Example code to see,
//! * [tests/memory_storage.rs](https://github.com/gluesql/gluesql/blob/main/storages/memory-storage/tests/memory_storage.rs)
//! * [tests/sled_storage.rs](https://github.com/gluesql/gluesql/blob/main/storages/sled-storage/tests/sled_storage.rs)
//! * [tests/shared-memory-storage.rs](https://github.com/gluesql/gluesql/blob/main/storages/shared-memory-storage/tests/shared_memory_storage.rs)
//! 
//! After you implement `Tester` trait, the only thing you need to do is calling `generate_tests!` macro.

pub mod core {
    pub use gluesql_core::*;
}

#[cfg(feature = "memory-storage")]
pub mod memory_storage {
    pub use memory_storage::*;
}

#[cfg(feature = "shared-memory-storage")]
pub mod shared_memory_storage {
    pub use shared_memory_storage::*;
}

#[cfg(feature = "sled-storage")]
pub mod sled_storage {
    pub use sled_storage::*;
}

#[cfg(feature = "test-suite")]
pub mod test_suite {
    pub use test_suite::*;
}

pub mod prelude {
    pub use gluesql_core::prelude::*;
    #[cfg(feature = "memory-storage")]
    pub use memory_storage::MemoryStorage;
    #[cfg(feature = "shared-memory-storage")]
    pub use shared_memory_storage::SharedMemoryStorage;
    #[cfg(feature = "sled-storage")]
    pub use sled_storage::SledStorage;
}
