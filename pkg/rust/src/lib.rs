//! # GlueSQL
//!
//! ## Multi-Model Database Engine as a Library
//! GlueSQL is a Rust library for SQL databases that includes a parser ([sqlparser-rs](https://github.com/sqlparser-rs/sqlparser-rs)), an execution layer, and a variety of storage options, both persistent and non-persistent, all in one package. It is a versatile tool for developers, supporting both SQL and its own query builder (AST Builder). GlueSQL can handle structured and unstructured data, making it suitable for a wide range of use cases. It is portable and can be used with various storage types, including log files and read-write capable storage. GlueSQL is designed to be extensible and supports custom planners, making it a powerful tool for developers who need SQL support for their databases or services. GlueSQL is also flexible, as it can be used in Rust and JavaScript environments, and its language support is constantly expanding to include more programming languages.
//!
//! For more information on how to use GlueSQL, please refer to the [**official documentation website**](https://gluesql.org/docs). The documentation provides detailed information on how to install and use GlueSQL, as well as examples and tutorials on how to create custom storage systems and perform SQL operations.

pub mod core {
    pub use gluesql_core::*;
}

#[cfg(feature = "memory-storage")]
pub use memory_storage;

#[cfg(feature = "shared-memory-storage")]
pub use shared_memory_storage;

#[cfg(feature = "sled-storage")]
pub use sled_storage;

#[cfg(feature = "json-storage")]
pub use json_storage;

#[cfg(feature = "parquet-storage")]
pub use parquet_storage;

#[cfg(feature = "composite-storage")]
pub use composite_storage;

#[cfg(all(feature = "web-storage", target_arch = "wasm32"))]
pub use web_storage;

#[cfg(all(feature = "idb-storage", target_arch = "wasm32"))]
pub use idb_storage;

#[cfg(feature = "test-suite")]
pub use test_suite;

pub mod prelude {
    pub use gluesql_core::prelude::*;

    #[cfg(feature = "memory-storage")]
    pub use memory_storage::MemoryStorage;

    #[cfg(feature = "shared-memory-storage")]
    pub use shared_memory_storage::SharedMemoryStorage;

    #[cfg(feature = "sled-storage")]
    pub use sled_storage::SledStorage;

    #[cfg(feature = "json-storage")]
    pub use json_storage::JsonStorage;

    #[cfg(feature = "parquet-storage")]
    pub use parquet_storage::ParquetStorage;

    #[cfg(feature = "composite-storage")]
    pub use composite_storage::CompositeStorage;

    #[cfg(all(feature = "web-storage", target_arch = "wasm32"))]
    pub use web_storage::WebStorage;

    #[cfg(all(feature = "idb-storage", target_arch = "wasm32"))]
    pub use idb_storage::IdbStorage;
}
