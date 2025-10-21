//! # `GlueSQL`
//!
//! ## Multi-Model Database Engine as a Library
//! `GlueSQL` is a Rust library for SQL databases that bundles a parser ([sqlparser-rs](https://github.com/sqlparser-rs/sqlparser-rs)), execution layer, and a variety of storage options—both persistent and ephemeral—into a single package. It supports SQL, its own AST builder, and works with structured or unstructured data across multiple backends. The project is extensible, enabling custom planners and storage integrations, and runs in both Rust and JavaScript environments with growing language support.
//!
//! For more details on using `GlueSQL`, visit the [**official documentation website**](https://gluesql.org/docs). The site covers installation, examples, and tutorials for building custom storage systems and executing SQL operations.

pub mod core {
    pub use gluesql_core::*;
}

pub use gluesql_core::params;

// Re-export the derive macro so users can `use gluesql::FromGlueRow`.
pub use gluesql_macros::FromGlueRow;

#[cfg(feature = "gluesql_memory_storage")]
pub use gluesql_memory_storage;

#[cfg(feature = "gluesql-shared-memory-storage")]
pub use gluesql_shared_memory_storage;

#[cfg(feature = "gluesql_sled_storage")]
pub use gluesql_sled_storage;

#[cfg(feature = "gluesql-redb-storage")]
pub use gluesql_redb_storage;

#[cfg(feature = "gluesql-json-storage")]
pub use gluesql_json_storage;

#[cfg(feature = "gluesql-csv-storage")]
pub use gluesql_csv_storage;

#[cfg(feature = "gluesql-parquet-storage")]
pub use gluesql_parquet_storage;

#[cfg(feature = "gluesql-file-storage")]
pub use gluesql_file_storage;

#[cfg(feature = "gluesql-git-storage")]
pub use gluesql_git_storage;

#[cfg(feature = "gluesql-mongo-storage")]
pub use gluesql_mongo_storage;

#[cfg(feature = "gluesql-composite-storage")]
pub use gluesql_composite_storage;

#[cfg(all(feature = "gluesql-web-storage", target_arch = "wasm32"))]
pub use gluesql_web_storage;

#[cfg(all(feature = "gluesql-idb-storage", target_arch = "wasm32"))]
pub use gluesql_idb_storage;

#[cfg(feature = "test-suite")]
pub use test_suite;

pub mod prelude {
    pub use gluesql_core::prelude::*;

    #[cfg(feature = "gluesql_memory_storage")]
    pub use gluesql_memory_storage::MemoryStorage;

    #[cfg(feature = "gluesql-shared-memory-storage")]
    pub use gluesql_shared_memory_storage::SharedMemoryStorage;

    #[cfg(feature = "gluesql_sled_storage")]
    pub use gluesql_sled_storage::SledStorage;

    #[cfg(feature = "gluesql-redb-storage")]
    pub use gluesql_redb_storage::RedbStorage;

    #[cfg(feature = "gluesql-json-storage")]
    pub use gluesql_json_storage::JsonStorage;

    #[cfg(feature = "gluesql-csv-storage")]
    pub use gluesql_csv_storage::CsvStorage;

    #[cfg(feature = "gluesql-parquet-storage")]
    pub use gluesql_parquet_storage::ParquetStorage;

    #[cfg(feature = "gluesql-file-storage")]
    pub use gluesql_file_storage::FileStorage;

    #[cfg(feature = "gluesql-git-storage")]
    pub use gluesql_git_storage::GitStorage;

    #[cfg(feature = "gluesql-mongo-storage")]
    pub use gluesql_mongo_storage;

    #[cfg(feature = "gluesql-composite-storage")]
    pub use gluesql_composite_storage::CompositeStorage;

    #[cfg(all(feature = "gluesql-web-storage", target_arch = "wasm32"))]
    pub use gluesql_web_storage::WebStorage;

    #[cfg(all(feature = "gluesql-idb-storage", target_arch = "wasm32"))]
    pub use gluesql_idb_storage::IdbStorage;
}
