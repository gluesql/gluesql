#![doc = include_str!("../../../README.md")]

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

    #[cfg(feature = "composite-storage")]
    pub use composite_storage::CompositeStorage;

    #[cfg(all(feature = "web-storage", target_arch = "wasm32"))]
    pub use web_storage::WebStorage;

    #[cfg(all(feature = "idb-storage", target_arch = "wasm32"))]
    pub use idb_storage::IdbStorage;
}
