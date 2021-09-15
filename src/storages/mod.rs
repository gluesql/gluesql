use cfg_if::cfg_if;

cfg_if! {
    if #[cfg(feature = "sled-storage")] {
        pub mod sled_storage;
        pub use sled_storage::SledStorage;
    }
}

cfg_if! {
    if #[cfg(feature = "memory-storage")] {
        pub mod memory_storage;
        pub use memory_storage::MemoryStorage;
    }
}
