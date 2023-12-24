use {self::memory_storage::ExMemoryStorage, rustler::NifUntaggedEnum};

pub mod memory_storage;

#[derive(NifUntaggedEnum)]
pub enum ExStorage {
    MemoryStorage(ExMemoryStorage),
}
