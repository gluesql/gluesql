use std::sync::Arc;

use gluesql_json_storage::JsonStorage;
use gluesql_memory_storage::MemoryStorage;
use gluesql_shared_memory_storage::SharedMemoryStorage;
use gluesql_sled_storage::{SledStorage, sled::Config};

use crate::error::GlueSQLError;

#[derive(uniffi::Enum)]
pub enum Storage {
    Memory,
    SharedMemory,
    Json {
        path: String,
    },
    Sled {
        config: SledConfig,
    },
}

pub enum StorageBackend {
    Memory(Arc<tokio::sync::Mutex<MemoryStorage>>),
    SharedMemory(Arc<tokio::sync::Mutex<SharedMemoryStorage>>),
    Json(Arc<tokio::sync::Mutex<JsonStorage>>),
    Sled(Arc<tokio::sync::Mutex<SledStorage>>),
}

impl StorageBackend {
    pub fn new(storage: Storage) -> Result<Self, GlueSQLError> {
        let storage_backend = match storage {
            Storage::Memory => {
                StorageBackend::Memory(Arc::new(tokio::sync::Mutex::new(MemoryStorage::default())))
            }
            Storage::SharedMemory => {
                StorageBackend::SharedMemory(Arc::new(
                    tokio::sync::Mutex::new(SharedMemoryStorage::new()),
                ))
            }
            Storage::Json { path } => {
                let json_storage = JsonStorage::new(&path)
                    .map_err(|e| GlueSQLError::StorageError(e.to_string()))?;
                StorageBackend::Json(Arc::new(tokio::sync::Mutex::new(json_storage)))
            }
            Storage::Sled { config } => {
                let sled_cfg = Config::default()
                    .path(&config.path)
                    .cache_capacity(config.cache_capacity as u64)
                    .create_new(config.create_new)
                    .mode(config.mode.into())
                    .temporary(config.temporary)
                    .use_compression(config.use_compression)
                    .compression_factor(config.compression_factor)
                    .print_profile_on_drop(config.print_profile_on_drop);

                let sled_storage = SledStorage::try_from(sled_cfg)
                    .map_err(|e| GlueSQLError::StorageError(e.to_string()))?;
                StorageBackend::Sled(Arc::new(tokio::sync::Mutex::new(sled_storage)))
            }
        };

        Ok(storage_backend)
    }
}

#[derive(uniffi::Enum, Default, Clone)]
pub enum Mode {
    #[default]
    LowSpace,
    HighThroughput,
}

impl From<Mode> for gluesql_sled_storage::sled::Mode {
    fn from(mode: Mode) -> Self {
        match mode {
            Mode::LowSpace => gluesql_sled_storage::sled::Mode::LowSpace,
            Mode::HighThroughput => gluesql_sled_storage::sled::Mode::HighThroughput,
        }
    }
}

#[derive(uniffi::Record, Default, Clone)]
pub struct SledConfig {
    pub path: String,
    pub cache_capacity: i64,
    pub mode: Mode,
    pub create_new: bool,
    pub temporary: bool,
    pub use_compression: bool,
    pub compression_factor: i32,
    pub print_profile_on_drop: bool,
}
