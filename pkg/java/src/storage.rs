use std::sync::Arc;

use gluesql_json_storage::JsonStorage;
use gluesql_memory_storage::MemoryStorage;
use gluesql_shared_memory_storage::SharedMemoryStorage;
use gluesql_sled_storage::{SledStorage, sled::Config};

use crate::error::GlueSQLError;

pub struct SledConfig {
    pub path: Option<String>,
    pub mode: Option<String>,
}

pub enum Storage {
    Memory,
    Json {
        path: String,
    },
    Sled {
        path: String,
        config: Option<SledConfig>,
    },
    SharedMemory {
        namespace: String,
    },
}

pub enum StorageBackend {
    Memory(Arc<tokio::sync::Mutex<MemoryStorage>>),
    Json(Arc<tokio::sync::Mutex<JsonStorage>>),
    SharedMemory(Arc<tokio::sync::Mutex<SharedMemoryStorage>>),
    Sled(Arc<tokio::sync::Mutex<SledStorage>>),
}

impl StorageBackend {
    pub fn new(storage: Storage) -> Result<Self, GlueSQLError> {
        let storage_backend = match storage {
            Storage::Memory => {
                StorageBackend::Memory(Arc::new(tokio::sync::Mutex::new(MemoryStorage::default())))
            }
            Storage::Json { path } => {
                let json_storage = JsonStorage::new(&path)
                    .map_err(|e| GlueSQLError::StorageError(e.to_string()))?;
                StorageBackend::Json(Arc::new(tokio::sync::Mutex::new(json_storage)))
            }
            Storage::SharedMemory { namespace: _ } => StorageBackend::SharedMemory(Arc::new(
                tokio::sync::Mutex::new(SharedMemoryStorage::new()),
            )),
            Storage::Sled { path, config } => {
                let sled_config = if let Some(config) = config {
                    let mut cfg = Config::default();
                    if let Some(path) = &config.path {
                        cfg = cfg.path(path);
                    }
                    cfg
                } else {
                    Config::default().path(&path)
                };

                let sled_storage = SledStorage::try_from(sled_config)
                    .map_err(|e| GlueSQLError::StorageError(e.to_string()))?;
                StorageBackend::Sled(Arc::new(tokio::sync::Mutex::new(sled_storage)))
            }
        };

        Ok(storage_backend)
    }
}
