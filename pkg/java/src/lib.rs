mod error;
mod executor;
mod payload;
mod storage;

pub use error::GlueSQLError;
pub use payload::Payload;
pub use storage::{SledConfig, Storage, StorageBackend};

use executor::QueryExecutor;

pub struct Glue {
    storage: StorageBackend,
}

impl Glue {
    pub fn new(storage: Storage) -> Result<Self, GlueSQLError> {
        let storage_backend = StorageBackend::new(storage)?;
        Ok(Self {
            storage: storage_backend,
        })
    }

    pub async fn query(&self, sql: String) -> Result<Vec<String>, GlueSQLError> {
        QueryExecutor::execute_query(&self.storage, sql).await
    }
}

uniffi::include_scaffolding!("gluesql");
