mod error;
mod executor;
mod storage;
mod uniffi_types;

pub use error::GlueSQLError;
pub use storage::{Mode, SledConfig, Storage, StorageBackend};
pub use uniffi_types::{Payload, SqlValue};

use executor::QueryExecutor;

#[derive(uniffi::Object)]
pub struct Glue {
    storage: StorageBackend,
}

#[uniffi::export]
impl Glue {
    #[uniffi::constructor]
    pub fn new(storage: Storage) -> Result<Self, GlueSQLError> {
        let storage_backend = StorageBackend::new(storage)?;
        Ok(Self {
            storage: storage_backend,
        })
    }

    pub async fn query(&self, sql: String) -> Result<Vec<Payload>, GlueSQLError> {
        QueryExecutor::execute_query(&self.storage, sql).await
    }
}

uniffi::setup_scaffolding!();
