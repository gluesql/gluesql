#![cfg(feature = "transaction")]

use {
    super::CompositeStorage,
    async_trait::async_trait,
    gluesql_core::{
        result::{Error, Result},
        store::Transaction,
    },
};

#[async_trait(?Send)]
impl Transaction for CompositeStorage {
    async fn begin(&mut self, autocommit: bool) -> Result<bool> {
        if autocommit {
            for storage in self.storages.values_mut() {
                storage.begin(autocommit).await?;
            }

            return Ok(true);
        }

        Err(Error::StorageMsg(
            "[CompositeStorage] Transaction::begin is not supported".to_owned(),
        ))
    }

    async fn rollback(&mut self) -> Result<()> {
        for storage in self.storages.values_mut() {
            storage.commit().await?;
        }

        Ok(())
    }

    async fn commit(&mut self) -> Result<()> {
        for storage in self.storages.values_mut() {
            storage.commit().await?;
        }

        Ok(())
    }
}
