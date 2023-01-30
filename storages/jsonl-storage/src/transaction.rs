#![cfg(feature = "transaction")]

use {
    super::JsonlStorage,
    async_trait::async_trait,
    gluesql_core::{
        result::{Error, Result},
        store::Transaction,
    },
};

#[async_trait(?Send)]
impl Transaction for JsonlStorage {
    async fn begin(&mut self, autocommit: bool) -> Result<bool> {
        if autocommit {
            return Ok(false);
        }

        Err(Error::StorageMsg(
            "[JsonlStorage] transaction is not supported".to_owned(),
        ))
    }

    async fn rollback(&mut self) -> Result<()> {
        Err(Error::StorageMsg(
            "[JsonlStorage] transaction is not supported".to_owned(),
        ))
    }

    async fn commit(&mut self) -> Result<()> {
        Err(Error::StorageMsg(
            "[JsonlStorage] transaction is not supported".to_owned(),
        ))
    }
}
