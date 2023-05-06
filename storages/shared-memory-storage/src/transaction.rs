use {
    super::SharedMemoryStorage,
    async_trait::async_trait,
    gluesql_core::{
        error::{Error, Result},
        store::Transaction,
    },
};

#[async_trait(?Send)]
impl Transaction for SharedMemoryStorage {
    async fn begin(&mut self, autocommit: bool) -> Result<bool> {
        if autocommit {
            return Ok(false);
        }

        Err(Error::StorageMsg(
            "[Shared MemoryStorage] transaction is not supported".to_owned(),
        ))
    }

    async fn rollback(&mut self) -> Result<()> {
        Err(Error::StorageMsg(
            "[Shared MemoryStorage] transaction is not supported".to_owned(),
        ))
    }

    async fn commit(&mut self) -> Result<()> {
        Err(Error::StorageMsg(
            "[Shared MemoryStorage] transaction is not supported".to_owned(),
        ))
    }
}
