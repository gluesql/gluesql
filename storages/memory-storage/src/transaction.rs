use {
    super::MemoryStorage,
    async_trait::async_trait,
    gluesql_core::{
        error::{Error, Result},
        store::Transaction,
    },
};

#[async_trait(?Send)]
impl Transaction for MemoryStorage {
    async fn begin(&mut self, autocommit: bool) -> Result<bool> {
        if autocommit {
            return Ok(false);
        }

        Err(Error::StorageMsg(
            "[MemoryStorage] transaction is not supported".to_owned(),
        ))
    }

    async fn rollback(&mut self) -> Result<()> {
        Ok(())
    }

    async fn commit(&mut self) -> Result<()> {
        Ok(())
    }
}
