use {
    super::SharedMemoryStorage,
    gluesql_core::{
        error::{Error, Result},
        store::Transaction,
    },
};

impl Transaction for SharedMemoryStorage {
    fn begin(&mut self, autocommit: bool) -> Result<bool> {
        if autocommit {
            return Ok(false);
        }

        Err(Error::StorageMsg(
            "[Shared MemoryStorage] transaction is not supported".to_owned(),
        ))
    }

    fn rollback(&mut self) -> Result<()> {
        Err(Error::StorageMsg(
            "[Shared MemoryStorage] transaction is not supported".to_owned(),
        ))
    }

    fn commit(&mut self) -> Result<()> {
        Err(Error::StorageMsg(
            "[Shared MemoryStorage] transaction is not supported".to_owned(),
        ))
    }
}
