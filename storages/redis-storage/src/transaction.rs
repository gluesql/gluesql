use {
    super::RedisStorage,
    gluesql_core::{
        error::{Error, Result},
        store::Transaction,
    },
};

impl Transaction for RedisStorage {
    fn begin(&mut self, autocommit: bool) -> Result<bool> {
        if autocommit {
            return Ok(false);
        }

        Err(Error::StorageMsg(
            "[RedisStorage] transaction is not supported".to_owned(),
        ))
    }

    fn rollback(&mut self) -> Result<()> {
        Ok(())
    }

    fn commit(&mut self) -> Result<()> {
        Ok(())
    }
}
