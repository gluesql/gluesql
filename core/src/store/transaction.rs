use {
    crate::result::{Error, Result},
    async_trait::async_trait,
};

#[async_trait(?Send)]
pub trait Transaction {
    async fn begin(&mut self, autocommit: bool) -> Result<bool> {
        if autocommit {
            return Ok(false);
        }

        Err(Error::StorageMsg(
            "[Storage] Transaction::begin is not supported".to_owned(),
        ))
    }

    async fn rollback(&mut self) -> Result<()> {
        Err(Error::StorageMsg(
            "[Storage] Transaction::rollback is not supported".to_owned(),
        ))
    }

    async fn commit(&mut self) -> Result<()> {
        Err(Error::StorageMsg(
            "[Storage] Transaction::commit is not supported".to_owned(),
        ))
    }
}
