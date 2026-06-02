use crate::result::{Error, Result};

pub trait Transaction {
    fn begin(&mut self, autocommit: bool) -> Result<bool> {
        if autocommit {
            return Ok(false);
        }

        Err(Error::StorageMsg(
            "[Storage] Transaction::begin is not supported".to_owned(),
        ))
    }

    fn rollback(&mut self) -> Result<()> {
        Ok(())
    }

    fn commit(&mut self) -> Result<()> {
        Ok(())
    }
}
