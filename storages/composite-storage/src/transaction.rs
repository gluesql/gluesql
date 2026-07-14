use {
    super::CompositeStorage,
    gluesql_core::{
        error::{Error, Result},
        store::Transaction,
    },
};

impl Transaction for CompositeStorage {
    fn begin(&mut self, autocommit: bool) -> Result<bool> {
        if autocommit {
            for storage in self.storages.values_mut() {
                storage.begin(autocommit)?;
            }

            return Ok(true);
        }

        Err(Error::StorageMsg(
            "[CompositeStorage] Transaction::begin is not supported".to_owned(),
        ))
    }

    fn rollback(&mut self) -> Result<()> {
        for storage in self.storages.values_mut() {
            storage.commit()?;
        }

        Ok(())
    }

    fn commit(&mut self) -> Result<()> {
        for storage in self.storages.values_mut() {
            storage.commit()?;
        }

        Ok(())
    }
}
