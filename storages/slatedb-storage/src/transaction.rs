use {
    crate::{SlatedbStorage, error::StorageError},
    gluesql_core::{error::Result, store::Transaction},
    slatedb::IsolationLevel,
};

impl Transaction for SlatedbStorage {
    fn begin(&mut self, autocommit: bool) -> Result<bool> {
        if self.txn.is_some() {
            if autocommit {
                return Ok(false);
            }

            return Err(StorageError::NestedTransactionNotSupported.into());
        }

        let txn = self
            .runtime
            .block_on(self.db.begin(IsolationLevel::Snapshot))
            .map_err(StorageError::SlateDb)?;
        self.txn = Some(txn);

        Ok(autocommit)
    }

    fn rollback(&mut self) -> Result<()> {
        if let Some(txn) = self.txn.take() {
            txn.rollback();
        }

        Ok(())
    }

    fn commit(&mut self) -> Result<()> {
        let Some(txn) = self.txn.take() else {
            return Ok(());
        };

        let handle = self
            .runtime
            .block_on(txn.commit())
            .map_err(StorageError::Commit)?;

        if let Some(handle) = handle {
            self.runtime
                .block_on(handle.await_durable())
                .map_err(StorageError::Durability)?;
        }

        Ok(())
    }
}
