use {
    super::{MemoryStorage, StorageState},
    async_trait::async_trait,
    gluesql_core::{
        error::{Error, Result},
        store::Transaction,
    },
};

#[async_trait(?Send)]
impl Transaction for MemoryStorage {
    async fn begin(&mut self, autocommit: bool) -> Result<bool> {
        let current_state = self.state.clone();
        match (current_state, autocommit) {
            (StorageState::Transaction { .. }, false) => Err(Error::StorageMsg(
                "nested transaction is not supported".to_owned(),
            )),
            (StorageState::Transaction { autocommit }, true) => Ok(autocommit),
            (StorageState::Idle, _) => {
                self.state = StorageState::Transaction { autocommit: false };

                Ok(autocommit)
            }
        }
    }

    async fn rollback(&mut self) -> Result<()> {
        while let Some(log) = self.pop_log() {
            self.undo(log).await?
        }

        self.clear_buffer();
        self.state = StorageState::Idle;

        Ok(())
    }

    async fn commit(&mut self) -> Result<()> {
        self.clear_buffer();
        self.state = StorageState::Idle;
        Ok(())
    }
}
