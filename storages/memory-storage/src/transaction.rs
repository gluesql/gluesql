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
            //TODO : implement nested transaction later
            (StorageState::Transaction { .. }, false) => Err(Error::StorageMsg(
                "nested transaction is not supported".to_owned(),
            )),
            (StorageState::Transaction { autocommit }, true) => Ok(autocommit),
            (StorageState::Idle, _) => {
                self.set_snapshot();
                self.state = StorageState::Transaction { autocommit: false };

                Ok(autocommit)
            }
        }
    }

    async fn rollback(&mut self) -> Result<()> {
        match &self.snapshot {
            Some(snapshot) => {
                let data = snapshot.to_owned().data;
                self.functions = data.functions;
                self.id_counter = data.id_counter;
                self.items = data.items;
                self.metadata = data.metadata;
                self.state = StorageState::Idle;
                self.snapshot = None;
            }
            None => println!("TODO: non snapshot error handling"),
        }
        Ok(())
    }

    async fn commit(&mut self) -> Result<()> {
        self.snapshot = None;
        self.state = StorageState::Idle;
        Ok(())
    }
}
