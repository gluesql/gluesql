use {
    super::SqliteStorage,
    async_trait::async_trait,
    gluesql_core::{
        error::{Error as GlueError, Result},
        store::Transaction,
    },
};

#[async_trait]
impl Transaction for SqliteStorage {
    async fn begin(&mut self, autocommit: bool) -> Result<bool> {
        if autocommit {
            let active = { *self.tx_active.lock().unwrap() };
            if active {
                return Ok(false);
            }

            self.execute("BEGIN IMMEDIATE").await?;
            Ok(true)
        } else {
            {
                let active = self.tx_active.lock().unwrap();
                if *active {
                    return Err(GlueError::StorageMsg(
                        "transaction already started".to_owned(),
                    ));
                }
            }

            self.execute("BEGIN").await?;

            let mut active = self.tx_active.lock().unwrap();
            *active = true;

            Ok(false)
        }
    }

    async fn rollback(&mut self) -> Result<()> {
        self.execute("ROLLBACK").await?;

        let mut active = self.tx_active.lock().unwrap();
        *active = false;

        Ok(())
    }

    async fn commit(&mut self) -> Result<()> {
        self.execute("COMMIT").await?;

        let mut active = self.tx_active.lock().unwrap();
        *active = false;

        Ok(())
    }
}
