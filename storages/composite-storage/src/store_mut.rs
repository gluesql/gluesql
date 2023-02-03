use {
    super::CompositeStorage,
    async_trait::async_trait,
    gluesql_core::{
        data::{Key, Schema},
        result::{Error, Result},
        store::{DataRow, StoreMut},
    },
};

#[async_trait(?Send)]
impl StoreMut for CompositeStorage {
    async fn insert_schema(&mut self, schema: &Schema) -> Result<()> {
        let storage = schema
            .engine
            .as_ref()
            .or(self.default_engine.as_ref())
            .and_then(|engine| self.storages.get_mut(engine));

        match (storage, schema.engine.is_some()) {
            (Some(storage), true) => storage.insert_schema(schema).await,
            (Some(storage), false) => {
                let schema = Schema {
                    engine: self.default_engine.clone(),
                    ..schema.clone()
                };

                storage.insert_schema(&schema).await
            }
            (None, _) => Err(Error::StorageMsg(format!(
                "storage not found for table: {}",
                schema.table_name
            ))),
        }
    }

    async fn delete_schema(&mut self, table_name: &str) -> Result<()> {
        match self.fetch_storage_mut(table_name).await? {
            Some(storage) => storage.delete_schema(table_name).await,
            None => Ok(()),
        }
    }

    async fn append_data(&mut self, table_name: &str, rows: Vec<DataRow>) -> Result<()> {
        match self.fetch_storage_mut(table_name).await? {
            Some(storage) => storage.append_data(table_name, rows).await,
            None => Ok(()),
        }
    }

    async fn insert_data(&mut self, table_name: &str, rows: Vec<(Key, DataRow)>) -> Result<()> {
        match self.fetch_storage_mut(table_name).await? {
            Some(storage) => storage.insert_data(table_name, rows).await,
            None => Ok(()),
        }
    }

    async fn delete_data(&mut self, table_name: &str, keys: Vec<Key>) -> Result<()> {
        match self.fetch_storage_mut(table_name).await? {
            Some(storage) => storage.delete_data(table_name, keys).await,
            None => Ok(()),
        }
    }
}
